# p2p host, identity and discovery

> **As built at `7d8833d`** (`develop`, 2026-08-16). Status labels and diagram legend: [README](README.md#status-labels).
> Target design: [`plan/architecture.md`](../../plan/architecture.md) §2.3, §2.5, §3.4, §9.1.

The `cc-p2p` process (`services/p2p`) as a host: boot, tasks and channels, the `cc-libp2p` stack,
identity, discv5, the peer manager and the gRPC edges. Gossip and DAS:
[10-p2p-gossip-and-das.md](10-p2p-gossip-and-das.md); req/resp, Status payloads and backfill:
[11-p2p-reqresp-and-sync.md](11-p2p-reqresp-and-sync.md).

## 1. Status at a glance

`cc-p2p` runs only in **shape A** (`docker-compose.yml`, service `p2p`). Shape B
(`bin/beacon-core`) hosts no p2p, so every row below is **ABSENT** in shape B. No shipped
configuration pairs cc-p2p with cc-beacon-core; how p2p attaches to shape B is not yet defined.

| Unit | Code | Shape A status |
|---|---|---|
| Process entry, `run_process` | `services/p2p/src/main.rs:463`, `services/p2p/src/service.rs:857` | LIVE |
| Swarm task (sole `Swarm<CcBehaviour>` owner) | `services/p2p/src/host.rs:183` | LIVE |
| Supervisor (per-task panic policy) | `services/p2p/src/supervisor.rs:84` | LIVE |
| Peer manager (dial, score, ban) | `services/p2p/src/peer_manager/mod.rs:878` | LIVE; its respawn is INERT ([2.3](#23-supervisor-and-panic-policy)) |
| Discovery (discv5, `EnrManager`, `DialQueue`) | `services/p2p/src/discovery/task.rs:425` | LIVE |
| Node key (`node_key`, mode 0600) | `services/p2p/src/identity.rs:162` | LIVE |
| ENR seq file `<node_key>.seq` | `services/p2p/src/discovery/enr.rs:921` | DEAD-AS-WIRED |
| Status / Ping / MetaData / Goodbye handshake | `services/p2p/src/host.rs:379-405` | LIVE (local MetaData static) |
| Gossip subscription (`SwarmCommand::Subscribe`) | `services/p2p/src/host.rs:1253` | DEAD-AS-WIRED (no sender); validation pool IDLE |
| GossipSub peer scoring | `crates/libp2p/src/scoring.rs` | PARAMS-ONLY |
| `SubnetManager`, runtime cgc hook | `services/p2p/src/discovery/subnet_manager.rs:437`, `services/p2p/src/discovery/cgc_hook.rs:181` | TEST-ONLY |
| KZG verify pool (`cc-kzg-verify-{i}`) | `services/p2p/src/service.rs:705` | DEAD-AS-WIRED (self-terminating) |
| Block/column req/resp serve | `services/p2p/src/host.rs:745-752` | STUB |
| `stub-reqresp`, `idle_worker` | `services/p2p/src/service.rs:741`, `:558` | STUB |
| QUIC transport | `crates/libp2p/src/transport.rs:59-65` | DEAD-AS-WIRED (enabling returns `QuicNotYetWired`) |
| libp2p-metrics sub-registry | `services/p2p/src/metrics.rs:645-651` | DEAD-AS-WIRED (never recorded) |
| Self-devnet mode (`run_devnet`) | `services/p2p/src/fault_mode.rs:907` | DEVNET-ONLY |

At HEAD p2p subscribes to no gossip topics; the plan schedules gossip wiring for a later stage.
What works today is the host shell: identity, discovery, dialing, limits, the Status handshake,
app-score enforcement, aggregate health and metrics.

## 2. Process layout

### 2.1 Startup order (`services/p2p/src/main.rs:463-531`)

1. `Cli::parse`; `--emit-bootnodes` writes devnet keys and exits; `--publish-fixture` or
   `--devnet-peer` switches to devnet mode ([section 10](#10-fault-modes-and-devnet-mode)).
2. `cc_config::load::<P2pConfig>("p2p")` (`config/p2p.toml`, then `CC_P2P_*`), then
   **identity before anything binds**: `identity::load_or_create` (`services/p2p/src/main.rs:491`).
   A refused key (mode, length, secret or path) makes `main` return `Err` before any port binds:
   exit 1, no gRPC, health or metrics. As wired, compose `restart: unless-stopped`
   (`docker-compose.yml:13`) would restart it repeatedly until the file is fixed.
3. `cc_bootstrap::init`, `P2pMetrics::register`, EngineStream deps and the `P2pService` routes.
4. `run_process` spawns `p2p-runtime` (running `serve`) and waits up to 10 s for `serve` to report
   ready, which it does after building the swarm and the task factories, just before the
   supervisor starts (`services/p2p/src/service.rs:625`). Only then does it spawn `grpc-serve`,
   so gRPC and health bind after the swarm exists; a timeout or early error stops both halves
   (`services/p2p/src/service.rs:874-910`).
5. `serve` reloads the key, builds `SlotClock` + `epoch-ticks`, the channel map, edge workers,
   `ForkContext`, swarm, handshake deps, WatchServeWindow client, the supervised factories
   (a real discovery task only when `enable_discovery`, [5.1](#51-the-discovery-task)),
   `status-epoch` and the supervisor (`services/p2p/src/service.rs:262-633`); the swarm task then
   listens on `/ip4/0.0.0.0/tcp/9000` (`services/p2p/src/host.rs:184-190`).

### 2.2 Task and channel map

```text
 cc-p2p (services/p2p): one tokio multi-thread runtime.  [S] = supervised task

 status-epoch ------- StatusEpoch -------------+  send(): waits for capacity
 peer_manager [S] --- ClosePeer, BlockPeer ----+  send(): waits for capacity
 peer_manager [S] --- Dial --------------------+  try_send(): dropped when full
 gossip-validate - -  ReportValidation - - - - +  send(); IDLE (0 topics)
 publish-bridge - - - Publish - - - - - - - - -+  try_send(); fed only by x E1 publish
                                               | cmd mpsc(512)
                                               v
                    +--------------------------------+
 peers <=tcp 9000=> |         swarm task [S]         |==> conn(256) + VecDeque ==> peer_manager
                    | sole owner Swarm<CcBehaviour>  |- -> gossip(1024) - - -> gossip-validate
                    | control req/resp inline        |==> reqresp_in(256) ===> stub-reqresp
                    +--------------------------------+
                    polls the network only after a chain_out reserve() (<= 500 ms, then sheds)

 peers <=udp 9000=> discovery [S] ==> DialQueue(256) ==> dial(256) try_send ==> peer_manager
 peer_manager ==> peer_view watch ==> discovery, status-epoch
 epoch-ticks ==> epoch watch ==> discovery, status-epoch
 gossip-validate - -> chain_out(1024) - -> chain-stream-client - -> E1 object/verdict (IDLE)
 gossip-validate, chain-in-late-verdicts - -> penalty(256) - -> peer_manager
 chain:9001 ==== E1 ChainView ====> chain-stream-client (cc_seam::Ipc; p2p dials)
 chain:9001 x E1 publish - -> chain-stream-client - -> proto_pub(256) - -> chain-publish-dispatch
 storage:9006 == E6 WatchServeWindow, u64::MAX (p2p dials) ==> advertised window -> Status
 engine container ....> EngineStream on :9002 (served, never dialed)
 cc-kzg-verify-{i} OS threads: x kzg mpsc(256) has no producer; the pool exits at start
```

Legend: [README](README.md#diagram-legend) (`====>` live data, `---->` live control, `- - ->`
idle, `x` dead-as-wired, `....>` served, never dialed). Text after a `+` says how that producer
sends on `cmd`.

The swarm task is the only owner of `Swarm<CcBehaviour>`; there is no `Mutex<Swarm>`, and every
mutation arrives as a `SwarmCommand` on `cmd` ([ADR-P2-02](../adr/ADR-P2-02.md);
`services/p2p/src/host.rs:1-5`). Control req/resp and bounded snappy decompression run inline.
Its loop (`services/p2p/src/host.rs:200-324`):

- **Lifecycle first.** `ConnectionEstablished` / `ConnectionClosed` / `DialFailure` go to `conn`
  with `try_send`; on Full they enter the unbounded `pending_conn` buffer and the task stops polling
  the swarm (still draining `cmd`) until capacity returns (`services/p2p/src/host.rs:345-364`).
- **Stall-then-shed.** Otherwise it reserves a `chain_out` permit, drops it, then polls the
  swarm; after `stall_max = heartbeat (1 s) x 0.5 = 500 ms` it polls anyway and sheds new gossip
  as IGNORE (`services/p2p/src/host.rs:254-311`). The swarm never sends on `chain_out`, which
  never fills, so nothing sheds.

Unsupervised tasks are plain `cc_bootstrap::spawn` (`tokio::spawn` plus a root span, no panic
capture): an unsupervised panic is neither caught nor counted.

| Task | Spawned at | Supervised | Status / note |
|---|---|---|---|
| `p2p-runtime`, `grpc-serve` | `services/p2p/src/service.rs:874`, `:908` | no | LIVE; raced in `run_process` |
| `supervisor` | `services/p2p/src/service.rs:631` | -- | LIVE; polls JoinHandles every 10 ms |
| `swarm`, `peer_manager`, `discovery` | `services/p2p/src/service.rs:602-623` | yes | LIVE; see [2.3](#23-supervisor-and-panic-policy) (peer-manager respawn INERT) |
| `idle_worker` | `services/p2p/src/service.rs:558` | yes | STUB (`pending()` placeholder) |
| `epoch-ticks`, `status-epoch` | `services/p2p/src/clock.rs:262`, `services/p2p/src/service.rs:569` | no | LIVE; per-epoch Status re-exchange |
| `gossip-validate` | `services/p2p/src/service.rs:736` | no | IDLE (one serial loop) |
| `stub-reqresp` | `services/p2p/src/service.rs:741` | no | STUB (drains and discards `reqresp_in`) |
| `kzg-verify-pool` bridge + `cc-kzg-verify-{i}` OS threads | `services/p2p/src/service.rs:761` | no | DEAD-AS-WIRED (self-terminating) |
| `publish-bridge`, `chain-publish-dispatch` | `services/p2p/src/service.rs:769`, `:797` | no | IDLE (E1 publish has no producer) |
| `chain-stream-client`, `chain-in-late-verdicts` | `services/p2p/src/service.rs:815`, `:805` | no | LIVE session; verdicts IDLE |
| `stub-chain-out` | `services/p2p/src/service.rs:832` | no | STUB; only when `[peers].chain` is empty, not in compose |
| WatchServeWindow loop | `services/p2p/src/storage_client.rs:395` | no | LIVE (content static) |
| EngineStream session | `services/p2p/src/engine_stream/server.rs:150` | no | DEAD-AS-WIRED (one per inbound stream; none at HEAD) |
| libp2p handlers, discv5 internals | `crates/libp2p/src/swarm.rs:66` (`with_tokio_executor`) | no | LIVE (library tasks) |

### 2.3 Supervisor and panic policy

Policy per [ADR-P2-13](../adr/ADR-P2-13.md) (`services/p2p/src/supervisor.rs:20-49`):

| Task | Policy | On panic | On clean exit |
|---|---|---|---|
| `swarm` | `ProcessFatal` | Fatal | Fatal ("clean exit"), e.g. `listen_on` failure or `cmd` closed |
| `peer_manager` | `Respawn` | immediate respawn | not respawned |
| `discovery` | `RespawnBudget{5, 300 s}` | respawn; Fatal on the 6th panic inside 300 s (after 5 restarts) (`services/p2p/src/supervisor.rs:179-190`) | converted to a panic unless shutting down (`services/p2p/src/service.rs:539-542`) |
| `idle_worker` | `Respawn` | immediate respawn | not respawned (it never exits: `pending()`) |

Every caught panic logs once and increments `cc_p2p_worker_panics_total{task}` cumulatively
(`services/p2p/src/supervisor.rs:159-161`). Two traced consequences:

- **Peer-manager respawn is INERT.** The factory `take()`s its channel bundle once; a respawned
  instance only waits for shutdown (`services/p2p/src/service.rs:445-478`; the comment at `:441`
  says otherwise). As wired, after a peer-manager panic no dials, scores or bans would happen,
  lifecycle events would be lost, and discovery's `peer_view_rx.changed()` would return `Err` on
  every select pass, a hot loop. If the swarm is parked on a lifecycle backlog (`pending_conn`,
  waiting on `conn_tx.reserve()`) when the peer manager's `conn_rx` drops, the reserve fails, the
  swarm exits on `ConnClosed` (`services/p2p/src/host.rs:238-241`), and the supervisor treats that
  clean exit as process-fatal (`services/p2p/src/supervisor.rs:139-153`). All traced, not observed.
- **discv5 bind failure is fast-fatal.** As wired, a persistent UDP bind error would panic,
  respawn immediately and exhaust the budget in well under a second (traced, not observed).

**Fatal path.** `SupervisorOutcome::Fatal` -> `serve` returns `RuntimeError::SwarmPanic{task}`
(used for `task = "discovery"` too) -> `run_process` fires the gRPC stop so aggregate health goes
NOT_SERVING, waits up to 5 s -> `main` logs "exiting non-zero after swarm panic" and calls
`process::exit(1)` (`services/p2p/src/main.rs:524-528`) -> compose `restart: unless-stopped`.

**Shutdown.** The drain sequence and budgets are in
[12 §5](12-boot-health-and-shutdown.md#5-shutdown). p2p adds one step: `serve` sets the shutdown
watch and waits up to 2 s for the supervisor, which aborts and awaits its handles
(`services/p2p/src/service.rs:636-648`). `gossip-validate`, `stub-reqresp`, `publish-bridge` and
`epoch-ticks` are not shutdown-aware. Shutdown normally fits the 5 s grace period; p2p's worst case
is 5.15 s, past which compose sends SIGKILL ([12 §5.2](12-boot-health-and-shutdown.md#52-budgets-against-stop_grace_period-5s)).

### 2.4 Channel map: bounds and overflow

Bounds are constants in `services/p2p/src/channels.rs:31-47` (plus `DIAL_QUEUE_BOUND`,
`services/p2p/src/discovery/dial_queue.rs:15`). Depth gauges `cc_p2p_queue_depth{q}` are
approximate: incremented and clamped on enqueue, never measured, and for `reqresp_in` bumped even
when the send fails (`services/p2p/src/host.rs:636-645`, `:1347-1361`).

| Channel | Cap | Producer send | On overflow | Consumer | Status |
|---|---|---|---|---|---|
| `gossip` swarm -> validation | 1024 | `try_send` | shed IGNORE, `cc_p2p_gossip_shed_total` | `gossip-validate` | IDLE |
| `reqresp_in` swarm -> stub | 256 | `try_send`, result ignored | silent drop | `stub-reqresp` | STUB |
| `conn` swarm -> peer manager | 256 | `try_send` | lifecycle: unbounded `pending_conn` + swarm read pause; `NewListenAddr` and `PeerPenalty`: drop | `peer_manager` | LIVE |
| `kzg` validation -> KZG pool | 256 | none (sender dropped, `services/p2p/src/service.rs:705`) | n/a | bridge exits | DEAD-AS-WIRED |
| `chain_out` validation -> chain stream | 1024 | validation `try_send` / `send` | swarm stall-then-shed via `reserve()` | `chain-stream-client` | IDLE |
| `chain_in` chain stream -> late verdicts | 1024 | `try_send` | drop | `chain-in-late-verdicts` | IDLE |
| `proto_pub` chain stream -> publish dispatch | 256 | `try_send` | drop + debug | `chain-publish-dispatch` | IDLE |
| `publish` | 256 | dispatch from local backlog | backlog drops oldest + counter | `publish-bridge` | IDLE |
| `cmd` -> swarm | 512 | policy `send().await`; `Dial`, `Publish` `try_send` | Dial/Publish dropped; others wait | swarm | LIVE |
| `dial` discovery -> peer manager | 256 | `try_send` | drop + warn | `peer_manager` | LIVE |
| `penalty` validation pool, `chain-in-late-verdicts` -> peer manager | 256 | `try_send` | drop | `peer_manager` | IDLE |
| `peer_view`, `epoch`, shutdown | watch | `send` | overwrite | discovery, `status-epoch`, many | LIVE |

### 2.5 Tests that pin this design

In `services/p2p/tests/`: `runtime_identity.rs` (identity before bind, mode 0600, supervisor and
the swarm-fatal path; single-`Swarm`-owner guard `swarm_type_only_in_host` at `:427`),
`peer_manager.rs` (connection limits, bans through `allow_block_list`), `discovery_enr.rs` (`nfd`
never disconnects, dial-queue bound, discovery restart budget), `enr_seq_probe.rs` (one seq bump
per batch), `fork_digest_vectors.rs` (Hoodi digests), `snappy_size_discipline.rs` (10 MiB bound).

## 3. cc-libp2p composition

`cc-libp2p` (`crates/libp2p`) is the only crate allowed `libp2p*` dependencies and has zero
workspace dependencies; it pins rust-libp2p at `LIBP2P_GIT_REV`
(`crates/libp2p/src/lib.rs:44`). Dependency choices and deviations are in
[p2p-dependencies.md](../p2p-dependencies.md). `services/p2p` builds the swarm through
`build_host_swarm` (`services/p2p/src/host.rs:1367-1386`), which installs the eth2 message-id
and the nine req/resp protocols with a 15 s request timeout (TTFB 5 s + RESP 10 s;
`services/p2p/src/reqresp/mod.rs:289-293`).

```text
 +----------------------------------------------------------------------------------------+
 | Swarm<CcBehaviour>, idle connection timeout 30 s (crates/libp2p/src/swarm.rs:59-69)    |
 +----------------------------------------------------------------------------------------+
 | CcBehaviour (crates/libp2p/src/behaviour.rs:87-105)                                    |
 |  gossipsub        Anonymous + validate_messages, 10 MiB, heartbeat 1 s, eth2 msg-id,   |
 |                   IDONTWANT on, SnappyTransform (frame format, 10 MiB cap)             |
 |                   no peer score (PARAMS-ONLY); 0 topics subscribed in production       |
 |  reqresp          beacon_blocks_by_range/2, _by_root/2, _by_head/1 (inbound only),     |
 |                   data_column_sidecars_by_range/1, _by_root/1        -> STUB serve     |
 |  reqresp_status   status/2         reqresp_goodbye    goodbye/1                        |
 |  reqresp_ping     ping/1           reqresp_metadata   metadata/3   (SszSnappyCodec)    |
 |  identify         /cc/identify/1.0.0       ping   libp2p ping    (events ignored)      |
 |  limits           max 100 / in 60 / out 60 / pend-in 100 / pend-out 8 / per-peer 1     |
 |  allow_block      BlockedPeers, fed by ClosePeer{ban: true} and BlockPeer              |
 +----------------------------------------------------------------------------------------+
 | yamux             muxer; max 512 streams (receive window not settable at the pin)      |
 | Noise XX          security; authenticates the remote PeerId                            |
 | upgrade           multistream-select V1Lazy per step; whole upgrade times out at 15 s  |
 | DNS over TCP      system resolver, tokio TCP, nodelay   (QUIC compiled, disabled)      |
 +----------------------------------------------------------------------------------------+
```

Read bottom-up: each TCP connection is upgraded to Noise XX, then yamux; every behaviour runs on
yamux streams.

- **Transport** (`crates/libp2p/src/transport.rs:55-97`): `quic.enabled = true` returns
  `QuicNotYetWired`. **Limits** (`crates/libp2p/src/limits.rs:39-47`) are hard-coded, not derived
  from `[peer_manager]`; they are the swarm-level cap, while the peer manager's `max_peers` /
  `max_outbound` are soft scheduler intents and its `max_inbound` is never read.
- **Gossipsub snappy** (`crates/libp2p/src/snappy.rs:41-62`, [ADR-P2-06](../adr/ADR-P2-06.md)):
  a `DataTransform` with `FrameDecoder` under `take(max + 1)`, so swarm-task decompression cannot
  exceed 10 MiB. Untested observation: consensus-specs `p2p-interface.md` ("Encodings") uses snappy
  *block* compression for gossip (frames only for req/resp); ADR-P2-06 chose frames. Unexercised:
  production has no topics, and the devnet meshes cc-p2p only with itself.
- **Req/resp codec** (`crates/libp2p/src/ssz_snappy_codec.rs`): `<varint><snappy-framed SSZ>`,
  10 MiB payload, TTFB 5 s, RESP 10 s, response cap 32 MiB (64 KiB for control protocols).
- **GossipSub peer scoring**: nothing calls `with_peer_score`, so the values in
  [p2p-scoring.md](../p2p-scoring.md) are PARAMS-ONLY. **identify / libp2p ping** answer peers,
  but their events are ignored (`services/p2p/src/host.rs:567-572`): no RTT, no latency scoring.

## 4. Identity: node key and ENR

| Item | Behaviour | Status |
|---|---|---|
| `node_key` | 32 raw secp256k1 bytes; one secret is both the libp2p `PeerId` and the discv5 `NodeId` (`services/p2p/src/identity.rs:224-248`). Default `./data/node_key`; compose `/app/data/node_key` on volume `cc-p2p-identity` | LIVE |
| Create | `create_new` with mode 0600, `write_all`, `fsync`, explicit `set_permissions(0600)`, then re-check the mode (`services/p2p/src/identity.rs:188-209`, `:285-327`) | LIVE |
| Load | refuse unless mode is **exactly** 0600 (0400 is refused too), length is 32, the secret is valid, and the path has no `..` (`services/p2p/src/identity.rs:133-156,260-283`) | LIVE |
| Shape B key creation | beacon-core creates a missing node key without enforcing 0600 (`bin/beacon-core/src/boot.rs:290-316`) | shape A: ABSENT; shape B: LIVE (as wired; not deployed) |
| Storage pairing | the storage service mounts the same volume read-only at `/identity` for I-node-id ([ADR-P4-13](../adr/ADR-P4-13.md)) | LIVE (storage service side) |
| `<node_key>.seq` | 8-byte LE `enr_seq`, mode 0600, write-then-rename; written only by `enable_persisted_seq` / `from_key_and_listen_persisted` (`services/p2p/src/discovery/enr.rs:921-955`), which have no non-test caller | DEAD-AS-WIRED |

Production uses `EnrManager::from_key_and_listen` (`services/p2p/src/discovery/task.rs:266-277`),
which has no seq path: the ENR seq restarts on every process start and discovery respawn, then
bumps once for the Phase-2 defaults (`attnets`, `syncnets`, `cgc`) and once for `eth2` + `nfd`
(`:438-443`). The `enr_seq` comments in `docker-compose.yml:62,235` and `docs/running.md` are
stale. MetaData `seq_number` stays 0.

The ENR advertises `ip4 = discovery.listen_ip` (default `0.0.0.0`), `udp4` and `tcp4`
(`services/p2p/src/discovery/enr.rs:853-871`); if `listen_udp` is 0 or 9000 it is replaced by the
TCP port from `listen_multiaddr` (`services/p2p/src/service.rs:488-493`). Shape A does not publish
the libp2p port; inbound peer reachability is not configured.

## 5. Discovery and dialing

### 5.1 The discovery task

`run_discovery_task` (`services/p2p/src/discovery/task.rs:425-620`) owns the `EnrManager` (the
single writer of the local ENR) and the discv5 handle:

- **Switch**: `enable_discovery` (`config/p2p.toml:17`; default true,
  `services/p2p/src/main.rs:163-164,190`). When false, `serve` builds no discovery task: the
  `discovery` slot runs a `pending()` placeholder and `dial_tx` is dropped
  (`services/p2p/src/service.rs:483,547-551`), so no discv5 UDP bind and no bootnode dials; the
  only dials are `[peer_manager].static_peers`, empty by default.
- **Bootnodes**: `discovery.boot_nodes` (9 Hoodi ENRs in `config/p2p.toml`) plus an optional
  `boot_nodes_file`. They are a config root of trust: no digest filter at table insert. At task
  start they are also enqueued for dialing directly (`services/p2p/src/discovery/task.rs:483-496`).
- **Queries**: generic `find_node_predicate` at the cadence shown in
  [5.2](#52-discovery---dial---connect---handshake) (`services/p2p/src/discovery/task.rs:43-47`;
  the timer is rebuilt only when the cadence flips),
  awaited inline in the select arm, so epoch, event and shutdown handling stall meanwhile.
- **Subnet queries**: attnet queries run only for deficits in `interested_attnets`, which is
  hard-coded to 0 (`services/p2p/src/main.rs:390`), so none run. The sync and column predicates
  (`services/p2p/src/discovery/predicate.rs:50-72`) are TEST-ONLY.
- **Epoch ticks**: `fork_ctx.on_epoch` then `apply_fork_context`, which rewrites `eth2` + `nfd`
  in one seq bump and is a no-op when unchanged (`services/p2p/src/discovery/enr.rs:984-993`).

### 5.2 Discovery -> dial -> connect -> handshake

```text
 [discovery task]
   bootnodes (9 Hoodi ENRs) --> discv5 routing table, UDP 9000
   + at start: bootnode ENRs --> enqueue_discovered --> flush up to 256 (initial dials)
   query tick: 5 s while connected < target_peers (50), else 30 s
     find_node_predicate(random target, allowed digests, 16)       (awaited inline)
   discv5 events: Discovered | SessionEstablished
        |
        v
   enqueue_discovered: ENR eth2 digest in the allowed set?  secp256k1 key -> PeerId?
                       tcp4 (else tcp6) address?  not already connected or dialing?
        |  priority = 1 + sparse attnets/syncnets the ENR claims + 1 if cgc > 4
        v
   DialQueue: max-heap of 256, dedupe by PeerId keeping the higher priority; full -> drop
        |  flush 256 at start, 32 per event, 64 per query tick
        |  --> dial mpsc(256) try_send; full -> drop
        v
 [peer_manager]
   offer_discovered: skip banned; record addr; priority kept in custody_usefulness
        |  schedule_dials (on each offer, conn event and 1 s tick): only while
        |  connected < target_peers and < max_peers; <= 8 dials in flight; outbound room;
        |  static peers first, then rows by custody_usefulness
        v  cmd try_send(Dial); full -> row rolled back to Disconnected, dial dropped
 [swarm task]
   swarm.dial(peer_id, [addr]) --fail--> DialFailure --> backoff 30 s, 60 s, ... cap 10 min
        |  DNS/TCP -> Noise XX -> yamux within 15 s; connection_limits enforced
        v
   ConnectionEstablished --> conn(256), never dropped --> peer_manager
        |                                         (banned peer -> ClosePeer{ban: true})
        v
   HandshakeBook.on_connect --> outbound status/2 (local digest, ChainView, advertised window)
   status/2 (request or response) with another fork digest --> Goodbye + disconnect (swarm)
   each new epoch (status-epoch --> StatusEpoch): ForkContext.on_epoch, re-send status/2
```

All steps are LIVE in shape A. Arrows show control flow, not the edge legend. Status payloads,
MetaData and cgc policy: [11](11-p2p-reqresp-and-sync.md).

Allowed digests are the current digest, plus the next one in the epoch before a boundary, plus
the previous one at the boundary (`services/p2p/src/fork_digest.rs:157-181`). Sparse means fewer
than `min_peers_per_subnet` (3) connected peers claim the subnet; with `interested_attnets = 0`
every attnet counts (`services/p2p/src/discovery/task.rs:192-223`). An ENR `nfd` mismatch is
never a disconnect input (`services/p2p/src/peer_manager/mod.rs:166-173`); only the Status
`fork_digest` disconnects. `reject_low_cgc_peers = false` accepts low-cgc peers.

### 5.3 Subnet manager and cgc hook

`SubnetManager` (`services/p2p/src/discovery/subnet_manager.rs:437-592`) and the five-effect
`set_custody_group_count` (`services/p2p/src/discovery/cgc_hook.rs:181-247`) are TEST-ONLY, so ENR
and MetaData keep `attnets = syncnets = 0` and `cgc = 4`; the RPC is a STUB
([8.1](#81-served-ethp2pv1p2pservice-on-9002)). The attestation backbone values in
`crates/config/src/attestation_subnets.rs:24-32` (`subnets_per_node` 2,
`epochs_per_subnet_subscription` 256, `attestation_subnet_count` 64) are consumed, as
`AttestationSubnetConfig`, only by the TEST-ONLY `SubnetManager` (the count constant also sizes
the gossip topic range, `services/p2p/src/gossip/mod.rs:66`), so production attnets stay 0 and
`interested_attnets` is hard-coded 0 (`services/p2p/src/main.rs:390`).

## 6. Peer manager: scheduling, scoring, bans

`PeerManager` (`services/p2p/src/peer_manager/mod.rs:420-871`) is the only component that
schedules dials or bans, and it owns every score-driven close. The swarm task also closes
connections itself: Goodbye + disconnect on a Status `fork_digest` mismatch, in the request or
the response (`services/p2p/src/host.rs:1098-1104`, `:1149-1156`); disconnect after an inbound
Goodbye (`:976-983`) and after an outbound req/resp Timeout, ConnectionClosed or Io
(`:601-615`). A low-cgc MetaData would close the connection the same way if
`reject_low_cgc_peers` were true (default false). Knobs come from `[peer_manager]`:
`target_peers` 50, `max_peers` 100, `max_inbound` 60 (never read), `max_outbound` 60,
`max_concurrent_dials` 8, `static_peers` [].

| Mechanism | As built | Status |
|---|---|---|
| Dial scheduling | `schedule_dials` (`services/p2p/src/peer_manager/dial.rs:32-129`), see [5.2](#52-discovery---dial---connect---handshake) | LIVE |
| Dial backoff | 30 s, x2 per failure, cap 10 min; reset on connect (`services/p2p/src/peer_manager/ban.rs:11-15,73-82`) | LIVE |
| App score | -100..+100; penalties `gossip_invalid` -10, `import_invalid` -25, `reqresp_fault` -5, `custody_unserved` -15, `behavioural` -5, `rate_limit` -5 (`services/p2p/src/peer_manager/score.rs:60-69`) | LIVE |
| Decay + coupling | every 12 s: `app += clamp(gossip / 1000, -5, 0)`, then `app *= 0.98` (`services/p2p/src/peer_manager/mod.rs:658-684`; [ADR-P2-09](../adr/ADR-P2-09.md)) | decay LIVE; coupling INERT (`gossip_score` never set) |
| Enforcement | `app < -50` -> `ClosePeer{ban: true}`; `app < -20` -> `ClosePeer{ban: false}` (`services/p2p/src/peer_manager/score.rs:203-218`) | LIVE |
| Bans | in-memory `HashSet`, no expiry, never unblocked; lost on restart (`services/p2p/src/peer_manager/ban.rs:89-121`) | LIVE |
| Eviction | only at `max_peers` with a better static candidate: lowest app score, then lowest `custody_usefulness`, then youngest (`services/p2p/src/peer_manager/mod.rs:385-414,632-646`) | INERT (`static_peers` empty in `config/p2p.toml` and compose, so `better_static_candidate` is always None, `services/p2p/src/peer_manager/mod.rs:828-848`; mesh protection INERT: `in_mesh` never set) |

The only live penalty source is the outbound req/resp failures above (the inbound rate limiter is
INERT, [11](11-p2p-reqresp-and-sync.md#rate-limiting)); the `penalty` channel is IDLE. Policy
commands (`ClosePeer`, `BlockPeer`) use `cmd_tx.send().await` and are never dropped; `ClosePeer`
is one command, so Goodbye, disconnect and ban cannot split
(`services/p2p/src/peer_manager/mod.rs:736-776`). `Dial` is best-effort, and dials stop at
`target_peers` (`services/p2p/src/peer_manager/dial.rs:42-44`), so only inbound peers could reach
`max_peers`. `custody_usefulness` holds the discovery priority, not custody coverage. Score
spaces and the PARAMS-ONLY GossipSub thresholds: [p2p-scoring.md](../p2p-scoring.md).

## 7. Fork digest and clock

- **Fork digest** (`services/p2p/src/fork_digest.rs:78-100`; EIP-7892 derivation and its traps
  in [06 §4](06-consensus-primitives.md#4-eip-7892-fork-digest)): the only allowed fork-schedule
  walk outside `cc-types`. Used for ENR `eth2` / `nfd`, the allowed-digest filter and Status;
  never for topic strings in production (no subscriptions).
- **Inputs**: `network_config` (`config/p2p.toml`: the bundled Hoodi YAML) else the embedded
  Hoodi config, and `genesis_validators_root` (else the zero root)
  (`services/p2p/src/service.rs:170-205`). Two `ForkContext` copies advance on epoch ticks: the
  swarm's handshake copy (via `StatusEpoch`) and discovery's copy.
- **Clock**: `SlotClock` is built from `[clock]` (Hoodi `genesis_time = 1742213400`, 12 s
  slots, 32-slot epochs, 500 ms gossip disparity) and has no setter, so ChainView never re-times
  it. `epoch-ticks` polls once per slot and publishes the epoch on a watch
  (`services/p2p/src/clock.rs:259-281`).
- **Clock offset**: `[clock].slot_clock_offset_seconds` (`config/p2p.toml:40`, default 0) shifts
  the wall clock `SlotClock` reads (`services/p2p/src/clock.rs:207,231`), so it moves the current
  slot and epoch, hence `epoch-ticks` and the fork-digest epoch; slot start times are not shifted
  (`:238-243`). The code calls it devnet-only, but the dangerous-knob guard does not gate it: p2p
  never calls `check_dangerous_knobs`, which covers three knobs of chain and the storage service
  (`crates/config/src/devnet_guard.rs:1-7`;
  [01](01-deployment-and-processes.md#dangerous-knob-guard)).

## 8. gRPC surface and cross-process edges

### 8.1 Served: `eth.p2p.v1.P2pService` on :9002

| RPC | Status | Behaviour |
|---|---|---|
| `GetInfo` | LIVE | `BuildInfo{service = "p2p", version, git_sha, rustc}` (`services/p2p/src/service.rs:1175-1187`) |
| `SetCustodyGroupCount` | STUB | `UnattachedCgcHook` -> `FAILED_PRECONDITION` "cgc hook not attached" (`services/p2p/src/service.rs:1086-1095,1189-1208`) |
| `EngineStream` (bidi) | DEAD-AS-WIRED | server attached (`services/p2p/src/main.rs:503-508`); the engine container's client was deleted in 631994c; inject uses `NoopPublisher` and its own sampling tracker with no DA emit; unauthenticated (`AuthMode::Unauthenticated`, `services/p2p/src/engine_stream/server.rs:63-66`): any container on the `cc` network can open it, and each `InjectColumns` runs KZG verification inline on its session task (`:244`) |
| `grpc.health.v1.Health` | LIVE | self-name `eth.p2p.v1.P2pService` SERVING from bind; aggregate `""` SERVING iff chain and the storage service both probe up (`ServeOptions::default()`, so no local-ready gate) |
| reflection v1 | LIVE | `cc_proto::FILE_DESCRIPTOR_SET` |

`known_methods` omits EngineStream, so its calls would be labelled `method="unknown"`
(`services/p2p/src/main.rs:458`). Swarm stalls or zero peers do not affect aggregate health; a chain
or storage service outage does. Compose gates p2p startup on `chain` healthy only (bound, not
bootstrapped). p2p has no liveness sampler; its aggregate health flips only on peer probes.
Liveness flips aggregate health; nothing in this repository restarts a process on that signal.

### 8.2 Dialed by p2p

p2p dials chain ([ADR-07](../adr/ADR-07.md)); chain is the server and the health DAG roots at
chain. Arrows follow data direction; p2p is the dialer for every row.

| Edge | Arm / RPC | Data flow | Shape A status | Evidence |
|---|---|---|---|---|
| E1 | `P2pStream` session + `ChainView` | chain -> p2p | LIVE (content static while chain is core-absent, the compose default) | `services/p2p/src/chain_stream/client.rs:212-288` |
| E1 | gossip object / verdict | p2p -> chain | IDLE (0 topics) | `services/p2p/src/host.rs:1253-1260` |
| E1 | publish | chain -> p2p | DEAD-AS-WIRED (`request_publish` never called) | `crates/chain-core/src/p2p_stream.rs:228` |
| E1 | column | p2p -> chain | DEAD-AS-WIRED | no p2p producer |
| E2 | `data_available` | p2p -> chain | DEAD-AS-WIRED | no `notify_data_available` caller |
| N2 | `GetValidatorRecords` | chain -> p2p | IDLE (reached only from gossip validation) | `services/p2p/src/chain_stream/records.rs:105-112` |
| E6 | `WatchServeWindow` | storage service -> p2p | LIVE (content static: `u64::MAX`) | `services/p2p/src/storage_client.rs:389-538` |
| E6 | `GetBlocksBy*` / `GetColumnsBy*` | storage service -> p2p | DEAD-AS-WIRED (`_client` dropped) | `services/p2p/src/service.rs:389-391` |
| E5 | `PutBackfillBatch` | p2p -> storage service | DEAD-AS-WIRED (no client method) | `services/p2p/src/storage_client.rs:214-371` |
| N3 | health `Check` | peer status | LIVE (3 s interval, 1 s timeout) | `crates/bootstrap/src/prober.rs:23-27` |

- **E1** is enabled iff `[peers].chain` is set (compose: `http://chain:9001`). The never-fatal
  `chain-stream-client` wraps `cc_seam::Ipc::connect_with`
  (`services/p2p/src/chain_stream/client.rs:239`; timeouts and backoff in
  [11](11-p2p-reqresp-and-sync.md#chain-stream-client-e1-n2)). Received `ChainView`s feed the
  Status handshake ([ADR-P2-05](../adr/ADR-P2-05.md)). In default compose chain sends one
  `VIEW_KIND_FULL` view of default fields, so Status advertises zero head and finalized fields.
  Contract details: [03-internal-contracts.md](03-internal-contracts.md).
- **E6** is enabled iff `[peers].storage` is set (compose: `http://storage:9006`). Each message
  writes the advertised window (`Arc<ServeWindow>`, one `AtomicU64`, one writer;
  [ADR-P2-14](../adr/ADR-P2-14.md)). On disconnect it holds the last value for
  `window_stale_grace_secs` (60), then collapses to the cache floor
  (`cc_p2p_window_collapsed_total`). No backfill cache binds that floor in production, so it
  stays `EMPTY_WINDOW_SLOT = u64::MAX` (`services/p2p/src/service.rs:376-378`,
  `services/p2p/src/backfill/window.rs:24`), the value the storage service already emits (the
  empty storage serve window). The advertised window is `u64::MAX` whether the storage service is up or down;
  a collapse changes only the counter.

## 9. Metrics

`/metrics` on :9102 (compose publishes `127.0.0.1:9102`): the shared `cc-bootstrap` families
(`cc_build_info`, `cc_grpc_*`, `cc_peer_health{service,peer}`) plus, from
`services/p2p/src/metrics.rs:455-640`:

| Area | Families | Status |
|---|---|---|
| Host | `cc_p2p_worker_panics_total{task}`, `cc_p2p_swarm_stall_seconds`, `cc_p2p_queue_depth{q}` | LIVE (`cc_p2p_swarm_stall_seconds` holds milliseconds despite its name, `services/p2p/src/metrics.rs:1118-1123`, and is effectively static: `chain_out` never fills; `queue_depth` gossip-side labels are IDLE and `q=kzg` is DEAD-AS-WIRED, [10 §13](10-p2p-gossip-and-das.md#13-concurrency-metrics-and-latent-defects)) |
| Peers | `cc_p2p_peers{direction}`, `cc_p2p_app_score`, `cc_p2p_peer_score`, `cc_p2p_peer_penalty_total{reason}`, `cc_p2p_peers_below_threshold`, `cc_p2p_peers_custody_compatible` | LIVE (gossip score always the default 0; `custody_compatible` counts discovery priority, not custody, `services/p2p/src/peer_manager/mod.rs:807-826`) |
| Req/resp | `cc_p2p_reqresp_{inbound,outbound}_total`; `cc_p2p_reqresp_ratelimit_total` | LIVE; ratelimit INERT (the bucket never empties, [11](11-p2p-reqresp-and-sync.md#metrics-in-scope)) |
| Window | `cc_p2p_earliest_available_slot`, `cc_p2p_window_collapsed_total` | LIVE (content static) |
| Chain stream | `cc_p2p_chain_stream_saturation_ratio`, `cc_p2p_verdict_*`, `cc_p2p_chain_*_total` | IDLE (no objects) |
| Gossip | `cc_p2p_gossip_messages`, `cc_p2p_gossip_shed`, `cc_p2p_inclusion_proof_verifications` | IDLE (0 topics; [10 §13](10-p2p-gossip-and-das.md#13-concurrency-metrics-and-latent-defects)) |
| Unwritten gossip / DAS / backfill | `cc_p2p_gossip_validation_seconds`, `cc_p2p_idontwant_total`, `cc_p2p_gossip_duplicate_bytes_total`, `cc_p2p_columns_received_total`, `cc_p2p_da_*`, `cc_p2p_sampling_seconds`, `cc_p2p_backfill_*`, `cc_p2p_cache_*` | DEAD-AS-WIRED (no production writer; `idontwant` and `duplicate_bytes` are only primed, `services/p2p/src/metrics.rs:1162-1175`; [10 §13](10-p2p-gossip-and-das.md#13-concurrency-metrics-and-latent-defects), [11](11-p2p-reqresp-and-sync.md#metrics-in-scope)) |
| libp2p | `cc_p2p_libp2p_libp2p_*` (sub-registry) | DEAD-AS-WIRED: the `Metrics` object is dropped |

## 10. Fault modes and devnet mode

`run_devnet` (`services/p2p/src/fault_mode.rs:907-1180`) is a second, DEVNET-ONLY host selected
by `--publish-fixture` (publisher role) or `--devnet-peer`, used by `devnet/compose.yml`
(`anchor`, `publisher`, `node-a`, `node-b`). Operating the devnet:
[devnet/README.md](../../devnet/README.md).

- One async loop owns the swarm directly (swarm events, 200 ms publish tick, 5 s re-dial tick,
  ctrl-c). No supervisor, peer manager, discovery, chain-stream or storage-service client, and
  no gRPC: `grpc_addr` is parsed but never bound, despite the comment at
  `services/p2p/src/fault_mode.rs:856`.
- The node key must already exist; this loader checks the length, not the 0600 mode.
- It subscribes `beacon_block` + 128 `data_column_sidecar_*` topics at the epoch-0 digest and
  ACCEPTs every message unvalidated.
- `--fault-mode` selects `None`, `WithholdColumn{columns}` or `Misbehave{kind}`
  (`services/p2p/src/fault_mode.rs:233-246`); a stall-reqresp fault `std::thread::sleep`s on a
  runtime worker.

## 11. Stale comments and latent issues

- Stale: "Listens; does not dial" (`services/p2p/src/main.rs:4-5`) and "No dialling"
  (`services/p2p/src/service.rs:3-4`); the forward reference "Discovery-driven dials are CC-21c"
  (`services/p2p/src/host.rs:182`); "+ PutBackfillBatch" on `StorageClient`
  (`services/p2p/src/storage_client.rs:210`); "table is rebuilt from connection events"
  (`services/p2p/src/service.rs:441`; the respawn is INERT); the single-writer claim at
  `services/p2p/src/discovery/subnet_manager.rs:1-26` (the module is TEST-ONLY); the `enr_seq`
  volume comments ([section 4](#4-identity-node-key-and-enr)); in `docs/p2p-dependencies.md`,
  libp2p ping as RTT input, one `request_response` behaviour at `:182` (there are five,
  `crates/libp2p/src/behaviour.rs:89-99`) and the advertised window from the CC-26a cache at `:108`
  (WatchServeWindow is the only writer); the "after swarm panic" log, which also fires for
  discovery budget exhaustion.
- Reachable at HEAD (traced, not observed): as wired, every Goodbye-then-disconnect (peer-manager
  `ClosePeer`, `services/p2p/src/host.rs:1289-1306`; handshake reject, `:1098-1104`) leaves the
  Goodbye request pending when the connection closes, so rust-libp2p emits
  `OutboundFailure::ConnectionClosed` and the swarm charges a spurious `reqresp_fault` (-5) to the
  already-closed peer and counts it in `cc_p2p_peer_penalty_total` (`:601-615`). The peer-manager
  row survives the disconnect (`services/p2p/src/peer_manager/mod.rs:294`), so the penalty lands.
- Latent; unreachable at HEAD because no gossip topics are subscribed: the `late_open` pin race,
  the `cc_p2p_peer_penalty` double count.

## Planned changes

All items below are target design from [`plan/architecture.md`](../../plan/architecture.md);
none is complete at `7d8833d`. Where a prerequisite already exists, the bullet says so.

- **E1/E2 transport selection at S3** (§2.5, §9.1 S3, §10.4): today p2p reaches chain through
  `cc_seam::Ipc` (gRPC); `cc_seam::InProcess` exists but is TEST-ONLY. Planned: S3 selects one
  impl, and an `Ipc` over a unix socket with `SO_PEERCRED` is an option (§6.3). Already built:
  the named `cc-chain` <-> `cc-p2p` dependency ban (`scripts/check-crate-dag.sh:95-116`); at
  HEAD only its manifest grep (`:300-321`) runs, because the script exits 1 before the metadata
  walk ([02](02-crate-map.md#mechanical-guards)). Planned: [ADR-07](../adr/ADR-07.md)'s
  direction is decided again.
- **Gossip and Loop A** (§2.3 E1 row, §3.4, §3.5, §9.1 S3, Appendix B Q-8): at HEAD p2p
  subscribes to no gossip topics; the plan schedules gossip wiring for a later stage (its S3 exit
  gate is a foreign-peer block import on Hoodi). §3.4 lists, before S3: reconnect `kzg_tx`
  (`services/p2p/src/service.rs:705`), shorten the 12 s local verdict wait, and hoist the redrive
  walks off the hot path; after S3, split the validation loop into typed queues. See
  [10-p2p-gossip-and-das.md](10-p2p-gossip-and-das.md).
- **GossipSub scoring** (§10.4, [ADR-P2-10](../adr/ADR-P2-10.md) row): wire it at S3.
- **Edges** (§2.3): E5 becomes `storage_core::backfill::admit()` behind `ArchiveWrite`; E6
  becomes an `AtomicU64` read; E8's inject becomes a second `P2pEgress` method.
- **Codec** (§1.4, §9.1 S3): one `cc-wire` SSZ+snappy codec replaces the duplicates in
  `services/p2p/src/reqresp/` and `crates/libp2p/src/ssz_snappy_codec.rs`.
- **Devnet path** (§8.3, §9.1 S3): remove `services/p2p/src/fault_mode.rs` from production paths.
