# Req/resp, sync and backfill

> **As built at `7d8833d`** (`develop`, 2026-08-16). Status labels and diagram legend: [README](README.md#status-labels).
> Target design: [`plan/architecture.md`](../../plan/architecture.md) §2.3, §2.5, §3.6, §4.3, §9.1.

This document covers the request/response side of `cc-p2p`: the nine Fulu req/resp protocols,
the Status handshake, block and column serving (not yet live), backfill, and the gRPC links that
feed or would feed them: the **chain-stream client** (E1, N2), the **EngineStream server** (E8)
and the **storage client** (E6, E5). Related: gossip and DAS in [10](10-p2p-gossip-and-das.md),
host and discovery in [09](09-p2p-host-and-discovery.md), P2pStream in [03 section 4](03-internal-contracts.md#4-p2pstream-anatomy),
the storage service's serving in [07 section 4.7](07-storage.md#47-serve-and-the-storage-serve-window-shape-a).
Short paths such as `host.rs` or `reqresp/server.rs` are under `services/p2p/src/`; a bare file
name or `:line` repeats the path last given in the same section or table row.

Everything here runs only in **shape A** (`docker-compose.yml`). Shape B (`cc-beacon-core`) hosts
no p2p, so every edge below is ABSENT there. No shipped configuration pairs cc-p2p with
cc-beacon-core; how p2p attaches to shape B is not yet defined. Both `[peers]` entries are set by
the TOML defaults (`config/p2p.toml:43,45`) and by compose, so the chain-stream client and the
storage client run in every shipped configuration (outside the DEVNET-ONLY mode).

## Status at a glance

| Component | Code | Status (shape A) |
|---|---|---|
| Nine req/resp protocols, `ssz_snappy` codec | `services/p2p/src/reqresp/mod.rs`, `crates/libp2p/src/ssz_snappy_codec.rs` | LIVE |
| Status v2 / Goodbye v1 / inbound Ping v1 | `reqresp/handshake.rs`, `services/p2p/src/host.rs:969-1133` | LIVE |
| MetaData v3 (local) | `services/p2p/src/service.rs:362-363` -> `reqresp/handshake.rs:87` | LIVE (content static: `seq=0, attnets=0, syncnets=0, cgc=4`) |
| Outbound Ping (`initiate_ping`) | `reqresp/handshake.rs:265-273` | DEAD-AS-WIRED (no caller) |
| Inbound rate limiter (block/column ids) | `reqresp/limits.rs` | INERT (see [Rate limiting](#rate-limiting)) |
| Block and column serve, all five ids | `host.rs:745-752`, `:836-842` | STUB (code 3 `"handler not ready"`) |
| Serve planners (`plan_block_response`, `plan_column_response`) and chunk budget | `reqresp/blocks.rs:577`, `reqresp/columns.rs:645`; called at `host.rs:795`, `:887`, `:934` | DEAD-AS-WIRED (`block_serve` is never `Some`) |
| `BackfillCache`; `serve_block_protocol` / `serve_column_protocol` | `backfill/cache.rs`; `reqresp/server.rs:110`, `:238` | TEST-ONLY |
| Column by-root serve from a fixture store (`--publish-fixture` / `--devnet-peer`) | `services/p2p/src/fault_mode.rs:1213-1348` | DEVNET-ONLY |
| `reqresp_in` copy and its `stub-reqresp` consumer | `host.rs:636-645`, `service.rs:741-744` | STUB |
| Advertised window (`Status.earliest_available_slot`) | `backfill/window.rs` | LIVE (content static: `u64::MAX`) |
| `RequestScheduler`, `OutboundLimiter`; backfill gap detector, planner, below-anchor verifier, outbound budget | `reqresp/client.rs`, `reqresp/limits.rs:369`; `backfill/{planner,below,rate}.rs` | TEST-ONLY |
| E1 `P2pStream` session + ChainView (chain -> p2p; p2p dials) | `chain_stream/`, `crates/seam/src/ipc.rs` | LIVE |
| E1 object / verdict arm; N2 `GetValidatorRecords` | same stream; `chain_stream/records.rs` | IDLE (0 gossip topics) |
| E1 publish arm (chain -> p2p); E1 column arm and E2 `data_available` (p2p -> chain) | same stream | DEAD-AS-WIRED |
| E6 `WatchServeWindow` (storage service -> p2p; p2p dials) | `services/p2p/src/storage_client.rs:389-538` | LIVE (content static) |
| E6 serve reads `Get{Blocks,Columns}By{Range,Root}`; E5 `PutBackfillBatch` (p2p -> storage service) | `storage_client.rs:273-370`; E5: no client method | DEAD-AS-WIRED |
| E8 `EngineStream` server (engine container -> p2p) | `engine_stream/` | DEAD-AS-WIRED (served, never dialed) |

## Req/resp stack

```text
   remote peer opens one libp2p stream per request (TCP 9000; transport in doc 09)
                                       |
                                       v
 +------------------------------------------------------------------------------+
 | CcBehaviour (crates/libp2p/src/behaviour.rs)                                 |
 |  4 control behaviours, one id each: status/2 goodbye/1 ping/1 metadata/3     |
 |  1 multi behaviour, 5 ids: beacon_blocks_by_range/2, by_root/2,              |
 |    by_head/1 (Inbound only), data_column_sidecars_by_range/1, by_root/1      |
 |  request timeout 15 s (TTFB + RESP), services/p2p/src/reqresp/mod.rs:289-293 |
 +------------------------------------------------------------------------------+
 | SszSnappyCodec (crates/libp2p/src/ssz_snappy_codec.rs)                       |
 |  request read: <varint len><snappy frames>, (min,max) checked before         |
 |    inflating; no codec timeout on inbound requests                           |
 |  our outbound reads: TTFB 5 s, RESP 10 s idle; cap 64 KiB ctl / 32 MiB       |
 +------------------------------------------------------------------------------+
                                       |  RequestResponseEvent::Message
                                       v
 +------------------------------------------------------------------------------+
 | swarm task: handle_inbound_request (services/p2p/src/host.rs:625)            |
 |  a. copy try_send -> reqresp_in mpsc(256) -> stub-reqresp drops it    STUB   |
 |  b. block/column ids: admit_request; empty bucket -> code 2           INERT  |
 |  c. status/ping/metadata/goodbye -> HandshakeBook inline -> reply     LIVE   |
 |  d. block/column ids: block_serve None -> code 3 "handler not ready"  STUB   |
 |  e. any other id -> code 3 (defensive; not negotiable on the wire)           |
 +------------------------------------------------------------------------------+
                                       |
                                       v
           send_reqresp_response -> write_response (<= 32 MiB, any id) -> close
```

All req/resp work runs **inline in the swarm task**, the sole `Swarm` owner ([ADR-P2-02](../adr/ADR-P2-02.md)).
Shape A does not publish the libp2p port; inbound peer reachability is not configured.

### Protocols

Protocol ids and limits: `services/p2p/src/reqresp/mod.rs:80-139`, `:195-245`, `:272-284`.

| Protocol id (`/eth2/beacon_chain/req/.../ssz_snappy`) | libp2p support | Request SSZ bytes | Context bytes | At HEAD |
|---|---|---|---|---|
| `status/2` | Full, own behaviour | 92..92 | none | LIVE |
| `goodbye/1` | Full, own behaviour | 8..8 | none | LIVE (disconnect, then empty response) |
| `ping/1` | Full, own behaviour | 8..8 | none | LIVE (answered; never initiated) |
| `metadata/3` | Full, own behaviour | 0..0 (empty body) | none | LIVE (content static) |
| `beacon_blocks_by_range/2` | Full, multi | 24..24 | ForkDigest | STUB (code 3) |
| `beacon_blocks_by_root/2` | Full, multi | 0..32768 | ForkDigest | STUB (code 3) |
| `beacon_blocks_by_head/1` | **Inbound only** | 40..40 | ForkDigest | STUB (code 3) |
| `data_column_sidecars_by_range/1` | Full, multi | 20..1044 | ForkDigest | STUB (code 3) |
| `data_column_sidecars_by_root/1` | Full, multi | 0..10 MiB | ForkDigest | STUB (code 3) |

Response chunk bounds: Status 92, Goodbye 0, Ping 8, MetaData 8..256, blocks and columns
0..10 MiB (`reqresp/mod.rs:229-245`). Response codes are 0 Success, 1 InvalidRequest, 2 ServerError
(also used for rate limiting), 3 ResourceUnavailable (`reqresp/codec.rs:66-78`). The helpers in
`reqresp/codec.rs` frame the swarm task's responses with a per-chunk ForkDigest (`:454`).

### Codec

- **On the wire** (`crates/libp2p/src/ssz_snappy_codec.rs`): a request is bounded by `(min, max)`
  before decompressing and gets no codec timeout, only the 15 s behaviour timeout (`:109-140`).
  When we read the reply to one of our own requests, the codec applies TTFB 5 s, then 10 s idle
  per read, under a stream cap of 64 KiB for control ids and 32 MiB otherwise (`:142-153`,
  `:227-241`, `:329-360`); our own replies are checked only against 32 MiB (`:175-195`).
- **Three copies of the limits table**: `reqresp/mod.rs:195`, `ssz_snappy_codec.rs:204-223` and
  `bin/serve-probe`. `request_limit_tables_agree` (`reqresp/mod.rs:357`) checks the first two
  against a literal of the probe's numbers. The probe keeps its own copy for its 5 ids
  (`bin/serve-probe/src/protocols.rs:74`) and links `cc-libp2p` for transport, not `cc-p2p`; its
  test `request_limits_match_libp2p_codec` (`:504`) compares that copy with
  `cc_libp2p::request_limits` ([ADR-P4-12](../adr/ADR-P4-12.md)).

## Status handshake

```text
  local cc-p2p (swarm task)                                          remote peer
   |                                                                        |
   |  ConnectionEstablished -> HandshakeBook::on_connect                    |
   |---- status/2 request {fork_digest, finalized, head, eas} ------------->|
   |<--- status/2 response -------------------------------------------------|
   |     digest differs? -> goodbye/1 reason 2 + disconnect                 |
   |<--- status/2 request (the peer runs the same machine) -----------------|
   |     digest differs? -> goodbye/1 reason 2 + disconnect, issued BEFORE  |
   |     the reply below is queued, so that reply may never arrive          |
   |---- status/2 response: our Status (code 1 if undecodable) ------------>|
   |<--- metadata/3 request (empty body) -----------------------------------|
   |---- metadata/3 response {seq 0, attnets 0, syncnets 0, cgc 4} -------->|
   |<--- ping/1 request {peer seq} -----------------------------------------|
   |---- ping/1 response {our seq = 0} ------------------------------------>|
   |---- metadata/3 request (no cached seq, or it differs; none pending) -->|
   |<--- metadata/3 response {seq, attnets, syncnets, cgc} -----------------|
   |     cgc < 4 and reject_low_cgc_peers? -> goodbye/1 reason 3            |
   |<--- goodbye/1 request {reason} ----------------------------------------|
   |     disconnect, drop handshake state, then queue the empty response    |
   |  each new epoch: status-epoch -> StatusEpoch -> fork_ctx.on_epoch      |
   |---- status/2 request (each active peer not yet sent one this epoch) -->|
   x ping/1 request from us: initiate_ping has no caller
```

Sequence diagram: each arrow is one message, arrowhead = receiver; indented lines are local
decisions; `x` = DEAD-AS-WIRED. In code, Goodbye+disconnect and the ping-triggered MetaData fetch
are enqueued before the reply (`host.rs:1032-1037`, `:1064-1075`), so as wired a mismatched peer's
Status reply is queued after the disconnect.

`HandshakeBook` (`services/p2p/src/reqresp/handshake.rs`) keeps per-peer state and returns
`OutboundAction`s that the swarm task executes (`services/p2p/src/host.rs:1088-1133`). Both sides
start a Status exchange on connect (`host.rs:379-405`; `handshake.rs:150-195`).

| Status v2 field (92 bytes, `reqresp/status.rs:20`) | Sole source | Value in default shape A |
|---|---|---|
| `fork_digest` | `ForkContext::current_digest()` (EIP-7892 digest, `services/p2p/src/fork_digest.rs`) | Hoodi digest for the current epoch |
| `finalized_root`, `finalized_epoch`, `head_root`, `head_slot` | `ChainViewStore` (E1 views) | zero: before the first view, and after it while chain runs core-absent (the compose default), because views then carry the empty `HeadSnapshot` (`crates/chain-core/src/head.rs:33-48`) |
| `earliest_available_slot` | advertised window `Arc<ServeWindow>` | `u64::MAX` (empty window) |

`build_local_status` reads exactly these three sources (`status.rs:105-118`). As wired, nothing ages
the stored view: if the chain stream drops, Status keeps advertising the last view
(`chain_stream/view.rs:56-58` has no reset); with `[peers].chain` empty the fields stay zero.

- **Digest rule.** `evaluate_peer_status` (`status.rs:146-156`); `nfd` is not an input. Responses
  are decoded by the *negotiated* protocol, never by body shape (`host.rs:1136-1183`).
- **MetaData.** Local: `LocalMetaData::new(CUSTODY_REQUIREMENT)` (`service.rs:362-363` ->
  `handshake.rs:87`), never mutated; the subnet manager and cgc hook are TEST-ONLY. Peer: through
  `CgcPolicy`, accepted by default, Goodbye(3) for cgc < 4 only when `reject_low_cgc_peers = true`
  (`reqresp/metadata.rs:200-245`; `config/p2p.toml:20`).
- **Ping** (`handshake.rs:247-262`; `reqresp/ping.rs:54-59`). A seq that differs from the cached
  MetaData seq, or any seq while none is cached, triggers one MetaData fetch.
- **Per-epoch.** `StatusEpoch` goes on `cmd_tx` (mpsc 512, `send().await`) for the peers in
  discovery's `active` view (connected or dialing, `service.rs:587`); the swarm task advances
  `fork_ctx`, then re-sends Status (`host.rs:1326-1343`).
- **Failures.** Outbound `Timeout` / `ConnectionClosed` / `Io` -> `PeerPenalty(ReqrespFault)` and
  disconnect; inbound failures only count (`host.rs:601-620`). Goodbye is followed at once by a
  disconnect (`host.rs:1098-1104`), so the Goodbye request itself may fail as `ConnectionClosed`
  and draw a spurious penalty. It can fire whenever the handshake sends a Goodbye (digest
  mismatch, e.g. around fork or BPO boundaries, or low cgc with `reject_low_cgc_peers = true`);
  traced, not tested.

## Server side and rate limiting

`handle_inbound_request` (`services/p2p/src/host.rs:625-733`) runs rows a-e of the stack diagram
in order. An admission failure counts `cc_p2p_reqresp_ratelimit_total` and raises
`ConnEvent::PeerPenalty(RateLimit)` (`:647-661`); block and column ids get code 3 at once because
`block_serve` is `None` (`:745-752`, `:836-842`; `SwarmTask::with_block_serve` has no caller,
`:166`). Server invariants, as designed: **never drop a `ResponseChannel`**; **never answer an
empty success for data we lack** (no cache -> code 3 `"handler not ready"`; cache miss with the
storage service unavailable -> code 3 `"storage backend unreachable"`, `host.rs:806-815`; an
in-window ByRange with no blocks is an empty success by design, `reqresp/blocks.rs:457-459`);
**rate limiting truncates, never errors mid-stream** (only an empty bucket at the start yields
code 2, `reqresp/limits.rs:256-295`).

### Rate limiting

| Limit | Value | Code | Status |
|---|---|---|---|
| Inbound (per peer, global) | blocks 128 chunks / 10 s, columns 1024 / 10 s, continuous refill; global 10 x per-peer | `reqresp/limits.rs:22-36` | INERT |
| Outbound self-limit | 1 in flight per (peer, protocol), 4 per peer | `OutboundLimiter`, `limits.rs:369` | TEST-ONLY |
| Backfill outbound budget | 128 blocks / 10 s sliding window per peer | `backfill/rate.rs:23-26`, `:128-150` | TEST-ONLY |

As wired the inbound limiter cannot limit: `admit_request` debits one token and refunds it
(`limits.rs:307-331`); only the DEAD-AS-WIRED `apply_chunk_budget` really debits (`host.rs:924-966`).

## Serving blocks and columns

The serve planners are wired into `host.rs` behind `block_serve`, which is never set
(DEAD-AS-WIRED). Each reads a `BackfillCache`, which is in-memory, capped at 1 GiB
(`services/p2p/src/backfill/cache.rs:31-49`) and constructed only in tests:

| Planner | Code | Bounds |
|---|---|---|
| Blocks ByRange v2 | `services/p2p/src/reqresp/blocks.rs:421-460` | `count <= MAX_REQUEST_BLOCKS_DENEB = 128` (`:34`); missing slots omitted |
| Blocks ByRoot v2 | `blocks.rs:466-502` | <= 128 roots; none found -> code 3 |
| Blocks ByHead v1 | `blocks.rs:508-574` | walks parents from the requested root, descending |
| Columns ByRange v1 | `services/p2p/src/reqresp/columns.rs:486-560` | <= 16384 sidecars (`:47-49`); a missing held column -> code 3 |
| Columns ByRoot v1 | `columns.rs:568-642` | <= 128 identifiers; fault seam `decide_by_root_column_serve` (`:419-442`) |

Each planner gates on the cache window (`check_slot_window`, `blocks.rs:294-312`;
`check_column_slot_window`, `columns.rs:340`): ByRange checks its start slot, blocks ByRoot skips
out-of-window roots, columns ByRoot refuses the whole request, ByHead checks the head root's slot
after the cache lookup; below the window or under the `u64::MAX` seed the answer is code 3. Floors:
`max(current - 33024, FULU_FORK_EPOCH)` epochs for blocks, `- 4096` for columns (`blocks.rs:43`;
`columns.rs:42`), duplicating `cc_store::window` ([serve-windows.md](../serve-windows.md)).
`reqresp/server.rs`'s `serve_*_protocol` run only in its tests; `host.rs:739-966` re-implements
them. Only the DEVNET-ONLY `--publish-fixture` / `--devnet-peer` swarm answers a column request:
by-root columns from a fixture store via `decide_by_root_column_serve`, every other id refused
(`services/p2p/src/fault_mode.rs:1213-1348`).

### Three "serve windows" and `earliest_available_slot`

| Name | What it is | Writer | Value at HEAD |
|---|---|---|---|
| **storage serve window** | `meta.serve_window` record that the storage service emits on `WatchServeWindow` | none in production (`crates/storage-core/src/serve.rs:281-338`) | `empty_window()`, eas `u64::MAX` (`serve.rs:1196-1235`). Gauge `cc_storage_earliest_available_slot` is written only by those dead publishers (`serve.rs:286`, `:320`), so it stays at its registration seed 0 (`crates/storage-core/src/metrics.rs:872`) |
| **advertised window** | p2p `Arc<ServeWindow>`, one `AtomicU64`, read by Status | `ServeWindow::store_recomputed`, `pub(crate)`, called only by the WatchServeWindow client (`services/p2p/src/backfill/window.rs:140`) | `u64::MAX` |
| **cache window** | `BackfillCache`'s own `ServeWindow` (`backfill/cache.rs:128`), read by the serve planners | nobody; the cache is never constructed | n/a |

The advertised window follows [ADR-P2-14](../adr/ADR-P2-14.md): one `AtomicU64`, one writer,
seeded `u64::MAX` (`window.rs:24`, `:88-145`). Readers only load it. The ADR also says every serve
handler reads that atomic; as wired the planners would read the cache window instead.

```text
 cc-storage :9006 -- storage serve window              cc-p2p :9002 -- advertised window
 +---------------------------------------+             +-------------------------------------------+
 | meta.serve_window: never written      |             | WatchServeWindow client (tokio::spawn,    |
 | -> empty_window(), eas = u64::MAX     |             |   iff [peers].storage is set)             |
 | window_tx watch; publish_window and   |=== E6 ====> | advertised_slot_from_serve_window()       |
 | derive_and_publish_window: unused     | (p2p dials) | -> ServeWindow::store_recomputed          |
 |                                       |             | stale 60 s -> collapse to cache floor     |
 |                                       |             |   (Arc never bound to a cache: u64::MAX)  |
 | GetBlocksByRange / ByRoot             |x E6 reads   | StorageClient: built, then dropped        |
 | GetColumnsByRange / ByRoot            |             |                                           |
 | PutBackfillBatch (served, no caller)  |x E5         | no put_backfill_batch method              |
 +---------------------------------------+             +-------------------------------------------+
                                                          advertised window  |  load
                                                                             v
                                                       Status v2 earliest_available_slot = u64::MAX

 BackfillCache (TEST-ONLY: never constructed)
 +-------------------------------------------+
 | cache window: the cache's own ServeWindow |   check_slot_window would gate every serve
 | (cache.rs:128); nothing writes it         |   planner on this window, not on the
 | -> u64::MAX seed: refuse everything       |   advertised one
 +-------------------------------------------+
```

`====>` LIVE stream (arrowhead = data direction); `x` DEAD-AS-WIRED arm. The TEST-ONLY cache box is
drawn because the planners would gate on its window; today row d of the stack diagram answers first.

## Client side: `RequestScheduler`

`services/p2p/src/reqresp/client.rs` is the intended single outbound scheduler for by-root
recovery, backfill and unknown-parent fetches (TEST-ONLY): fewest in flight, then highest score,
then random (`choose_peer`, `:268-302`); Recovery preempts Backfill; 3 attempts x 4 peers; driven
only by the mock-sender `run_to_completion` (`:323-415`). At HEAD the only outbound req/resp
requests are Status, MetaData and Goodbye.

## Backfill

All of `services/p2p/src/backfill/` except `window.rs` is TEST-ONLY; cc-p2p constructs none of it.

```text
 +--------------------------------------------------------------------------+
 | GapDetector, five triggers: head jump > 1 slot; clock stall >= 2 slots;  |
 |  peer Status head > ours + 4; transport reconnect; storage serve window  |
 |  holes or eas above target; emits GapDetected{from_slot, to_slot, ..}    |
 +--------------------------------------------------------------------------+
 | BackfillPlanner::on_gap: <= 64-slot BatchPlans (descending below the     |
 |  anchor / for holes); skip a batch whose slots are all in cached_slots   |
 +--------------------------------------------------------------------------+
 | schedule_at: <= 4 batches in flight; distinct peers preferred, else one  |
 |  reused; digest match, peer eas <= start; RequestScheduler::choose_peer; |
 |  OutboundBlockBudget 128 blocks / 10 s; columns via mode_for(batch):     |
 |  end <= anchor -> 4 custodied, else 8 sampled                            |
 +--------------------------------------------------------------------------+
   x transport callback: no ByRange sender is wired
 +--------------------------------------------------------------------------+
 | on_batch_success -> one oldest-first hold (no branch on mode after it)   |
 | on_batch_failure -> retry on another peer; after 3 tries abandon and     |
 |  record a gap (the planner never exits)                                  |
 +--------------------------------------------------------------------------+
 | drain_imports, strictly oldest first along the forward cursor:           |
 |  ImportReady{slot, root, parent_root, ..}, metadata only, no block SSZ   |
 |  -> feed_backfill_to_sampling (ColumnSource::ByRange)                    |
 +--------------------------------------------------------------------------+
   x send block down E1 (try_stream_once): not implemented

 Apart, called by nothing (not even the planner): below.rs verify_below_batch,
   SSZ field binding -> parent chain -> one-domain batch BLS
   x E5 PutBackfillBatch: no client method
```

TEST-ONLY library code, read top to bottom: GapDetector, then BackfillPlanner methods (`on_gap`;
`schedule_at`, which calls `RequestScheduler::choose_peer`; `on_batch_*`; `drain_imports`) in
intended call order, not a LIVE path; `x` = no caller or not implemented.

| Part | Code | Notes |
|---|---|---|
| Gap triggers | `services/p2p/src/backfill/planner.rs:61-96`, thresholds `:38-51`, `on_serve_window` `:294-321` | The first four compare chain's head with p2p's view. The fifth reads the storage serve window (proto `ServeWindow`): its `holes`, or eas above the target |
| Batch planning | `planner.rs:364-381` (`plan_batches`), `:387-392` (`batch_timeout`); `backfill/below.rs:478-483` (`plan_batches_descending`) | `BATCH_SLOT_LIMIT = 64`, half of `MAX_REQUEST_BLOCKS_DENEB`; timeout `min(5 s + chunks x 10 s, 60 s)`. Latent: with no anchor set, `on_gap` plans a non-hole gap forward but `mode_for` picks Below (`planner.rs:685`, `:798`) |
| Scheduling, transport seam | `planner.rs:911-1013` (`schedule_at`); `:13-15` | requests would be `beacon_blocks_by_range/2` + `data_column_sidecars_by_range/1`; "the host wires live ByRange clients", but nothing in `host.rs` or `service.rs` does. Peer reuse is the fallback (`:965`) |
| Release | `drain_imports` `:1126-1153`; `ImportReady` `:503-514` | oldest-first along the forward cursor, to avoid UNKNOWN_PARENT; carries no block bytes. As wired, held batches below that cursor (below-anchor ones) are never released |
| Below anchor | `backfill/below.rs:292-366` | binds fields from the SSZ body; checks the parent chain before any BLS; zero fork-choice calls; called only from tests |
| Cache | `backfill/cache.rs` | 1 GiB hard ceiling, 2048 block entries, oldest-first eviction. The planner holds no cache: it skips slots in its own `cached_slots` set (`planner.rs:572`, filled by `note_cached_slot` `:733`), which nothing ties to `BackfillCache` |

**Where batches would be written (E5).** As designed, Forward batches go to chain as DA-gated
imports over E1 and below-anchor batches go to the storage service with `PutBackfillBatch`. At
HEAD they are written **nowhere**: no request is sent, `StorageClient` has no such method
(`services/p2p/src/storage_client.rs:214-371`), and nothing turns an `ImportReady` into a
`GossipObject`. The storage service's `PutBackfillBatch` handler and its admission checks are
served in shape A but never called (`crates/storage-core/src/serve.rs:757-883`; [07 section 4.8](07-storage.md#48-backfill-write-path-e5-shape-a)).

## Chain-stream client (E1, N2)

p2p dials chain ([ADR-07](../adr/ADR-07.md)) when `[peers].chain` is set (compose
`http://chain:9001`): one never-fatal task, `chain-stream-client`, wraps `cc_seam::Ipc`
(`services/p2p/src/service.rs:790-828`; `services/p2p/src/chain_stream/client.rs:212-291`).
Session, Hello, reconnect and verdict mapping: [03 section 4](03-internal-contracts.md#4-p2pstream-anatomy);
the gossip block path (`chain_out` 1024, `Semaphore(1024)`, one `dispatch_outbound` per object):
[10 section 6](10-p2p-gossip-and-das.md#6-block-path-to-chain); N2 records (IDLE): [10 section 8](10-p2p-gossip-and-das.md#8-operations-getvalidatorrecords-and-sync-committee).

- **Views (LIVE)**, chain-owned and pushed ([ADR-P2-05](../adr/ADR-P2-05.md)). The client is the
  single writer of `ChainViewStore` (`chain_stream/view.rs:22-66`, written at
  `chain_stream/client.rs:309`) and replaces the whole view each time; readers are the Status
  builder and gossip validation. With an installed core, chain pushes slot (1), epoch (2) and
  head-change (3) views (`crates/chain-core/src/p2p_stream.rs:276-330`, `:545-560`); a core-absent
  chain (the compose default) sends only the kind-4 view on Hello, with zero chain fields.
- **Objects and verdicts (IDLE).** `submit_gossip` is Policy A ([ADR-R-01](../adr/ADR-R-01.md):
  2 s, then `Backpressure`; `crates/seam/src/ipc.rs:1121-1159`). At HEAD p2p subscribes to no
  gossip topics; the plan schedules gossip wiring for a later stage.
- **Publish (DEAD-AS-WIRED).** Chain never calls `request_publish`; the p2p side is wired:
  `on_publish` -> `proto_pub_tx` (256) -> `chain-publish-dispatch` (drop-oldest,
  `chain_stream/publish.rs:62-147`) -> `publish_tx` (256) -> `publish-bridge` ->
  `cmd_tx.try_send(Publish)` (512, `service.rs:769-788`). Ipc's own `publish_rx` mailbox would
  also fill and is never drained (`ipc.rs:854-858`).
- **Column / DataAvailable (DEAD-AS-WIRED).** No p2p code calls `notify_data_available` or
  `submit_column_sidecar` (`ipc.rs:1161-1196`); DA-gate consequences: [05](05-block-lifecycle.md).

## EngineStream server (E8)

- **Attach.** `build_minimal_engine_stream` (`services/p2p/src/main.rs:503-508`;
  `services/p2p/src/engine_stream/server.rs:68-101`) builds its own `SamplingTracker` (365-day
  deadline, `da_tx = None`) and `SeenSets`.
- **Session.** One `tokio::spawn` per stream (`engine_stream/server.rs:141-158`): `EngineHello`
  -> `SubscriptionSet` (sampled columns) on an mpsc(64), never re-sent: `CgcSubscriptionBridge`
  is never constructed (`engine_stream/subscription.rs:120-160`).
- **Inject.** decode -> index < 128 -> lengths -> inclusion proof -> KZG (always run,
  `engine_stream/inject.rs:78-86`; the fastpath skip of [ADR-P3-15](../adr/ADR-P3-15.md) does not
  extend to inject) -> seen -> sampling -> publish iff subscribed ([ADR-P3-07](../adr/ADR-P3-07.md))
  through a `NoopPublisher` that only logs (STUB, `inject.rs:530-726`); no `P2pToEngine.fetch`.
- **DEAD-AS-WIRED (served, never dialed).** The engine container's client was deleted in `631994c`
  (`services/engine/src/main.rs:6`); `engine.p2p_uri` is parsed, never dialed. Latent: if a
  client did dial, KZG would run synchronously on a tokio worker.

## Storage client (E6, E5)

`services/p2p/src/storage_client.rs` runs when `[peers].storage` is set (compose
`http://storage:9006`; `services/p2p/src/service.rs:375-407`).

- **`StorageClientHandle`** (`:106-208`): its private `apply_stream_window` and
  `collapse_to_cache_floor` (`:184-207`) are the only production callers of `store_recomputed`.
- **WatchServeWindow loop** (`:389-459`, `run_watch_once` `:469-538`; LIVE, content static), a
  detached `tokio::spawn`. It dials with a 5 s timeout, sets `available = true` on open, maps
  each message through `advertised_slot_from_serve_window` (`:88-99`) and sets the i64 gauge
  `cc_p2p_earliest_available_slot` with `as i64` (`:189`, `:198`): the `u64::MAX` seed shows as
  -1 (0 before the first message or collapse).
- **Disconnect.** `available = false`; hold the last value for `window_stale_grace_secs = 60`
  while retrying (also when the first dial fails), then collapse to the cache floor; back off
  250 ms -> 10 s (doubling, no jitter). The floor `Arc` starts at `u64::MAX` and is never bound
  to a cache (`service.rs:376-382`), so collapse leaves the advertised value at `u64::MAX`; only
  `cc_p2p_window_collapsed_total` and a warn log change.
- **`advertise_block_floor`** (`config/p2p.toml:30`) is INERT: both branches return `u64::MAX` for
  a `u64::MAX` input. **Serve reads** are DEAD-AS-WIRED: `StorageClient::new(...)` is bound to
  `_client` and dropped (`service.rs:389-391`), so the four `get_*` methods have no caller.

Health: p2p's aggregate health is SERVING only if chain and the storage service both probe
SERVING, so a down storage service marks p2p NOT_SERVING although E6 carries only the empty seed
([12 section 4.3](12-boot-health-and-shutdown.md#43-health-dag)).

## Tasks in scope

`swarm` (`ProcessFatal`: a panic in the req/resp path exits 1, [ADR-P2-13](../adr/ADR-P2-13.md));
`status-epoch`; `stub-reqresp`; `publish-bridge` and `chain-in-late-verdicts` (always); with
`[peers].chain`: `chain-stream-client`, one `dispatch_outbound` per object, and
`chain-publish-dispatch`, else `stub-chain-out` (`services/p2p/src/service.rs:769-840`); the
detached WatchServeWindow loop (with `[peers].storage`); one EngineStream session per inbound
stream (none at HEAD). Inventory: [09 section 2.2](09-p2p-host-and-discovery.md#22-task-and-channel-map), [13](13-concurrency-model.md).

## Metrics in scope

| Metric | Set at | At HEAD |
|---|---|---|
| `cc_p2p_reqresp_inbound_total{protocol,result}` | `services/p2p/src/host.rs:625-733` | LIVE; every block or column request counts as `result="resource_unavailable"` |
| `cc_p2p_reqresp_outbound_total{protocol,result}` | `host.rs:595-604`, `:1116` | LIVE (Status, MetaData, Goodbye only) |
| `cc_p2p_reqresp_ratelimit_total{peer_kind,protocol}` | `host.rs:652` | INERT (the bucket never empties) |
| `cc_p2p_earliest_available_slot`, `cc_p2p_window_collapsed_total` | `services/p2p/src/storage_client.rs:189-200` | LIVE (content static): gauge -1 (`u64::MAX` as i64; 0 before the first message), counter 0 unless collapse fires |
| `cc_p2p_backfill_progress_slots`, `cc_p2p_backfill_batch_abandoned_total`, `cc_p2p_cache_occupancy_bytes`, `cc_p2p_cache_bound_bytes` | `backfill/planner.rs:1079`, `:1134`; `backfill/cache.rs:224-225`, `:707-708` | DEAD-AS-WIRED: registered, writers never constructed (same label in [09 section 9](09-p2p-host-and-discovery.md#9-metrics)) |

## Comments and docs that disagree with the code

| Where | Says | Code at HEAD |
|---|---|---|
| `services/p2p/src/lib.rs:22-23` | blocks and columns are served from the backfill cache | always code 3 `"handler not ready"` |
| `services/p2p/src/host.rs:3-4` | "The swarm task never does work" | Status/Ping/MetaData/Goodbye decode, policy and encode run inline in the swarm task (`host.rs:625-733`) |
| `services/p2p/src/service.rs:389-390` | "Keep a client alive for future cache-miss fetch" | `_client` is dropped at the end of the block |
| [ADR-P2-14](../adr/ADR-P2-14.md) Decision; `reqresp/blocks.rs:14-15`, `reqresp/columns.rs:17-18` | Status and every block/column serve handler read the one advertised atomic | serve planners read `BackfillCache`'s own `ServeWindow` (`blocks.rs:405`, `columns.rs:466`, `backfill/cache.rs:128`) |
| [ADR-P2-14](../adr/ADR-P2-14.md) lines 43-50 | no stream message arrives today; the cache floor is updated by the backfill cache | one message carrying `u64::MAX` arrives (`crates/storage-core/src/serve.rs:889-893`); the floor `Arc` is never bound (`service.rs:376-382`) |
| `services/p2p/src/engine_stream/mod.rs:6-10` | inject feeds the same entry point as gossip; one DataAvailable producer; no second `seen` | the server builds its own `SamplingTracker` (`da_tx = None`) and `SeenSets` (`engine_stream/server.rs:84-91`) |
| `services/p2p/src/chain_stream/mod.rs:4-5` | verdict timeouts resolve as local IGNORE | `Ipc` resolves them as `Backpressure`, which validation then maps to IGNORE |
| `proto/eth/p2p/v1/p2p.proto:21-23`, [ADR-P3-02](../adr/ADR-P3-02.md) | the engine container is the EngineStream client | no client exists |
| `docs/p2p-dependencies.md:107-109`, `docs/running.md:649-652`, `config/p2p.toml:44`, `storage_client.rs:210` | Status advertises from the cache; `CC_ENGINE_P2P_URI` feeds EngineStream; `[peers].storage` feeds a serve path; the client has `PutBackfillBatch` | the advertised value comes only from WatchServeWindow; nothing dials EngineStream; no serve reads; no such method |

## Planned changes

Planned, not built. Source: [`plan/architecture.md`](../../plan/architecture.md) §2.3, §2.5, §3.6, §4.3 and §9.1 (S2, S3).

- **E5 -> in-process admission.** Planned: S2 replaces `PutBackfillBatch` with
  `storage_core::backfill::admit()` behind `cc-seam::ArchiveWrite`, wired at S3; per §4.3 a batch
  may only extend the durable frontier. At HEAD the RPC is served, no `admit()` exists (only
  `pub(crate)` helpers in `crates/storage-core/src/backfill.rs`) and `ArchiveWrite` has no backfill
  method (`crates/seam/src/lib.rs:337-366`), though `plan/issues/s2-exit-note.md:563` says deleted.
- **E6 -> one `AtomicU64` read.** Planned: S2 deletes `WatchServeWindow` and the serve reads as a
  transport (both exist at HEAD); S3 publishes the storage serve window, equal to
  `cc_storage_earliest_available_slot` with no second writer ([ADR-P2-14](../adr/ADR-P2-14.md)).
  At HEAD only the dead publishers write that gauge, so it reads its seed 0 while E6 emits
  `u64::MAX` (detail: [07](07-storage.md) metrics table).
  Plan Q-10 is answered at HEAD: nobody publishes it (`crates/storage-core/src/serve.rs:281-338`).
- **S3 wiring.** Planned: one codec (`cc-wire`) replaces the duplicated tables (`bin/serve-probe`
  keeps its own, [ADR-P4-12](../adr/ADR-P4-12.md)); the real DA feed and backfill's write path;
  `fault_mode.rs` off production paths; §3.6 revisits `drop_during_sync` once backfill is wired.
- **E1 / E2 / E8 transport.** Planned: S3 selects the E1/E2 transport implementation (§2.5). E8
  becomes a second method on `P2pEgress` (chain-core -> p2p) instead of a separate contract.
- **Open:** a cache for `with_block_serve`, a swarm-backed `RequestScheduler` sender, a backfill
  transport owner and the source of `ImportReady`'s block bytes; the plan names none beyond "S3".
