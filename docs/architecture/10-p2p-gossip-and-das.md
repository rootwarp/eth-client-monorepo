# Gossip validation and data availability

> **As built at `7d8833d`** (`develop`, 2026-08-16). Status labels and diagram legend: [README](README.md#status-labels).
> Target design: [`plan/architecture.md`](../../plan/architecture.md) §2.3, §3.4, §3.5, §3.6.

What happens to a gossip message inside `cc-p2p` once libp2p hands it over: topics, subscription,
validation, the authority split with chain, KZG, and PeerDAS custody / sampling / recovery.
Elsewhere: swarm, discovery, peer manager [09](09-p2p-host-and-discovery.md); forwarded blocks on
chain [05](05-block-lifecycle.md); req/resp [11](11-p2p-reqresp-and-sync.md); outbound publish (E1
publish arm -> `SwarmCommand::Publish`, DEAD-AS-WIRED upstream) [03](03-internal-contracts.md),
[09](09-p2p-host-and-discovery.md).

**Read this first.** The code holds a nearly complete Fulu gossip and PeerDAS stack, but deployed
p2p (shape A) subscribes to **zero** gossip topics. `SwarmCommand::Subscribe` has a handler
(`services/p2p/src/host.rs:1253-1260`) and no production sender, and `services/p2p/src/service.rs`
builds no `TopicRegistry`. The pinned gossipsub (`Cargo.toml:153`, rev `6348a0b`) drops messages on
unsubscribed topics before `Event::Message`, so sections 4-9 and the sampling inputs in section 11
receive no input; section 10 is DEAD-AS-WIRED and section 12 PARAMS-ONLY for unrelated reasons. At
HEAD p2p subscribes to no gossip topics; the plan schedules gossip wiring for a later stage. As
wired, even with topics subscribed shape A would ACCEPT nothing: core-absent chain answers blocks
IGNORE(`Internal`) and fails `GetValidatorRecords` (operations IGNORE `Internal` at the record
step); its one default-valued `ChainView` has no proposer pubkeys (so columns IGNORE `Internal` at
step 8); sync IGNOREs on `NoopSyncSource`; aggregates and attestations are stubs.

## 1. Status at a glance

Shape B (`cc-beacon-core`) hosts no p2p, so every row is ABSENT there. No shipped configuration
pairs cc-p2p with cc-beacon-core; how p2p attaches to shape B is not yet defined. Unprefixed paths
are under `services/p2p/src/`.

| Component | Code | Shape A status |
|---|---|---|
| Topic names, eth2 message-id fn | `gossip/topics.rs` | IDLE (installed; no subscribed traffic) |
| Fork digest (EIP-7892) | `fork_digest.rs` | LIVE for ENR `eth2` / `nfd`, the discv5 allowed-digest filter and Status |
| Gossip subscription (`SwarmCommand::Subscribe`) | `channels.rs:269`, `host.rs:1253` | DEAD-AS-WIRED (handler, no sender) |
| Swarm pre-checks, `gossip-validate` loop; block, column, operation validators | `host.rs`, `gossip/validate/{pipeline,block,column,operations}.rs` | IDLE |
| Sync-committee / aggregate + attestation validators | `gossip/validate/sync.rs:138` / `gossip/validate/pipeline.rs:91-94` (`StubIgnore`) | STUB (`NoopSyncSource` / always IGNORE `AlreadyKnown`); no input at HEAD |
| E1 object / verdict arm, late-verdict task; N2 `GetValidatorRecords` | `chain_stream/`, `cc_seam::Ipc`; `chain_stream/records.rs` | IDLE |
| KZG verify pool and its bridge | `das/verify_pool.rs` | DEAD-AS-WIRED (self-terminating) |
| Gossip-side sampling feed (`NoopSamplingFeed`) | `gossip/validate/column.rs:265` | STUB; no input at HEAD |
| EngineStream `CustodyManager` + `SamplingTracker` | `engine_stream/server.rs:68-101` | DEAD-AS-WIRED (no client, `da_tx=None`) |
| `TopicRegistry` + BPO Steady/Overlap/Drain; `poll_deadlines`, `mark_abandoned`, `recover()` | `gossip/registry.rs`; `das/{sampling,recovery}.rs` | TEST-ONLY |
| E1 column arm, E2 `data_available` | `crates/seam/src/ipc.rs:1161-1196` (no caller in `services/p2p`) | DEAD-AS-WIRED |
| Outbound publish: E1 publish arm -> `chain-publish-dispatch` / `publish-bridge` -> `SwarmCommand::Publish` | `service.rs:767-799`, `host.rs:1246-1252` | arm DEAD-AS-WIRED (`request_publish` has no caller); bridge tasks IDLE; topic used verbatim, `publish_digest` unused |
| `P2pService.SetCustodyGroupCount` (`UnattachedCgcHook`) | `service.rs:1086` | STUB (always `FAILED_PRECONDITION`) |
| GossipSub peer scoring | `gossip/scoring.rs` | PARAMS-ONLY |
| Devnet gossip (`--publish-fixture` / `--devnet-peer`) | `fault_mode.rs` | DEVNET-ONLY (129 topics, blind ACCEPT) |

## 2. Topics and fork digests

- **Ten Fulu families**, `TopicName` (`services/p2p/src/gossip/topics.rs:145-167`); no
  `blob_sidecar`. Wire form `/eth2/{digest}/{name}/ssz_snappy`, digest as lowercase hex without
  `0x` (`topics.rs:220-230`). `expand_fulu_topic_names(counts)` (`topics.rs:238-257`) with
  `SubnetCounts::mainnet()` (`services/p2p/src/gossip/mod.rs:68-78`) yields 64 attestation, 128
  column and 4 sync subnets, so **2 + 64 + 128 + 1 + 4 + 4 = 203 topics per digest**.
- **Message-id** = `SHA256(domain || u64_le(len(topic)) || topic || payload)[..20]`, domain
  `[1,0,0,0]`, over the snappy-decompressed payload (`topics.rs:55-70,100-103`), installed with
  IDONTWANT-on-publish by `ethereum_behaviour_config()` (`topics.rs:120-124`).
- **Fork digest** (`services/p2p/src/fork_digest.rs:78-100`): the first 4 bytes of
  `compute_fork_data_root`; from Fulu, XORed with `sha256(u64_le(bp.epoch) || u64_le(bp.max_blobs))`
  for `bp = get_blob_parameters(epoch)` from the runtime config, so a blob-parameter-only (BPO)
  change renames all 203 topics. Production uses the digest for the ENR `eth2` / `nfd` fields, the
  discv5 allowed-digest filter (`services/p2p/src/discovery/task.rs:486,522,558`, through
  `allowed_digests` at `:626-627`) and Status ([09](09-p2p-host-and-discovery.md)); only devnet
  mode and the registry build topic strings.
  Inbound topics are **not** digest-checked: `parse_topic_name` reads only the name segment
  (`services/p2p/src/gossip/validate/pipeline.rs:117-160`).
- **Runtime blob limits only.** `scripts/check-no-max-blobs-p2p.sh` bans any `MAX_BLOBS_PER_BLOCK` /
  `ELECTRA_FORK_EPOCH` token in `services/p2p` and `crates/libp2p` Rust sources (`:17-22`); at HEAD
  it exits 1 on a self-referential test literal (`services/p2p/tests/bpo_transition.rs:589`). The CI
  `deps` job runs it (`.github/workflows/ci.yml:409`); `make deps` does not (`Makefile:247`), though
  the script header (`check-no-max-blobs-p2p.sh:9-10`) says it does.

## 3. Subscription

**As designed (TEST-ONLY).** `TopicRegistry<G: GossipsubControl>`
(`services/p2p/src/gossip/registry.rs:189-205`) is the intended single owner of `set_topic_params`,
`subscribe` and `unsubscribe`. It sets params before subscribing (`registry.rs:616-627`), refuses a
topic with no registered validator (`registry.rs:348-353`), and `advance_to` (`registry.rs:541`)
runs a fork or BPO transition:

| Phase | Entered when | Live digests | Action |
|---|---|---|---|
| Steady | start, or the tick after Drain | {cur} | none |
| Overlap | `epoch >= boundary - 1` | {cur, next} | params + subscribe every current topic under `next` |
| Drain | `epoch >= boundary + 1` | {next} | unsubscribe `cur`; `publish_digest` already switched at `boundary` |

A missed tick catches up in one call: Overlap and Drain together, and the Drain completes too if
`epoch >= boundary + 2` (`registry.rs:583-603`). Subnet resync helpers (`sync_column_subnets` and
friends) keep the params-then-subscribe order; their intended drivers, `SubnetManager` and the
runtime cgc hook, are TEST-ONLY ([09](09-p2p-host-and-discovery.md)). The only `GossipsubControl`
impl is `RecordingGossipsub` (`registry.rs:125-161`); no adapter over the real
`gossipsub::Behaviour` exists, and `advance_to` / `publish_digest` have no non-test caller.

**As built.** Production sends no `Subscribe`, joins no mesh, and the validation pool is IDLE. Only
devnet mode subscribes: `beacon_block` plus all 128 `data_column_sidecar_{i}` at the **epoch-0**
digest (`services/p2p/src/fault_mode.rs:979-996`), and it reports ACCEPT for every message
**without validation** (`fault_mode.rs:1360-1381`). DEVNET-ONLY.

## 4. The validation pipeline

```text
 gossipsub (Anonymous, validate_messages, SnappyTransform 10 MiB, heartbeat 1 s, dup cache 60 s)
                x  Event::Message: shape A subscribes no topic, so none reaches the swarm task
                v
 +-----------------------------------------------------------------------------+
 | swarm task  (host.rs:462-548)                                               |
 |  a. shed     chain_out had no free slot for 500 ms   -> IGNORE(Internal)    |
 |  b. topic    unknown name segment (digest unchecked) -> IGNORE(Invalid)     |
 |  c. size     over the per-topic SSZ maximum          -> REJECT(Invalid)     |
 |  d. enqueue  try_send gossip mpsc(1024); Full/Closed -> IGNORE(Internal)    |
 +-----------------------------------------------------------------------------+
                |  GossipWork
                v
 +-----------------------------------------------------------------------------+
 | gossip-validate task: ONE serial loop over std Mutex<ValidationPoolState>   |
 |  1. redrive_unknown_proposer   (only when ChainView carries a lookahead)    |
 |  2. validate_one: prune seen sets at the finalized slot, then by kind:      |
 |       BeaconBlock        local stage, then forward to chain   (section 6)   |
 |       DataColumnSidecar  13 steps, BLS + Merkle + KZG inline  (section 7)   |
 |       Operation          9-10 steps, records over N2          (section 8)   |
 |       SyncCommittee      NoopSyncSource -> IGNORE(Internal)   (section 8)   |
 |       StubIgnore         aggregate, attestation -> IGNORE(AlreadyKnown)     |
 |  3. report: REJECT -> PeerPenaltyCmd  (try_send, penalty mpsc 256)          |
 |             every verdict -> SwarmCommand::ReportValidation (cmd mpsc 512)  |
 +-----------------------------------------------------------------------------+
                |  send().await (blocks the loop while cmd mpsc is full)
                v
   same swarm task: report_gossipsub_validation (host.rs:434-449), the one report call site
```

Caption: `x` = DEAD-AS-WIRED edge (no subscription). The gossip arm of the swarm task, the
gossip-validate loop and the other vertical edges are IDLE at HEAD; the swarm task itself is LIVE.

Step **a** is stall-then-shed: after `stall_max = heartbeat x 0.5 = 500 ms` without `chain_out`
capacity the swarm polls anyway, and sheds the next swarm event only if it is a gossip message
(`host.rs:244-311,472`; `services/p2p/src/chain_stream/mod.rs:59`). `run_validation_pool`
(`services/p2p/src/gossip/validate/pipeline.rs:385-398`) drives a `ValidationPool` built by
`with_chain_uri` (`pipeline.rs:325-381`) with `production_kzg_verify()` (c-kzg, or fail-closed),
`NoopSamplingFeed`, `NoopSyncSource`, and `RpcValidatorRecordSource` when `[peers].chain` is set.
`validate_one` loads `ChainView` lock-free from an ArcSwap fed by the E1 session + view arm (LIVE;
content static while chain is core-absent, see [03](03-internal-contracts.md)).

### 4.1 Verdict mapping

| Outcome | Raised by | Gossipsub | App score (peer manager) |
|---|---|---|---|
| ACCEPT | validator; chain for blocks | Accept: propagate to mesh | none |
| IGNORE `Duplicate` / `UnknownParent` / `FutureSlot` / `AlreadyKnown` / `DeferredDa`; `Invalid` | validator or chain; `Invalid` only for an unknown topic name at the swarm | Ignore | none |
| IGNORE `Internal` | shed, queue full/closed, seam backpressure or timeout, missing keys | Ignore | **never** penalised (`verdict.rs:136`) |
| REJECT `Invalid` / `InvalidSignature` / `NotDescendedFromFinalized` | validator or chain | Reject | `gossip_invalid` -10 via `PeerPenaltyCmd` (dropped if that mpsc is full) |
| REJECT at the swarm size pre-check | swarm task | Reject | none (no `PeerPenaltyCmd` on that path) |
| chain REJECT after we reported ACCEPT | late-verdict task on `chain_in` | not re-reported | `import_invalid` -25 (`pipeline.rs:860-886`) |

Deltas: `services/p2p/src/peer_manager/score.rs:60-69`. `Verdict` is `#[must_use]`
(`services/p2p/src/verdict.rs:12-24`); `to_message_acceptance` has no wildcard arm and maps
`Unspecified` to IGNORE (`verdict.rs:102-109`). Every path reports **exactly once** through
`report_gossipsub_validation` (`host.rs:434-449`); redrives of parked messages never re-report.
Chain's errors map exhaustively onto verdicts ([ADR-P1-07](../adr/ADR-P1-07.md)).

## 5. Authority: validated locally or by chain

[ADR-P2-04](../adr/ADR-P2-04.md): **only BLOCK is chain-authoritative**. Every other family is
judged in p2p and its REJECTs never reach chain; chain answers any non-BLOCK `GossipObject` with
IGNORE(`AlreadyKnown`) and discards it (`crates/chain-core/src/p2p_stream.rs:753-766`).

| Family | Authority | p2p checks | Sent to chain | Status |
|---|---|---|---|---|
| `beacon_block` | chain | size, Fulu SSZ, timing, `(slot, proposer)` dedup, parent gate | `GossipObject{kind=Block}`, awaits verdict | IDLE |
| `data_column_sidecar_{i}` | p2p | all 13 spec steps incl. KZG | nothing (E1 column arm DEAD-AS-WIRED) | IDLE |
| 4 operation topics | p2p | full condition lists, records via N2 | nothing; ACCEPT is not stored | IDLE |
| `sync_committee_{i}`, contribution | p2p | 8 steps; signature needs keys | designed: `try_send`, `reply: None` | STUB |
| aggregate, `beacon_attestation_{i}` | p2p | none | nothing | STUB |

## 6. Block path to chain

```text
 gossip-validate    chain-stream client      cc_seam::Ipc         chain P2pStream session
     |                    |                        |                        |
     |<== ArcSwap view ===|<====== on_view ========|<== E1 session + view ==| LIVE (content static)
     | local stage ok     |                        |                        |
     |- - chain_out - - ->| mpsc(1024), no timeout |                        |
     |                    | Semaphore(1024) permit |                        |
     |                    |- - submit_gossip - - ->| send_timeout(2 s)      |
     |                    |                        |- - E1 object arm - - ->| core-absent:
     |                    |                        |                        |   IGNORE(Internal)
     |                    |                        |                        | installed core:
     |                    |                        |                        |  scheduler import lane
     |                    |                        |                        |  -> early ACCEPT (05)
     |                    |                        |<- - - - E1 verdict - - |
     |                    |  [1] on_verdict -> chain_in try_send -> late-verdict task
     |<- - - - - - - - - - - [2] resolve oneshot within the rest of the 2 s budget; 12 s backstop
     | ACCEPT -> note_accepted_block + redrive_for_parent; report once (section 4)
     |                    |  a later verdict for the same root has no waiter:
     |                    |  on_stray_verdict -> chain_in -> late REJECT = import_invalid -25
```

Caption: three lifelines in cc-p2p (:9002), the session in cc-chain (:9001); p2p dials. `====>` LIVE
stream (content static while chain is core-absent); `- - ->` IDLE (wired, no input at HEAD).
Non-BLOCK kinds: section 5. [1]/[2] is the order inside `apply_verdict`
(`crates/seam/src/ipc.rs:917-920`); stray path `ipc.rs:894-896`.

- **Local stage** (`services/p2p/src/gossip/validate/block.rs:72-156`): size, Fulu-only SSZ, timing
  (`slot < finalized_slot` -> IGNORE `AlreadyKnown`; beyond `now + disparity`, default 500 ms ->
  IGNORE `FutureSlot`), `(slot, proposer)` dedup, then a **local parent gate**: once any block was
  ACCEPTed, a block with an unknown parent is parked and IGNOREd (`block.rs:122-137`). Otherwise it
  is forwarded as `GossipObject{ssz, fork: 0, root, source: Gossip, kind: Block, subnet_id: 0}`.
- **Budgets and resolution.** The `chain_out_tx.send().await` enqueue has no timeout
  (`pipeline.rs:611`). The seam enqueues with `send_timeout(2 s)` and awaits the reply with the
  **rest** of that budget (`crates/seam/src/ipc.rs:47,1121-1159`); the local `timeout(12 s)`
  (`pipeline.rs:615`) is a backstop. `Backpressure` (budget expired, or the `(Ignore, Internal,
  Invalid)` shape that `apply_verdict` maps to scheduler import lane backpressure,
  `ipc.rs:901-915`), `Timeout` and a dropped reply all become IGNORE(`Internal`)
  (`pipeline.rs:614-639`). Some internal import failures surface to p2p as backpressure.
- **Chain side**: core-absent chain answers IGNORE(`Internal`), so as wired every shape A block
  would end there; session handling, early ACCEPT, second verdicts: [05](05-block-lifecycle.md).
- **After ACCEPT** the block enters the seen and root sets (`block.rs:159-165`) and
  `redrive_for_parent` re-validates parked children locally. A redriven block goes up with
  `reply: None` and is never noted as known (`pipeline.rs:755-764`): only the on-time path calls
  `note_accepted_block` (`pipeline.rs:640-656`), and the late-verdict path only penalises
  (`pipeline.rs:860-886`). As wired, a redriven block that chain ACCEPTs would never become a known
  parent, so its parked children would stay parked until evicted. Latent; unreachable at HEAD
  because no gossip topics are subscribed.

## 7. Column sidecar validation

`validate_data_column_sidecar` (`services/p2p/src/gossip/validate/column.rs:347-582`) runs the 13
ordered steps of `ColumnStep` (`column.rs:85-101`), all under the validation-pool mutex:

| # | Check | On failure |
|---|---|---|
| 1-2 | size; SSZ decode `DataColumnSidecar` | REJECT `Invalid` |
| 3-4 | `index < 128`; `compute_subnet_for_data_column_sidecar(index)` = topic subnet | REJECT `Invalid` |
| 5 | slot in `[finalized_slot, now + disparity]` | IGNORE `AlreadyKnown` / `FutureSlot` |
| 6 | commitments non-empty, `<= get_blob_parameters(epoch).max_blobs_per_block`, lengths equal | REJECT `Invalid` |
| 7 | seen `(slot, proposer, column)` | IGNORE `Duplicate` |
| 8 | proposer BLS signature, pubkey from `ChainView` | bad: REJECT `InvalidSignature`; no key: IGNORE `Internal` |
| 9 | parent known (seen roots or `view.head_root`); `slot > parent_slot`; finalized ancestry | unknown: park + IGNORE `UnknownParent`; else REJECT |
| 10 | expected proposer from `ChainView.proposer_lookahead` | unknown: park + IGNORE `Internal`; mismatch: REJECT |
| 11 | commitments inclusion proof (depth 4, field index 11), 512-entry LRU | REJECT `Invalid` |
| 12 | `verify_cell_kzg_proof_batch` **inline** via `KzgVerify` (`column.rs:555`) | REJECT `Invalid` |
| 13 | insert seen, `sampling.on_column_accepted`, ACCEPT | -- |

- **Fail-closed KZG.** If the trusted setup cannot load, `production_kzg_verify()` returns
  `FailClosedKzg` and every column REJECTs at step 12, penalising peers for a local fault
  (`services/p2p/src/gossip/validate/kzg_verify.rs:117-136`; [ADR-P1-05](../adr/ADR-P1-05.md)).
- **Accepted columns go nowhere** beyond mesh propagation and the no-op sampling feed: neither
  stored nor sent to chain; the `P2pToChain` column arm is DEAD-AS-WIRED (contract-only per
  [ADR-P2-11](../adr/ADR-P2-11.md); chain-side relay intent: [ADR-P4-03](../adr/ADR-P4-03.md)).
- **Cold or static view.** With no pubkeys in `ChainView`, every column ends IGNORE(`Internal`) at
  step 8 (`column.rs:438,663`). In shape A chain is core-absent, so the slot-tick driver is silent
  (`genesis_time` unset), the epoch driver fires only on real publishes
  (`crates/chain-core/src/p2p_stream.rs:276-330`), and p2p keeps the one default `VIEW_KIND_FULL`
  view from Hello: as wired, every column would end IGNORE(`Internal`) at step 8 even with gossip
  subscribed. With an installed core, slot-tick and head-change views carry an empty lookahead and
  p2p replaces the whole view (`p2p_stream.rs:395-430`;
  `services/p2p/src/chain_stream/view.rs:56-58`), so after the next non-epoch view steps 8 and 10
  lose their inputs. Latent; unreachable at HEAD because no gossip topics are subscribed.

## 8. Operations, `GetValidatorRecords` and sync committee

The four operation validators (`services/p2p/src/gossip/validate/operations.rs:257-345`; 10 steps
for `voluntary_exit`, 9 for the slashings and `bls_to_execution_change`) are p2p-authoritative and
never forwarded; ACCEPTed operations are not stored (`operations.rs:7-11`).

- **Records (N2, IDLE).** `ValidatorRecordCache` is a 4096-entry LRU with a one-epoch TTL
  (`services/p2p/src/chain_stream/records.rs:199-216`). A miss calls the unary
  `ChainService.GetValidatorRecords` (<= 256 indices) via `RpcValidatorRecordSource`, which opens a
  **fresh `Endpoint::connect()` per call with no timeout** (`records.rs:87-128`). The fetch is
  awaited without the validation-pool mutex, but inside the single loop (`pipeline.rs:507-541`).
- **Error mapping** (`operations.rs:469-478`): `Missing`, or a fetch error containing "out of
  range" -> REJECT `Invalid`; `OverBound`, `Empty` or any other error -> IGNORE `Internal`. As
  wired, core-absent chain (shape A default) fails the RPC with `FAILED_PRECONDITION`, so every
  operation reaching the record step would end IGNORE `Internal`; without `[peers].chain` (compose
  sets it) the empty-map source answers `Missing`, so each would REJECT (`pipeline.rs:335-337`).
- **Signature-domain GVR.** Column step 8, the operation validators and the sync-committee
  validator take `ChainView.genesis_validators_root` (`column.rs:647`, `operations.rs:868`,
  `pipeline.rs:446`), not the p2p config GVR that builds the fork digest
  (`services/p2p/src/service.rs:174`). GVR sources: [06](06-consensus-primitives.md).
- **Anti-replay.** Four `BoundedIndexSet`s of 4096 (`operations.rs:39`) are **cleared**, not
  pruned, when `finalized_epoch` advances (`operations.rs:84-92`).
- **Sync committee (STUB).** Message steps (`services/p2p/src/gossip/validate/sync.rs:307-412`):
  size, decode, timing, index, subnet, seen `(validator, subnet, slot)`, signature, accept and
  forward. `NoopSyncSource` answers `None` to every key query (`sync.rs:136-153`), so step 7
  always ends IGNORE `Internal` and the designed forward to chain never fires.

## 9. Seen, dedup and pending caches

Paths without a crate prefix are under `services/p2p/src/`.

| Structure | Key | Bound | Eviction / prune | Code |
|---|---|---|---|---|
| gossipsub duplicate cache | message-id | 60 s TTL | library | `crates/libp2p/src/behaviour.rs:252` |
| column / block seen | (slot, proposer, column) / (slot, proposer) | 16,384 / 1,024 | oldest first; pruned below finalized slot; blocks inserted only after chain ACCEPT | `gossip/seen.rs:16,19` |
| block roots (parent-known) | root | 1,024 | oldest first; not pruned at finality | `gossip/seen.rs:133,145,160-162` |
| sync msg / contribution seen | (validator, subnet, slot) / (slot, aggregator, subcommittee) | 4,096 each | pruned at finality | `gossip/validate/sync.rs:46-49` |
| operation index sets (x4) | validator index | 4,096 each | cleared at finality advance | `gossip/validate/operations.rs:39` |
| inclusion-proof verdicts | {commitments, proof, block, body} roots | 512 | LRU; shared with the KZG pool | `gossip/validate/column.rs:82` |
| reported ACCEPTs / `late_open` pins | correlation id | 1,024 / purge above 2,048 | LRU / arbitrary | `gossip/validate/pipeline.rs:61,217-232` |
| pending sidecars / blocks | parent or proposer unknown / parent unknown | 256 / 64 | oldest evicted, counted | `gossip/pending.rs:22,25` |

Redrive is local-only (`services/p2p/src/gossip/pending.rs:8-16`): `redrive_for_parent` after a
block ACCEPT, `redrive_unknown_proposer` per loop iteration when a lookahead is present. Blocks lack
the `head_root` fallback columns have, so a block whose parent arrived by sync, timed out, was shed,
or was itself redriven (section 6) is never redriven. Latent; unreachable at HEAD because no gossip
topics are subscribed.

## 10. KZG verification: inline today, pool disconnected

```text
 gossip-validate (tokio worker, holds Mutex<ValidationPoolState>), column step 12:
   kzg.verify_column_kzg -> verify_cell_kzg_proof_batch, INLINE (column.rs:555)

 designed producer of KzgJob (column job): none at HEAD
       x  kzg_tx bound as `_` in spawn_edge_workers (service.rs:705); dropped when it returns
       v
 kzg-verify-pool bridge: kzg_rx.recv() == None -> task exits (verify_pool.rs:1130-1144)
       x  last Arc<VerifyPool> drops; Drop closes the queue (verify_pool.rs:1117-1125)
       v
 +-------------------------------------------------------------------------+
 | VerifyPool  DEAD-AS-WIRED (self-terminating)                            |
 | contract: ADR-P2-02 (OS threads), ADR-P2-08 (cross-sidecar batching)    |
 |   queue VERIFY_QUEUE_BOUND = 256; full: drop oldest -> Dropped (IGNORE) |
 |   K = max(2, cores / 2) OS threads cc-kzg-verify-{i}: exit on close     |
 |   pop first job + contiguous jobs for the same block_root               |
 |     >= 4: structure + inclusion each, one cross-sidecar KZG batch,      |
 |           re-verify each sidecar on batch failure                       |
 |     <  4: structure, inclusion, KZG per sidecar                         |
 |   -> VerifyOutcome (oneshot) -> verdict      (never produced at HEAD)   |
 +-------------------------------------------------------------------------+
```

Caption: `x` = DEAD-AS-WIRED edge. The pool threads start at boot
(`services/p2p/src/service.rs:746-765`) and exit once the queue closes (static trace).

Contract points to keep when the pool is reconnected ([ADR-P2-02](../adr/ADR-P2-02.md),
[ADR-P2-08](../adr/ADR-P2-08.md)): `pool_worker_count()` = `max(2, available_parallelism / 2)`
(`verify_pool.rs:54-61`); evicted jobs complete as `Dropped` (= IGNORE) with a counter
(`verify_pool.rs:288-309`); a hard 4096-row cap applies regardless of the caller's
`max_blobs_per_block` (`verify_pool.rs:47-52`); the inclusion LRU is shared with the gossip
validator and its lock is never held across KZG. `penalise_peer` only feeds a diagnostic ring and
the `gossip_invalid` counter and sends no `PeerPenaltyCmd` (`verify_pool.rs:839-854`). The trusted
setup is loaded three times (gossip pool, verify pool, EngineStream inject). Benchmarks:
[kzg-benchmark.md](../kzg-benchmark.md).

## 11. PeerDAS custody, sampling and recovery

```text
 +--------------------------------------------------------------------------------------+
 | CustodyManager::new(node_id, cgc = CUSTODY_REQUIREMENT = 4)                          |
 |   sampling_size = max(SAMPLES_PER_SLOT = 8, cgc) = 8; get_custody_groups(node, n):   |
 |   custodied (n = cgc) -> 4 groups; sampled (n = sampling_size) -> 8 groups           |
 |   custodied subset-of sampled: debug_assert! only; column_subnets of SAMPLED groups  |
 +--------------------------------------------------------------------------------------+
       |                          |                             |
       v                          v                             v
 subscribe_sampled_columns    required = sampled columns    SubscriptionSet on EngineStream
 via TopicRegistry (TEST-ONLY)    v                         (ADR-P3-07; no in-tree client)
                         +-------------------------------+
 column ACCEPT --x------>| SamplingTracker               |<-x- end of slot N: poll_deadlines
   (STUB feed:           |   <= 64 roots, oldest evicted |     -> recover() by-root ladder
    NoopSamplingFeed)    |   complete iff                |     -> mark_abandoned
 EngineStream inject -x->|   verified == required        |     (TEST-ONLY: no prod caller)
   (no in-tree client)   |                               |
 by-root fetch --x------>|                               |
   (recover() TEST-ONLY) +-------------------------------+
                                  |
                                  x  da_tx = None; E2 data_available arm DEAD-AS-WIRED
                                  v
 chain DA gate (pending_da): p2p never opens it; it exists only with an installed core
```

Caption: `x` on an input = no production producer (as labelled); `x` on the output = DEAD-AS-WIRED
edge (`DataAvailable{root, slot}` never sent). Arrows out of `CustodyManager` show derivation.
Outside devnet mode the only `CustodyManager` and `SamplingTracker` are the EngineStream copies.

- **Custody** (`services/p2p/src/das/custody.rs:175-198`). `set_cgc` (`custody.rs:296-314`) is
  reached only from the TEST-ONLY `cgc_hook::set_custody_group_count`
  (`services/p2p/src/discovery/cgc_hook.rs:181-201`); the gRPC invoker, STUB `UnattachedCgcHook`,
  returns `NotAttached` without touching custody (`services/p2p/src/service.rs:1086-1095`). The
  production instance is `build_minimal_engine_stream(node_id, CUSTODY_REQUIREMENT, Some(metrics))`
  (`services/p2p/src/main.rs:503-507`); MetaData advertises a static `cgc=4`.
- **Sampling** (`services/p2p/src/das/sampling.rs`). Up to `SAMPLING_TASK_BOUND = 64` per-root
  tasks, oldest evicted (`sampling.rs:30`). Completion is all-or-nothing `verified == required`; an
  empty set completes only for an explicit zero-blob block (`sampling.rs:483-508`). On completion it
  would emit `DataAvailable{root, slot}` on `da_tx` (`sampling.rs:510-546`) for
  `ChainIngress::notify_data_available`, which nothing in `services/p2p` calls, so p2p never opens
  chain's DA gate. As wired, with a core installed (checkpoint providers configured) every new block
  that passes the pre-STF checks would park in `pending_da` and expire after 4 slots
  ([05](05-block-lifecycle.md)); in default shape A chain is core-absent and imports nothing.
- **Recovery** (`services/p2p/src/das/recovery.rs:400-547`, TEST-ONLY). Each iteration sends at most
  one `DataColumnSidecarsByRootV1` request to one chosen peer (possibly chosen again), batching its
  custodied share of the missing columns. Per column: at most 3 attempts per peer across at most 4
  distinct peers (`recovery.rs:347-352`); the loop is capped at `missing x 4 x 3 + 4` iterations
  (`recovery.rs:429-433`). An unserved, provably custodied column costs that peer `custody_unserved`
  -15, at most once per peer per `recover()` call (`recovery.rs:427`). The code's own formula
  (`recovery_ladder_worst_case_secs`, `recovery.rs:248-256`) gives 3 x (5 s TTFB + 10 s RESP) = 45 s
  < 48 s (4 slots x 12 s of `pending_da`) but drops `max_peers`; as wired, 3 attempts on each of 4
  peers per column could exceed the 48 s window. Matrix reconstruction is out of scope.
- **Publish side (designed, [ADR-P3-07](../adr/ADR-P3-07.md)).** p2p would publish only
  custody-sampled columns, and the engine side would inject only subscribed indices. At HEAD neither
  half runs: the engine-side inject client was DELETED in `631994c`, the engine-api fastpath drops
  all 128 sidecars (`inject_tx = None`, `crates/engine-api/src/fastpath/mod.rs:230`), and the p2p
  EngineStream server (E8, DEAD-AS-WIRED) ends at `NoopPublisher`
  (`services/p2p/src/engine_stream/server.rs:93-99`).

## 12. Gossip scoring parameters (PARAMS-ONLY)

`build_scoring_config` (`services/p2p/src/gossip/scoring.rs:303-487`) computes a full GossipSub
score config; values: [p2p-scoring.md](../p2p-scoring.md), generated from it. None of it is applied:
`CcBehaviour::new` never calls `with_peer_score` (`crates/libp2p/src/behaviour.rs:337-357`), and
`to_libp2p_scoring_config` / `cc_libp2p::build_peer_score_params` have no production caller. Two
choices matter architecturally: the column family weighs 0.5 in total, spread as
`0.5 / sampling_size` per topic at any cgc (`scoring.rs:124-129`, [ADR-P2-07](../adr/ADR-P2-07.md)),
and P3/P3b are weight 0 on every topic (`scoring.rs:363-371`, [ADR-P2-10](../adr/ADR-P2-10.md)). The
peer manager's decay tick couples a gossip score into the app score
([ADR-P2-09](../adr/ADR-P2-09.md)), but `set_gossip_score` has only test callers, so the gossip
score stays at its default 0.0 and the coupling term is INERT.

## 13. Concurrency, metrics and latent defects

Full inventory: [13-concurrency-model.md](13-concurrency-model.md). `gossip-validate` and
`chain-in-late-verdicts` are unsupervised `cc_bootstrap::spawn` tasks (outside
[ADR-P2-13](../adr/ADR-P2-13.md)'s supervised set): a panic ends the loop silently; after a
`gossip-validate` panic the swarm IGNOREs on `Closed` (`host.rs:537-546`). On this path only the
swarm task is process-fatal. Channel bounds: [09](09-p2p-host-and-discovery.md).

| Metric family (`services/p2p/src/metrics.rs:474-616`; counters gain `_total`) | Fed by | Status |
|---|---|---|
| `cc_p2p_gossip_messages{topic,verdict}`, `cc_p2p_gossip_shed{topic}`, `cc_p2p_peer_penalty{reason}` (gossip reasons), `cc_p2p_queue_depth{q=gossip,pending_*,seen_*}`, `cc_p2p_inclusion_proof_verifications` | `report` (`pipeline.rs:846-847`); swarm size REJECT, shed, queue full (`host.rs:462-548`); `export_occupancy` (`pipeline.rs:235-253`); column step 13 | IDLE |
| `cc_p2p_gossip_validation_seconds`; `cc_p2p_queue_depth{q=kzg}`; `cc_p2p_columns_received{source}`, `cc_p2p_da_outcome`, `cc_p2p_sampling_seconds` | nothing (primed once with topic `none`, `metrics.rs:1157-1161`); KZG bridge and pool workers, which exit at startup; `SamplingTracker`, whose only metrics-bearing copy is the EngineStream one | DEAD-AS-WIRED |
| `cc_p2p_verdict_latency`, `cc_p2p_verdict_late`, `cc_p2p_verdict_timeout`, `cc_p2p_chain_objects_sent`, `cc_p2p_chain_stream_saturation_ratio` | chain-stream client hooks (`services/p2p/src/chain_stream/client.rs:318-355`) | IDLE (ratio stays 0) |

The defects below are static traces. Latent; unreachable at HEAD because no gossip topics are
subscribed.

- **Head-of-line blocking in one loop**: the `chain_out` enqueue (mpsc 1024, `send().await` with
  no timeout), the 12 s backstop, an N2 fetch with no connect timeout, `cmd_tx.send().await`, and
  BLS + Merkle + KZG on a tokio worker while holding the `ValidationPoolState` std mutex
  (`pipeline.rs:547-570`).
- **`late_open` pin race**: the seam calls `on_verdict` (feeding `chain_in`) before resolving the
  oneshot (`crates/seam/src/ipc.rs:917-919`), so the late-verdict task can consume an on-time
  verdict before `report()` pins it; the pin then leaks until the purge above 2048.
- **Column ACCEPTs share the block pin**: columns ACCEPT with the block root as correlation id
  (`services/p2p/src/gossip/validate/column.rs:386,581`), and `report()` pins every such ACCEPT
  (`pipeline.rs:823-836`). As wired, a column ACCEPT reported after its block would replace the
  block entry, so a late chain REJECT would charge `import_invalid` to the column sender; pins for
  roots chain never answers never leave and count toward the 2048 purge (evicting block pins).
- **`cc_p2p_peer_penalty` double count**: a REJECT is counted at `host.rs:1240-1244` and again
  when the peer manager applies the `PeerPenaltyCmd`; `import_invalid` likewise.
- **Hard-coded `Mainnet` preset** in the swarm size check and every validator (`host.rs:499`).

Docs that disagree with HEAD: [running.md](../running.md) (line 370, head following over gossip),
[kzg-benchmark.md](../kzg-benchmark.md) (pool as live consumer), [p2p-scoring.md](../p2p-scoring.md)
(parameters presented as shipped). Stale module docs under `services/p2p/src/`:
`engine_stream/mod.rs:6-10` (shared entry point, single seen set; EngineStream builds its own),
`gossip/validate/column.rs:4-5` (KZG stub; it is c-kzg), `gossip/mod.rs:12-14` (operations called
stubs; they are real validators).

## Planned changes

Everything below is plan, not HEAD. Source: [`plan/architecture.md`](../../plan/architecture.md)
§2.3 (edge table), §3.4, §3.5 and §3.6.

- Planned (§2.3): gossip wiring at a later stage; E1 transport impl selected and E2
  `DataAvailable` wired at S3. Q-8 (Appendix B): the Loop A head-of-line argument stays analytic
  until gossip is subscribed.
- Planned (§3.4, before S3): reconnect `kzg_tx` so column KZG runs on the OS-thread pool; shorten
  the 12 s wait toward the 2 s seam budget; hoist both redrives off the hot loop. After S3: 8 typed
  queues keyed by `ValidatorKind` (FIFO for blocks, columns, parked work, operations; LIFO for
  aggregates, attestations, sync), workers, and a per-family `ValidationPoolState`.
- Planned (§3.5): keep the ADR-P2-08 pool contract, replacing its ad-hoc oldest-drop queue with
  `cc-scheduler`'s `LifoQueue`; [ADR-P2-10](../adr/ADR-P2-10.md) (P3/P3b off) is `proposed`.
- Planned (§3.6): layered shedding (large inbound `GOSSIP_BOUND`, then each typed queue's FIFO/LIFO
  policy, then producer backpressure); an untyped channel bound must never decide a consensus drop.
