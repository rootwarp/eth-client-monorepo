# Consensus primitives

> **As built at `7d8833d`** (`develop`, 2026-08-16). Status labels and diagram legend: [README](README.md#status-labels).
> Target design: [`plan/architecture.md`](../../plan/architecture.md) §5 and §4.2 (D-15).

This doc covers the four pure consensus crates (**cc-types**, **cc-crypto**,
**cc-state-transition**, **cc-fork-choice**), the test harness **cc-spec-tests** that hosts part
of their vector runs ([8](#8-cc-spec-tests-and-vector-coverage)), and the EIP-7892 fork digest in
`services/p2p/src/fork_digest.rs`. The crates are synchronous libraries that spawn nothing and do
not depend on tokio; where they run is in [04](04-chain-core.md), [05](05-block-lifecycle.md) and
[13](13-concurrency-model.md). The client is **Fulu-only**: one shape per container, no
`upgrade_to_*`, and both SSZ decode chokepoints reject every `ForkName` except `Fulu`; pre-Fulu
variants exist only to compute domains and fork digests ([9](#9-supported-forks-and-gaps)).

## 1. Status at HEAD

Shape A and shape B both run **core-absent** by default (`checkpoint_providers = []`), so no
fork-choice `Store` exists. "iff core": only when providers are configured and a core is installed.

| Primitive | Shape A | Shape B | Why |
|---|---|---|---|
| `ChainConfig` YAML load + fork-schedule accessors | LIVE | LIVE | A: cc-chain, cc-p2p, storage-core (bundled Hoodi) and the engine container (cc-engine) load it at boot; B: beacon-core and its storage-core ([3.2](#32-chain-config-parsing)) |
| Fork digest / `ForkContext` (p2p) | LIVE | ABSENT | ENR `eth2`, discv5 digest filter and Status ([section 4](#4-eip-7892-fork-digest)); no gossip topic is built from it (0 topics) |
| Fulu-only decode at checkpoint sync | LIVE (iff providers set) | LIVE (iff providers set) | `services/chain/src/checkpoint_sync.rs:1001,1023`; this is what installs a core |
| Fulu-only decode, the other nine sites | IDLE, DEAD-AS-WIRED or TEST-ONLY by site | IDLE or ABSENT by site | per-site table in [3.4](#34-containers-ssz-and-the-fulu-only-chokepoints) |
| Fork-choice anchor store (`get_forkchoice_store`) and `on_tick` | LIVE (iff core) | LIVE (iff core) | anchor built at checkpoint sync (`services/chain/src/checkpoint_sync.rs:1139`); `on_tick` on every SlotTick (`crates/chain-core/src/core.rs:1940`) |
| `get_head` (`compute_deltas`, `apply_score_changes`, `find_head`), `prune_on_finalized` | IDLE (iff core) | IDLE | callers: ApplyAttestations (no in-tree dialer), `on_block` and `finish_imported` (behind the DA gate), and in B `seed_from_durable` (durable set always None). The Head query reads `last_head_root` (`crates/chain-core/src/core.rs:1338-1343`) and GetHead reads `HeadSnapshot` (`crates/chain-core/src/service.rs:277-290`), without recomputing |
| `on_block` -> `state_transition` (STF signature set, per-block `ExecutionStatus`), BLS proposer pre-check | IDLE (iff core) | IDLE | no block input (pre-check: before `on_block`; STF signature set: behind the DA gate); if fed, every block defers at the DA gate before the STF ([05](05-block-lifecycle.md)) |
| `on_attestation` vote path | IDLE (iff core) | IDLE (iff core) | only ApplyAttestations calls it, and that RPC has no in-tree dialer |
| `ExecutionStatus` of the anchor node | LIVE (iff core) | LIVE (iff core) | inserted `Valid` by `get_forkchoice_store` (`crates/fork-choice/src/on_block.rs:158`); IsOptimistic reads it |
| Invalidation walk (CC-35), `on_attester_slashing`, `is_from_block=true`, `is_optimistic_candidate_block` | DEAD-AS-WIRED | DEAD-AS-WIRED | no caller outside `crates/fork-choice` (production passes `is_from_block=false` only); cc-chain loads and logs `safe_slots_to_import_optimistically` but never applies it (`services/chain/src/run.rs:349`), beacon-core has no such knob |
| `deposit_domain()` zero-version helper | DEAD-AS-WIRED | DEAD-AS-WIRED | no production caller; negative fixture of `crates/state-transition/tests/differing_config.rs:195` |
| `CKzgBackend` trusted-setup load | LIVE (load only; every consumer is DEAD-AS-WIRED or IDLE) | LIVE (load only) | p2p: gossip validation, verify pool and EngineStream inject (three loads, [10 §10](10-p2p-gossip-and-das.md#10-kzg-verification-inline-today-pool-disconnected)); `EngineApi` build in cc-chain, beacon-core and cc-engine ([section 5](#5-cc-crypto)) |
| KZG verify pool (`verify_cell_kzg_proof_batch`); `KzgBackendKind` selection | DEAD-AS-WIRED (pool self-terminating) | ABSENT | the pool's sender drops at startup ([10](10-p2p-gossip-and-das.md)); the kind is computed and logged only in cc-chain (`services/chain/src/run.rs:355`), beacon-core does not compute it |
| `RustEthKzgBackend`, `HarnessAvailability`, cc-spec-tests | TEST-ONLY | TEST-ONLY | feature `kzg-rust-eth-kzg` (CI vectors, bench); `#[cfg(test)]` |
| `BeaconState::commit()` | STUB (iff providers set) | STUB (iff providers set) | no-op placeholder for milhouse (`crates/types/src/state/accessors.rs:986`); reached via `warm_canonical_root` at checkpoint sync (`services/chain/src/checkpoint_sync.rs:1122`) |

## 2. Crate stack

```text
 +--------------------------------------------------------------+
 | cc-fork-choice            crates/fork-choice                 |<--- cc-chain-core (core thread),
 | Store<P>  ProtoArray  VoteTracker  CheckpointContext (LRU 8) |     cc-chain (checkpoint sync)
 +--------------------------------------------------------------+
                                v
 +--------------------------------------------------------------+
 | cc-state-transition       crates/state-transition            |<--- cc-chain-core, cc-chain,
 | state_transition  TransitionContext  ExecutionEngine<P>      |     cc-storage-core (replay),
 +--------------------------------------------------------------+     cc-devnet-gen
                                v
 +--------------------------------------------------------------+
 | cc-crypto                 crates/crypto                      |<--- cc-chain-core, cc-chain,
 | bls (blst 0.3.17, min_pk)  domain  kzg (c-kzg 2.1.8)  hash   |     cc-p2p, cc-engine-api,
 +--------------------------------------------------------------+     cc-engine, cc-devnet-gen
                                v
 +--------------------------------------------------------------+
 | cc-types                  crates/types                       |<--- every crate above, plus
 | Preset  ChainConfig  ForkName  SignedBeaconBlock<P>          |     cc-chain, cc-p2p, cc-engine,
 +--------------------------------------------------------------+     cc-store, cc-storage-core,
                                                                      cc-beacon-core, tools
   Arrows: Cargo normal deps (user -> library), not runtime edges. cc-spec-tests: no workspace
   deps; dev-dep of cc-types and cc-p2p. Test crate cc-beacon-import uses all four.
   cc-attestation and cc-beacon-api do not depend on cc-types; cc-storage only as a dev-dep.
```

`scripts/check-crate-dag.sh` encodes the DAG ([02](02-crate-map.md)) but exits 1 at HEAD: its
storage-core opaque-bytes rule flags `crates/storage-core/src/archive_write.rs:26,332,335,395,803`
(the N1 root-check decode at `:332,335`, its import at `:26`, and test-module lines)
([14](14-testing-and-enforcement.md)). Production chain-core and checkpoint sync are
monomorphised to `Mainnet` (`services/chain/src/run.rs:449`, `bin/beacon-core/src/boot.rs:463,497`);
only the N1 root check also tries a `Minimal` decode (`archive_write.rs:330-338`).
`__crypto_surface_markers` (`crates/types/src/lib.rs:121`) lists the only cc-types items cc-crypto
may use (test-asserted). Interior mutability exists in five places: the shuffling LRU (`Mutex` +
`AtomicU64` compute counter, `crates/types/src/state/caches.rs:629-635`), `PeerDasAvailability`,
`TransitionContext` (`RefCell`, not `Sync` by design,
`crates/state-transition/src/block/mod.rs:59`), thread-local test counters, and two process-global
`AtomicU64` counters only tests read (`INVALIDATED_NODES_TOTAL`,
`crates/fork-choice/src/invalidation.rs:32`; `COMPUTE_DELTAS_CALLS`, `on_attestation.rs:442`).

## 3. cc-types

### 3.1 Three sources of consensus parameters

| Source | Mechanism | Examples | Where |
|---|---|---|---|
| Compile-time preset | `trait Preset` (scalar consts + typenum capacities) | `SLOTS_PER_EPOCH`, `PROPOSER_LOOKAHEAD_LEN` (64 / 16), `MaxAttestationsElectra` | `crates/types/src/preset.rs:31`; `Mainnet` `:206`; `Minimal` `:298` |
| Runtime chain config | `ChainConfig` parsed from consensus-specs YAML | fork versions and epochs, `BLOB_SCHEDULE`, churn limits, `SHARD_COMMITTEE_PERIOD`, deposit chain id | `crates/types/src/config.rs:141` |
| Hard-coded constants | `pub const` | DAS sizes (`NUMBER_OF_COLUMNS` 128, `CUSTODY_REQUIREMENT` 4, `SAMPLES_PER_SLOT` 8), `EJECTION_BALANCE`, `PROPOSER_SCORE_BOOST` 40 | `crates/types/src/lib.rs:74-111`; `crates/state-transition/src/helpers/constants.rs:86-107`; `crates/fork-choice/src/head_cache.rs:27` |

Some third-row keys are spec **config** keys that WARN on load ([3.2](#32-chain-config-parsing));
moving them is an unplanned gap ([9](#9-supported-forks-and-gaps)).

### 3.2 Chain config parsing

`ChainConfig::from_yaml_file` / `from_yaml_str` (`crates/types/src/config.rs:196,206`) parse in
three passes. (1) `warn_unknown_yaml_keys` (`:478`) WARNs and discards each key outside the 25
known (`:444`); the Hoodi fixture triggers 22 WARNs per load. (2) Serde reads `RawChainConfig`
(`:406`), defaulting the churn keys, `SHARD_COMMITTEE_PERIOD` and `MAX_BLOBS_PER_BLOCK_ELECTRA`
to mainnet. (3) `TryFrom` (`:310`) validates; `BlobSchedule::try_from_entries` (`:60`) rejects an
empty, unsorted or duplicate schedule, so **a YAML without `BLOB_SCHEDULE` fails to load**. Slot
time is `SECONDS_PER_SLOT`, else `SLOT_DURATION_MS / 1000`, else 12; a mismatch is an error.

#### Network identity and time sources

| Process | `ChainConfig` (if the file fails) | GVR | `genesis_time` / slot timing | EL fork times |
|---|---|---|---|---|
| chain [A], beacon-core [B] | `network_config`, required when `checkpoint_providers` is non-empty; if unset, the source-tree Hoodi fixture, then the `include_str!` copy (`services/chain/src/run.rs:234-262`, `bin/beacon-core/src/boot.rs:266-285`); also passed to `EngineApi::prepare_with_chain_config` (`run.rs:317`). Boot error | anchor-state field from the checkpoint provider, LIVE (iff providers set): the STF reads it from the state; `EpochContext` (`crates/chain-core/src/core.rs:1574`, `epoch_context.rs:28-30`) -> `ChainView` field 10 (`crates/chain-core/src/p2p_stream.rs:416-417`). The chain `genesis_validators_root` key feeds only the dangerous-knob guard (`services/chain/src/run.rs:200,269`) | anchor `genesis_time` + `SECONDS_PER_SLOT`: store time and the `chain-slot-tick` driver (`core.rs:1287-1291`); engine-api `slot_duration_ms` 12000 (`config/chain.toml:65`, `config/beacon-core.toml:28`) for the upcheck cadence and soft deadline | `[el_forks]` (`config/chain.toml:83-86`, `config/beacon-core.toml:42-45`) |
| p2p [A] | `network_config` (`config/p2p.toml:15`); WARN, then the embedded Hoodi copy (`services/p2p/src/service.rs:170-205`) | digest, ENR `eth2`, Status: `config/p2p.toml:16`, ZERO if absent (`service.rs:173-176`). Gossip signature domains use the ChainView GVR instead (`services/p2p/src/gossip/validate/pipeline.rs:446`, `column.rs:647`, `operations.rs:868`), ZERO while chain is core-absent: IDLE (0 topics) | `[clock]` (`config/p2p.toml:32-40`), which also drives the epoch ticks that advance the digest (`service.rs:282-284`); `SlotClock` has no setter and nothing cross-checks it | none |
| storage-core (in storage [A], beacon-core [B]) | config digest: always the bundled Hoodi (`crates/storage-core/src/open.rs:341`). `storage.network_config` feeds only the CC-4A block floor, through a separate parser (`crates/storage-core/src/boot.rs:383`; [07](07-storage.md)). Floor: boot error; a bundled parse failure exits 1 | digest: the `genesis_validators_root` key, else ZERO (`open.rs:322-339`); unset in `config/storage.toml:134`. beacon-core passes its own key through (`bin/beacon-core/src/boot.rs:254`), and `config/beacon-core.toml` does not set it | prune [A]: `genesis_time` (`config/storage.toml:111`) + bundled `SECONDS_PER_SLOT` (`crates/storage-core/src/boot.rs:355-373,586-587`); ABSENT in B (no prune) | none |
| engine container | sandboxed loader in `EngineApi::prepare` (`crates/engine-api/src/network_config.rs:95-101`; `config/engine.toml:18`). Fails before bind | none | `slot_duration_ms` (`config/engine.toml:31`) | `[el_forks]` (`config/engine.toml:55-58`) |

The store's config digest does not currently distinguish networks. No shipped configuration
pairs cc-p2p with cc-beacon-core; how p2p attaches to shape B is not yet defined.

```text
 checkpoint provider (X4), LIVE (iff providers set)    config/*.toml, read once at boot;
   anchor BeaconState: GVR, genesis_time                 nothing cross-checks them
        |                                                    |
        v                                                    v
 +-------------------------------+ E1 ChainView [A] +-------------------------------+
 | chain [A] / beacon-core [B]   |=================>| p2p [A]                       |
 | installed core: GVR and       | genesis_time,GVR | gossip domains: ChainView GVR |
 |   genesis_time from the state | (p2p dials)      |   (ZERO if core-absent; IDLE) |
 | STF domains: state GVR (IDLE) |                  | digest, ENR, Status:          |
 | slot tick: anchor genesis_time|                  |   p2p.toml:16 GVR             |
 |   + SECONDS_PER_SLOT          |                  | SlotClock: [clock], no setter |
 +-------------------------------+                  +-------------------------------+
        |
        +~~~~> EL (geth): JWT iat from the host clock, accepted within +-60 s
 +-------------------------------+
 | storage-core digest: GVR key  |
 |   (else ZERO), bundled Hoodi  |
 | prune [A]: genesis_time key   |
 +-------------------------------+
```

Plain `|` / `v` = where a value comes from (HTTPS fetch or config read); `====>` = E1, LIVE in
shape A; `~~~~>` = DirectEngine -> EL HTTP. storage-core runs inside storage [A] or beacon-core [B].

Nothing reconciles these sources. `cross_check_spec` compares the seven fork versions,
`FULU_FORK_EPOCH`, `SECONDS_PER_SLOT` and `BLOB_SCHEDULE`, but not GVR
(`services/chain/src/checkpoint_sync.rs:286-294,345-426`). If the provider's genesis endpoint and
the anchor state disagree, checkpoint sync only warns (`:1029-1040`). That log claims the endpoint
values are used for domains, but no production code reads `BootstrapSummary.genesis` (written only
at `:1220`; DEAD-AS-WIRED), so domains use the state's GVR. The p2p digest GVR and the gossip
domain GVR can therefore differ. Latent; unreachable at HEAD because no gossip topics are
subscribed. Host clock sync matters: geth accepts a JWT `iat` only within +-60 s, and one 401 is
terminal `AuthFailed` ([08 §3.5](08-execution-engine.md#35-el-health-state-machine)).

### 3.3 Fork-schedule authority

`fork_schedule()` (`config.rs:228`) is a newest-first table of seven `(epoch, ForkName, version)`
rows, Fulu down to Base; adding a fork adds one row. `fork_name_at_epoch` (`:263`),
`fork_version_at_epoch` (`:275`) and `next_fork_after` (`:299`) walk it, skipping
`FAR_FUTURE_EPOCH` rows; `fork_epoch` (`:285`) returns the row's epoch as is.
`scripts/check-fork-schedule-walks.sh` (`make lint`) requires **exactly one** walk outside
`crates/types` (zero also fails): `services/p2p/src/fork_digest.rs`, whose `regular_fork_epochs`
(`:363`) feeds `next_digest_boundary_epoch` (`:331`), which merges in the `BLOB_SCHEDULE` epochs.
`get_blob_parameters::<P>` (`config.rs:219`, `P` unused) binary-searches `BLOB_SCHEDULE`; before
its first entry it returns `(ELECTRA_FORK_EPOCH, MAX_BLOBS_PER_BLOCK_ELECTRA)`.

### 3.4 Containers, SSZ and the Fulu-only chokepoints

Containers derive SSZ and `TreeHash`, one shape per type (Electra/Fulu `BeaconBlockBody<P>`,
Deneb-shape `ExecutionPayload<P>`, `DataColumnSidecar<P>` and the partial-column containers);
`SSZ_STATIC_TYPE_NAMES` lists 59 (`crates/types/src/registry.rs:53`). Two decode chokepoints
return `DecodeError::BytesInvalid` for any fork but `ForkName::Fulu`:
`SignedBeaconBlock::from_ssz_bytes_with` (`crates/types/src/block.rs:135`) and
`BeaconState::from_ssz_bytes_with` (`crates/types/src/state/mod.rs:239`), whose alias
`from_ssz_bytes_hydrated` (`:229`) no longer hydrates anything; raw decode is `pub(crate)`
(`:253`). All **11** non-test call sites hard-code `ForkName::Fulu`, two of them reachable only
from tests; the `ImportBlockRequest` fork is ignored (`crates/chain-core/src/import.rs:1206-1208`).

| Site | Shape A | Shape B | Why |
|---|---|---|---|
| checkpoint sync, block + state (`services/chain/src/checkpoint_sync.rs:1001,1023`) | LIVE (iff providers set) | LIVE (iff providers set) | installs a core (installed core) |
| chain-core import, unary + gossip (`crates/chain-core/src/import.rs:1208`) | IDLE (iff core) | IDLE (iff core) | no block input (E1 object arm IDLE; ImportBlock has no in-tree dialer) |
| seed (`crates/chain-core/src/seed.rs:171`) | ABSENT | IDLE | `seed_from_durable` is shape B only; durable set always None |
| storage-core replay (`crates/storage-core/src/replay.rs:581,627`) | IDLE | ABSENT | the replay task skips while `split.slot == 0`; beacon-core hosts no replay |
| N1 root check (`crates/storage-core/src/archive_write.rs:332,335`) | DEAD-AS-WIRED | IDLE (iff core) | A: cc-storage builds the ArchiveWriter and drops it (N1 is ABSENT); B: no block passes the DA gate |
| p2p gossip block (`services/p2p/src/gossip/validate/block.rs:84`) | IDLE | ABSENT | 0 gossip topics subscribed |
| p2p below-anchor backfill (`services/p2p/src/backfill/below.rs:335,376`) | TEST-ONLY | ABSENT | `verify_below_batch` has no production caller (re-export only, `services/p2p/src/backfill/mod.rs:20`) |

`crates/types/src/light_client.rs` holds the light-client containers (Electra branch depths);
they exist only for `ssz_static` (`crates/types/src/registry.rs:18-20,81-85`) and have no runtime
user (TEST-ONLY).

### 3.5 BeaconState and merkleization

`BeaconState<P>` (`crates/types/src/state/mod.rs:70`) is a flat struct with 38 private spec fields
(`crates/types/src/state/caches.rs:20`) plus a non-spec `caches: StateCaches<P>` that SSZ,
tree-hash and `PartialEq` exclude; `List` / `Vector` alias the `ssz_types` lists
(`state/mod.rs:50,53`), the milhouse seam. The STF uses the hot root path
([ADR-P1-04](../adr/ADR-P1-04.md)), `canonical_root(&mut self)` -> `commit()` (no-op) ->
`recompute_caches()` -> container root (`crates/types/src/state/accessors.rs:995,1006`); the cold
derived `TreeHash::tree_hash_root` serves `ssz_static` and `crates/types/tests/state_hashing.rs`.
`ShufflingCache` (16-entry `Mutex<LruCache>`, `caches.rs:596,629`) is never invalidated;
`PubkeyIndexMap` (`caches.rs:512`) is owned by `TransitionContext`. The PeerDAS custody helpers
are in `crates/types/src/networking/custody.rs:33-99` ([10](10-p2p-gossip-and-das.md)).

## 4. EIP-7892 fork digest

```text
 YAML --> ChainConfig (cc-types): fork_schedule() rows ... (0, Base, GENESIS_VERSION)
            Hoodi: FULU_FORK_EPOCH 50688; BLOB_SCHEDULE [(52480, 15), (54016, 21)]
              |
 compute_fork_digest(cfg, gvr, epoch)               services/p2p/src/fork_digest.rs:78
   version = cfg.fork_version_at_epoch(epoch)
   base    = compute_fork_data_root(version, gvr)   crates/crypto/src/domain.rs:63
           = hash_tree_root(ForkData), version zero-padded to 32 B; NOT sha256(version||gvr)
   epoch < FULU_FORK_EPOCH ? --yes--> digest = base[0..4]
              | no
              v
   bp     = blob_parameters(cfg, epoch)       fork_digest.rs:323; dispatches on PRESET_BASE
            before the 1st BPO: (ELECTRA_FORK_EPOCH, MAX_BLOBS_PER_BLOCK_ELECTRA)
   mask   = sha256(u64_le(bp.epoch) || u64_le(bp.max_blobs_per_block))
   digest = (base XOR mask)[0..4]
 Hoodi vectors (services/p2p/tests/fork_digest_vectors.rs:50-65): e50000 Electra 82556a32,
   e51000 Fulu with fallback (2048, 9) e2abcca4, e52480 BPO1 ae9f70a0, e54016 BPO2 c6ecb76c
```

Plain arrows show derivation, not runtime edges. From Fulu the XOR applies **even before the
first BPO**. Inside p2p the digest is used as follows:

```text
 config/p2p.toml: network_config + genesis_validators_root (:16; ZERO if absent)
        v
 build_fork_context (services/p2p/src/service.rs:170) -> ForkContext (fork_digest.rs:196)
        +--> swarm task      on_epoch at StatusEpoch (host.rs:1331)
        |      current_digest() -> status/2 fork_digest (host.rs:1026,1151)             LIVE
        +--> discovery task  on_epoch on each epoch tick (discovery/task.rs:509)
               enr_fork_id() -> ENR eth2 (discovery/enr.rs:706)                         LIVE
               discovery_allowed_digests() -> discv5 peer filter (task.rs:486,522,558)  LIVE
 gossip topic strings /eth2/{digest}/...: not built in production (0 topics)
```

Each task advances its own `ForkContext` clone. The allowed set (`fork_digest.rs:157`) adds the next
digest one epoch before a boundary and the previous one at it ([09](09-p2p-host-and-discovery.md),
[11](11-p2p-reqresp-and-sync.md)). `EnrForkId` (`:37`, built by `enr_fork_id` `:126`) tracks regular
forks in `next_fork_version` but regular forks **or** BPOs in `next_fork_epoch`. At HEAD p2p
subscribes to no gossip topics; the plan schedules gossip wiring for a later stage.

## 5. cc-crypto

**BLS** (`crates/crypto/src/bls/mod.rs`): blst `min_pk`, DST
`BLS_SIG_BLS12381G2_XMD:SHA-256_SSWU_RO_POP_` (`:25`). Subgroup and infinity checks run **once**,
at deserialisation (`PublicKey::deserialize` `:99`, `Signature::deserialize` `:149`); verify paths
pass `validate=false`. `SignatureSet` (`crates/crypto/src/bls/batch.rs:59`) checks all triples in
one `verify_multiple_aggregate_signatures` (`:168`) with fresh non-zero 64-bit `getrandom`
coefficients; a `getrandom` failure yields `false`. `SecretKey` needs `cfg(test)` or the `signing`
feature (`bls/mod.rs:339`), which `cc-devnet-gen` (offline tool) and cc-p2p enable as a normal
dependency (`services/p2p/Cargo.toml:26`): it is compiled into cc-p2p but used only by its tests;
cc-chain-core and cc-chain enable it only for tests. Features (`crates/crypto/Cargo.toml:13-20`):
`kzg-c-kzg` (default), `kzg-rust-eth-kzg` (TEST-ONLY), `signing`.

**Domains** (`crates/crypto/src/domain.rs`): `compute_fork_data_root` (`:63`), `compute_domain`
(`:76`; a `None` version or GVR becomes all-zero bytes before hashing), `compute_signing_root`
(`:93`), `get_domain` (`:107`). Callers pass the versions: exits use `capella_fork_version`
(EIP-7044, `crates/state-transition/src/signatures.rs:370`); BLS-to-execution changes (`:394`) and
deposits (`crates/state-transition/src/block/operations/deposit.rs:47`) use `genesis_fork_version`.

**KZG**: the `CellKzg` trait (`crates/crypto/src/kzg/trait.rs:139`, `Send + Sync`) has five
methods; a verify returns `Ok(true)` for valid, `Ok(false)` for an invalid proof and `Err` for
malformed input ([ADR-P1-05](../adr/ADR-P1-05.md)). The trusted setup is `include_str!`-embedded
(`crates/crypto/src/kzg/setup.rs:12`), and `CKzgBackend::load_default()` (`kzg/c_kzg.rs:29`)
re-parses it on every call. Failure handling is asymmetric: p2p (verify pool) only logs and swaps
in `FailClosedCellKzg` (`services/p2p/src/service.rs:746-753`), while `production_cell_kzg()`
(`crates/engine-api/src/fastpath/mod.rs:95`), run by both `EngineApi::prepare` variants, is fatal
before bind (`EngineBuildError::Kzg`, `crates/engine-api/src/api.rs:123,146`); the fastpath then
discards its output ([08](08-execution-engine.md)). [kzg-benchmark.md](../kzg-benchmark.md) has
backend timings; its "consumed at CC-18b" claim predates HEAD (the kind is only logged).

## 6. cc-state-transition

```text
 state_transition(state, signed, ctx, verify)       crates/state-transition/src/block/mod.rs:146
  |-- 1 process_slots(state, block.slot, config)                              slots.rs:27
  |       per slot: process_slot (canonical_root -> state_roots[i], block_roots[i]);
  |       at an epoch start process_epoch (epoch/mod.rs:110). NO fork-upgrade step
  |-- 2 verify_block_signatures(state, block, config, verify)          signatures.rs:415
  |       proposer + RANDAO + every operation signature -> one BlockSignatureSet
  |       VerifyBatch: batch, then one by one on failure (attribution); NoVerification: skip
  |-- 3 process_block_with_strategy                                    block/mod.rs:199
  |       top_up_pubkey_cache -> process_block_header -> process_withdrawals
  |       -> process_execution_payload: local checks, blob count <= get_blob_parameters,
  |            versioned hashes, ctx.engine.verify_and_notify_new_payload (sole call site)
  |            -> PayloadStatus into ctx outbox; Invalid* -> Err(Engine(InvalidPayload))
  |       -> process_randao -> process_eth1_data -> process_operations(verify=false)
  |       -> process_sync_aggregate (BLS unless NoVerification) -> state.commit()
  `-- 4 canonical_root() != block.state_root -> Err(StateRootMismatch)
```

The plain `->` arrows show call order; [section 1](#1-status-at-head) gives the status. The module
map mirrors `specs/fulu/beacon-chain.md` (`crates/state-transition/src/lib.rs:1-18`); there is no
per-fork dispatch, and `process_epoch` runs 15 handlers in spec order.

**Root budget and epoch cache.** Every STF state root goes through `measured_canonical_root`,
with thread-local call and elapsed-time counters; tests assert exactly two calls per block-slot
transition (`crates/state-transition/src/root_measure.rs:1-10`; `block/mod.rs:304`).
`epoch_cache.rs` caches `total_active_balance`, `base_reward_per_increment` and the active index
sets, and any registry or effective-balance change invalidates it
(`crates/state-transition/src/epoch_cache.rs:1-8`; `note_registry_or_effective_balance_change`
`:54`).

**Signature strategy.** `BlockSignatureStrategy` (`crates/state-transition/src/lib.rs:74`) is
`VerifyIndividual`, `VerifyBatch` (`#[default]`; no production caller) or `NoVerification`.

| Caller | Strategy |
|---|---|
| every chain-core import: unary `ImportBlock`, gossip STF, pending_da / pending_engine re-drive | `CoreConfig.verify`, default `VerifyIndividual` (`crates/chain-core/src/core.rs:843`; `import.rs:484`; re-drive `core.rs:2114,2194`); both hosts keep the default |
| chain-core gossip proposer pre-check | always `VerifyIndividual` (`crates/chain-core/src/import.rs:754-761`) |
| durable seed replay, storage-core replay, residency rematerialise | `NoVerification` (`crates/chain-core/src/seed.rs:257`; `crates/storage-core/src/replay.rs:562`; `crates/chain-core/src/residency.rs:281`) |
| spec vectors | `bls_setting` 0 -> `NoVerification`; 1 and 2 -> `VerifyIndividual` |

**Engine seam and errors.** `ExecutionEngine<P>::verify_and_notify_new_payload`
(`crates/state-transition/src/engine_seam.rs:71`) has one call site
([ADR-P3-03](../adr/ADR-P3-03.md)); its five `PayloadStatus` variants
([ADR-P3-04](../adr/ADR-P3-04.md)) fold into `is_not_validated` and `is_invalidated`.
Implementations: `DirectEngine` (`crates/chain-core/src/engine.rs:120`), the always-Valid
`ReplayAcceptEngine` (storage-core replay) and test engines. Every `BlockError` has an exhaustive
`gossip_class()` (`crates/state-transition/src/error.rs:280`; [ADR-P1-07](../adr/ADR-P1-07.md));
a registry pubkey that fails to decode is Internal, block-carried material Reject. DirectEngine
maps every engine-api failure to `Engine(Transport)` (`crates/chain-core/src/engine.rs:199-201`),
a fork-choice deferral ([7.2](#72-on_block-justification-and-finalization)). Engine failures
defer the block rather than reject it; re-drive depends on an EL health transition.

## 7. cc-fork-choice

`Store<P>` is owned by value on the core thread ([ADR-P1-09](../adr/ADR-P1-09.md)). Every
head-affecting mutator bumps `mutation_counter` and clears the head cache (`store.rs:432`);
`set_time` does not.

**Anchor store construction (LIVE (iff core)).** Checkpoint sync calls `get_forkchoice_store`
(`crates/fork-choice/src/on_block.rs:112`; `services/chain/src/checkpoint_sync.rs:1139`): justified
= finalized = (anchor epoch, anchor root), time = genesis + slot x `seconds_per_slot`, the anchor
proto node inserted `Valid` (trusted), header and state stored, and the justified
`CheckpointContext` and balance snapshot filled so the first `compute_deltas` does not see zero
balances. It shares chain-core's `PeerDasAvailability` (`:1137`); `on_tick` follows (`:1155`).

```text
 Store<P>                                                    crates/fork-choice/src/store.rs:122
 +----------------------------------------------------------------------------------------------+
 | time  genesis_time  seconds_per_slot  justified/finalized/unrealized_*: Checkpoint           |
 | proposer_boost_root  equivocating_indices: BTreeSet  block_timeliness  last_head_root        |
 | blocks: HashMap<Root, BeaconBlockHeader>  block_states: HashMap<Root, BeaconState<P>>        |
 | checkpoint_contexts: LruCache<(Epoch, Root), Arc<CheckpointContext>> (8)  justified_balances |
 | votes: Vec<VoteTracker> (dense)  head_cache: Option<CachedHead>  mutation_counter: u64       |
 | engine: Arc<dyn ExecutionEngine<P>>  da: Arc<dyn DataAvailability>  proto_array: ProtoArray  |
 +----------------------------------------------------------------------------------------------+
                     |
                     v
 ProtoArray { nodes: Vec<ProtoNode>, indices: HashMap<Root, usize>,
              justified/finalized: Checkpoint, previous_proposer_boost }
   append-only; parent index < child index; prune() is the only index-invalidating op
   ProtoNode: slot root parent state_root target_root (unrealized_)justified/finalized
              weight best_child best_descendant execution_status execution_block_hash
   A [0]          (array index in brackets)
   `-- B [1]
       +-- C [2]
       `-- D [3]
           `-- E [4]     get_head = best_descendant(justified root) = E when D outweighs C
```

### 7.1 Head selection (LMD-GHOST over proto-array)

`on_attestation` (`crates/fork-choice/src/on_attestation.rs:329`) runs the spec's slot, target and
LMD checks but **not** `is_valid_indexed_attestation`: no BLS, committee or sorted-unique check
(`:18-23`). Its only production caller, ApplyAttestations, is thus a privileged weight-injection
surface (`crates/chain-core/src/apply_attestations.rs:11-25`) with no in-tree dialer (IDLE). It
does no weight math: it adds a missing target `CheckpointContext` to the LRU of 8
([ADR-P1-08](../adr/ADR-P1-08.md)), then writes `votes[i]` (O(1) per index), skipping equivocators.

`get_head` (`crates/fork-choice/src/head_cache.rs:154`) returns the cached head while
`mutation_counter` is unchanged. Otherwise it:

1. runs `compute_deltas` (`on_attestation.rs:455`) over the votes, the old balances, the new
   justified `CheckpointContext` balances and the equivocators; all-or-nothing (votes restored);
2. computes the proposer boost `(total_active / SLOTS_PER_EPOCH) * 40 / 100`
   (`compute_proposer_boost_score`, `head_cache.rs:242`), applied as a reversible delta;
3. runs `apply_score_changes` (`proto_array.rs:587`): two reverse passes swap the old boost for
   the new, add the deltas, hold `Invalid` nodes at weight 0 (none exist at HEAD;
   [7.3](#73-optimistic-bookkeeping-and-invalidation)) and rebuild best links via
   `node_leads_to_viable_head` (`:832`); ties go to the higher root;
4. calls `find_head(justified.root)` (`:684`); a node is viable (`:400`) if it is not Invalid and
   its justified (voting source) and finalized checkpoints are correct. `detect_reorg` emits at
   most one `ChainReorg`. `get_proposer_head` and public `compute_pulled_up_tip` are TEST-ONLY.

### 7.2 on_block, justification and finalization

`on_block_with_context` (`crates/fork-choice/src/on_block.rs:202`) runs these steps in order:

0. A fully imported root (header and proto node) returns `Imported`; a header without a proto
   node resumes via `complete_partial_import` (`:212-228`).
1. **DA gate** (`:232`), first check on a new block: unavailable -> `Deferred(DataUnavailable)`.
2. Missing parent -> `Deferred(UnknownParent)`; future slot -> `Deferred(FutureSlot)`; slot <=
   finalized start slot or wrong finalized ancestor -> `Err(NotDescendedFromFinalized)` (Reject).
3. `state_transition` on a parent-state clone; `Engine(Transport)` returns
   `Deferred(ExecutionEngineUnavailable)` with the store unmutated.
4. Map the payload-status outbox to an `ExecutionStatus`. An empty outbox gives `Irrelevant`,
   DEAD-AS-WIRED on Fulu since `process_execution_payload` always calls the `ExecutionEngine` seam.
5. `get_head` before the insert, for the proposer-boost gate.
6. `integrate_block` (`:398`): unrealized checkpoints from `process_justification_and_finalization`
   on a clone; proto-array insert **before** header and state; vote tables grown; realized,
   justified-context and unrealized checkpoints updated (a prior-epoch block promotes unrealized).
7. If Valid, propagate validation from the parent; record timeliness; update the boost root.

Deferrals are `Ok(Deferred(_))` (Ignore). `on_tick` (`on_tick.rs:16`) runs per SlotTick
(`crates/chain-core/src/core.rs:1940`; errors such as backwards time are debug-logged and
skipped), clearing the boost each slot and promoting unrealized checkpoints at epoch starts;
`prune_on_finalized` (`store.rs:575`) runs when an import advances finalization
(`crates/chain-core/src/import.rs:1085`). The DA seam (`da_seam.rs:56`) has one production
implementation, `PeerDasAvailability`: up to 256 roots, oldest evicted first, no zero-blob
exemption. Its two production markers, DataAvailable (`crates/chain-core/src/core.rs:2085`; p2p
never sends it) and durable seed replay (`crates/chain-core/src/seed.rs:236`; never runs), do not
fire at HEAD, so the gate never opens ([05-block-lifecycle.md](05-block-lifecycle.md)).

### 7.3 Optimistic bookkeeping and invalidation

Each `ProtoNode` carries an `ExecutionStatus` ([ADR-P3-10](../adr/ADR-P3-10.md);
`execution_status.rs:30,65-67`): Valid -> Valid, Syncing or Accepted -> Optimistic, Invalid or
InvalidBlockHash -> Invalid. A Valid import clears optimistic ancestors (`validation.rs:96`). The
H-3 guard (`h3_execution_hash_ok`, called at `proto_array.rs:254`) refuses an Optimistic or
Invalid node with a zero execution hash at runtime.

No production path creates an `Invalid` node: an INVALID status fails the STF before the insert
(`crates/state-transition/src/block/execution_payload.rs:86-89`), and the CC-35 code
(`apply_invalidation`, `invalidate_from_latest_valid_hash`: `invalidation.rs:225,237`;
`propagate_execution_payload_invalidation`: `invalidation_walk.rs:171`;
`try_mark_execution_invalid`) is **DEAD-AS-WIRED** ([ADR-P3-11](../adr/ADR-P3-11.md)). An EL
INVALID rejects only the new block, never its Optimistic descendants; `on_attester_slashing` is
uncalled, so equivocators keep their LMD weight.

## 8. cc-spec-tests and vector coverage

The pin (`v1.7.0-alpha.13`), fetch and `cc-spec-tests` harness are in
[14 §Spec vectors](14-testing-and-enforcement.md#spec-vectors). The state-transition and
fork-choice runners copy the harness, because their crate-DAG rows forbid `cc-spec-tests`:

| Runner | Harness | Marker check | Snappy cap | Skiplist |
|---|---|---|---|---|
| cc-types `ssz_static`, `ssz_generic`, `custody`; p2p `custody_subset` | cc-spec-tests | yes | yes | not read |
| cc-state-transition (operations, epoch_processing, sanity, finality, random, rewards, shuffling) | local copies | `is_dir` only | no | local `skiplist_prefixes` (none in `shuffling`), no stale check |
| cc-fork-choice `fork_choice.rs` | local copy | `is_dir` only | no | local `skiplist_prefixes` (`:85`), no stale check |

`SkipList` and `assert_all_match_cases` have no caller outside `crates/spec-tests` (TEST-ONLY), so
nothing enforces the stale-entry rule in [spec-vectors-skiplist.md](../spec-vectors-skiplist.md).

**Covered** (both presets unless noted): `ssz_static` (59 types); `ssz_generic` (phase0 general);
`networking` custody handlers (mainnet only); `operations` (12 handlers); `epoch_processing`
(15); `sanity`, `finality`, `random`, `rewards`; `shuffling` (phase0 path); `fork_choice`, plus
`fork_choice_compliance` (minimal). **No runner** (invisible to the empty skiplist): `fork/fork`
(`upgrade_to_fulu`), `transition/core`, `light_client/single_merkle_proof`, `merkle_proof`,
`sync/optimistic`, `networking/gossip_*`, `general/altair/bls`; no KZG suite exists at this pin
(`crates/crypto/tests/kzg.rs` uses dual-backend identity tests). The BLS tests read
`${HOODI_FIXTURES_CACHE:-$HOME/.cache/cc-hoodi-fixtures}` and **skip** (pass) without it
(`crates/crypto/tests/bls.rs:4-6,464`), so without `scripts/fetch-hoodi-fixtures.sh` no BLS test
runs against chain data.

## 9. Supported forks and gaps

| Fork | `ForkName` | Fork version / domains / digest | SSZ decode | STF |
|---|---|---|---|---|
| Base .. Electra | yes | yes (via the `ChainConfig` table) | rejected | no (no pre-Fulu processing, no `upgrade_to_*`) |
| Fulu | yes | yes, plus BPO via `BLOB_SCHEDULE` | yes | yes (the only shape) |

Gloas has no `ForkName` variant, SSZ decode or STF: only Gloas-era domain constants exist
(`crates/crypto/src/domain.rs:40-48`), and `GLOAS_*` YAML keys WARN as unknown. Nothing upgrades
a pre-Fulu state, so the node can start only from a Fulu anchor (checkpoint sync).

Open gaps: a `PRESET_BASE: minimal` YAML is not rejected by the Mainnet-only services;
Electra-era block bytes may decode under the Fulu decoder (unverified); `bls_setting 2` maps to
`VerifyIndividual` in vector runs; an empty `BLOB_SCHEDULE` fails load; and the invalidation walk
and attester slashing are unwired. Not yet addressed at HEAD. Unplanned gaps (no plan item): the
hard-coded config keys of [3.1](#31-three-sources-of-consensus-parameters) are not `ChainConfig`
fields, and nothing plans to wire the CC-35 walk or `on_attester_slashing` into chain-core.

### Fork and BPO change checklist

"Adding a fork adds one row" ([3.3](#33-fork-schedule-authority)) covers only cc-types.

- **New `BLOB_SCHEDULE` entry (BPO).** Edit the network YAML; the p2p digest follows on its own
  (`services/p2p/src/fork_digest.rs:331`), and checkpoint sync's `cross_check_spec` rejects a
  schedule that differs from the provider's (`services/chain/src/checkpoint_sync.rs:957`). `crates/types/tests/fixtures/hoodi-config.yaml` is
  also the bundled fallback for chain and beacon-core (`services/chain/src/run.rs:234-262`,
  `bin/beacon-core/src/boot.rs:266-285`), p2p
  (`services/p2p/src/service.rs:170-205`), the storage digest
  (`crates/storage-core/src/open.rs:341-358`) and storage prune
  (`crates/storage-core/src/boot.rs:440-466`). p2p falls back straight to its `include_str!` copy;
  the others try the source-tree path first, then that copy. Rebuild images after editing it. The store digest includes `BLOB_SCHEDULE`
  (`crates/store/src/schema.rs:142-160`) but reads only the bundled fixture: changing it makes
  every existing store refuse open with `ConfigDigestMismatch` ([07 §3.4](07-storage.md#34-open-gates)),
  while a BPO added only to an operator's YAML leaves the digest unchanged. `[el_forks]`
  `bpoN_time` does not change method selection: Prague, Osaka and every BPO select
  `engine_newPayloadV4` (`crates/engine-api/src/version.rs:94-131`; only `bpo1_time` and
  `bpo2_time` exist, `:80-89`).
- **New fork (Gloas / Amsterdam).** A `ForkName` variant, `fork_schedule()` row and `ChainConfig`
  fields; the 11 `ForkName::Fulu` decode sites ([3.4](#34-containers-ssz-and-the-fulu-only-chokepoints));
  `REQUIRED_CONSENSUS_VERSION = "fulu"` (`services/chain/src/checkpoint_sync.rs:54`) and the
  cross-check version list (`:286-294`); the engine-api arms `method_for` (`version.rs:127-131`)
  and `forkchoice_method_for` (`:144`). Setting `amsterdam_time` before that code exists makes
  every newPayload at or after it fail `UnsupportedFork` before any HTTP
  ([08 §3.3](08-execution-engine.md#33-methods-versions-and-timeouts)); DirectEngine maps that to
  `Engine(Transport)`, which defers the block ([6](#6-cc-state-transition)). A new fork-epoch
  field in `ConfigDigestPayload` needs a `SCHEMA_VERSION` bump
  (`crates/store/src/schema.rs:36,139-140`), which makes every existing store refuse open
  (`SchemaVersionMismatch`).

### Stale comments in these crates

- `crates/crypto/src/bls/mod.rs:244-247`: "Restore uses NoVerification"; E4 is DELETED (60c6200).
- `crates/state-transition/src/engine_seam.rs:82-85` cites the old `services/chain/src/engine.rs`.
- `crates/state-transition/src/block/mod.rs:51`: "writes and leaves unread"; `on_block` reads it.
- `crates/crypto/src/kzg/mod.rs:33`: "Consumed by `services/chain`"; chain only logs the kind.
- `crates/fork-choice/src/on_attestation.rs:18-23`: "Phase 5 verifies"; ApplyAttestations does not.
- 29 comments in `crates/fork-choice/src` (e.g. `lib.rs:1`) cite section 6 of the architecture
  plan for fork choice; in `plan/architecture.md` §6 is now the trust model.

## Planned changes

Items from [`plan/architecture.md`](../../plan/architecture.md) §5; absent at HEAD unless done.

- **Done:** `pub mod network` deleted (its keys are `ChainConfig` fields); schedule accessors in
  cc-types with a script-enforced single walk; unknown YAML keys WARN and are discarded. The
  `restore.rs` raw-decode bypass was closed via `from_ssz_bytes_hydrated` and `pub(crate)` raw
  decode; `restore.rs` itself is DELETED (60c6200, with E4).
- **Planned:** replace the 11 `ForkName::Fulu` decode sites with
  `config.fork_name_at_epoch(epoch)`; plan §5.3 still counts five sites.
- **Planned** (plan §5.4): `ForkName::Gloas` plus `gloas_fork_version` / `gloas_fork_epoch`;
  `superstruct` enums for the containers EPBS reshapes (`DataColumnSidecar` to be confirmed),
  with STF dispatch on monotone `X_enabled()` predicates, not per-fork modules.
- **Planned:** a milhouse swap behind `List` / `Vector` / `commit()`, ordered before the Gloas
  schema (plan §5.5); its prerequisite, a pubkey cache off `BeaconState`, is done (§4.2 D-15).
- **Planned** (§5.6): build-failing coverage of `upgrade_to_fulu`, `transition`, `light_client`.
