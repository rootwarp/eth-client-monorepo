# cc-seam

Typed handles and overflow contracts for internal service edges
(`[ARCH]` §2.1).

- `ChainIngress` / `P2pEgress` — p2p ↔ core (E1+E2).
- `ArchiveWrite` — chain-core → archive typed column ingest (S2-A-04),
  plus the durable-import signatures (ADR-R-08): `commit_anchor`,
  `commit_import`, `set_head`, `commit_snapshot`. Those four default to a
  no-op and do not write. `DaVerdict` is `Available` or `Deferred` — no
  `None`. Canonical rewrite is `head` / `set_head`, not an
  `update_canonical` field. Block persist is `commit_import`, not a separate
  ingest type.
  Payload is `ColumnBatch { parent_root, slot, block_root, index, ssz }`;
  **`index` is a field**. Head is `(parent_root, slot)`: a batch may only
  extend the durable frontier, never jump it. Overflow is policy **A**:
  the writer mailbox (ADR-P4-04) surfaces `SeamError::Backpressure` to
  the import path. Ingest is `S2-A-05` (`chain-core` → live P0 mailbox).
  `commit_snapshot` rides P2 (bound 256, drop-newest) with the ADR-R-08
  exemption: a dropped chunk is not a durable snapshot.
- Event payload layouts (`BlockImportedPayload`, `HeadPayload`,
  `ChainReorgPayload`, `FinalizedCheckpointPayload`) — S2-A-08. Decode
  is fail-closed; consumers must not index event `payload` bytes.

Transport impls:

- `InProcess` — bounded tokio mpsc + oneshot replies (S1-A-08). Single Hull.
  These channels **are** the live queues; do not wrap them in front of the
  scheduler import lane, the event ring, or `publish_fwd`.
- `Ipc` — p2p-side tonic-over-TCP client (`ChainIngress`). `IpcEgress` is a
  local mailbox, not a chain→p2p wire write — do not give p2p the
  `P2pEgress` path. S3 option is a unix socket + `SO_PEERCRED`. The
  jittered reconnect loop lives in `Ipc`; `run_chain_stream_client` is a
  thin proto adapter over it.

**Both impls stay buildable permanently** (`[ARCH]` §9.2 / `[PLAN]` R-14).
The losing impl is never deleted at S3; it is demoted to a test fixture
so the conformance suite stays honest.
