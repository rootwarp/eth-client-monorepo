# ADR-P4-07 — Storage pushes RestoreFromStore over the existing storage→chain edge

- **Status:** superseded-by ADR-R-02 · completed-by: ADR-R-09 · **Date:** 2026-08-16 (reconstructed)
- **Phase:** 4
- **Issues:** S1-B-17, CC-45b
- **Citations:** deleted at S2-J-02 — `restore.rs`, `restore_client.rs`, and `RestoreFromStore` on `chain.proto` are gone. History record only.
- **Provenance:** re-derived from code (2026-08-16)

This is a **history record**. The decision bound the two-process boot. It
does not bind after S2. ADR-R-02 (written at S2) deletes
`RestoreFromStore`. `S2-J-02` deleted the files; this record stays for history.

## Context

Phase 4 split durable state and the live fork-choice store across
processes. Storage opens redb and holds the snapshot + replay set.
Chain owns `Store` on a dedicated OS thread and serves gRPC health
before that store exists.

Compose makes `storage` `depends_on: chain: service_healthy`, and
storage's health peer is `chain` (`docker-compose.yml:142-144`;
`restore_client.rs:4`). Chain cannot be a gRPC client of storage
without a dependency cycle. Restart therefore has to be *pushed* by
the side that holds the data.

A first-ever node has nothing to push. Waiting the full
`chain.restore_grace_seconds` (default **30**) on an empty store would
delay the demoted CC-19 checkpoint-sync fallback for no reason.

## Decision

**Storage pushes `RestoreFromStore` over the existing storage→chain
edge. Chain does not pull.**

- One additive RPC on `ChainService` (`chain.proto:51-65`). Not a new
  service.
- On startup chain enters `AwaitingRestore` and stays gRPC-healthy
  while the grace timer runs (`restore.rs:3-6`).
- Storage dials chain and streams header / state / blocks / footer, or
  `kind: EMPTY`. EMPTY collapses the grace immediately
  (`restore.rs:5-7`). Timeout falls back to checkpoint sync.
- The path is privileged: `BlockSignatureStrategy::NoVerification`, and
  stored `da_status` is applied as a verdict — the DA gate is not
  re-run (`restore.rs:9-11`).

## Consequences

What this made easy:

- Boot without a health-DAG cycle.
- An empty store does not wait 30 s before checkpoint sync.

What this made hard:

- The RPC is served on published `:9001` with no auth and installs
  consensus state with BLS off and caller-chosen DA verdicts.
- `apply_restore_set` is synchronous on the tonic worker (the
  `block_on` panic is a separate, already-patched item: S0-A-28 /
  ADR-R-06).
- A failed restore that never notifies waiters hangs bootstrap
  (`end_stream`).

What this forbids (while the two-process split lasts):

- Chain opening a storage client to pull the durable set.
- Re-verifying BLS or re-running the DA gate on a restore stream.

## Alternatives considered

**Chain pulls from storage.** Rejected. `storage` already
`depends_on: chain: service_healthy`. The reverse client is a compose
cycle.

**A new storage→chain service (a tenth contract).** Rejected at the
time in favour of one additive RPC on the existing edge. That "keep
the count at nine" argument is contemporaneous colour, not a
constraint this record restates.

**Always wait the grace window, including on EMPTY.** Rejected.
First-ever start would pay 30 s to learn there is nothing to restore.

## Refactor impact

**Deleted at S2.** `[ARCH]` §4.2 / §10.5 ADR-R-02: `beacon-core` opens
redb in-process; `chain_core::seed_from_durable` replaces
`apply_restore_set`; `RestoreFromStore` is deleted. The push/pull
question does not survive the process merge.

| Stage | What happens to this record |
|---|---|
| S1 | This file. Code citations stay. |
| S2 (`S2-J-02`) | Delete `restore.rs`, `restore_client.rs`, and the proto RPC. Citations go with the code. ADR-R-02 is the surviving boot record. |
| S2R (ADR-R-09) | Completes the replacement this deletion left open. The trusted anchor is one shot on an uninitialized store, verified in chain, not a second `RestoreFromStore`. This record stays superseded-by ADR-R-02. |
