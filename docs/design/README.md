# Design notes

Explainers for the hard parts: how things work and the traps found while building
them, with evidence from the stack. Decisions themselves are in [../adr](../adr);
operations in [../runbooks](../runbooks).

| Note | Covers |
|---|---|
| [cdc-event-semantics](cdc-event-semantics.md) | what an event contains: full after-image, TOAST, default-filled deletes, LSN order, transactions |
| [sink-commit-protocol-and-gate](sink-commit-protocol-and-gate.md) | worker/coordinator protocol, the races, the completeness gate, the 2026-11-04 sink failure |
| [iceberg-write-path-and-maintenance](iceberg-write-path-and-maintenance.md) | buffering, metadata-only commits, small files, optimize, expiry gaps |
| [daily-windows-and-publication](daily-windows-and-publication.md) | windows, cut-offs, closed days, FULL vs INCREMENTAL, hashes |
| [reconciliation](reconciliation.md) | proving gold against Postgres; input to milestone j |
