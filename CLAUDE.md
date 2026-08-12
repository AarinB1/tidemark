# tidemark

Distributed stream processing engine in Go. Event-time windowing, watermark propagation, Chandy-Lamport barrier checkpointing, exactly-once output under worker failure.

Correctness is the deliverable. Throughput is secondary. Bugs in this system are silent: a wrong watermark produces slightly incorrect window counts, not a crash. Prefer the obvious implementation over the clever one everywhere.

## Invariants

Violating any of these produces a silent failure. Never "simplify" past one.

1. InputGate output watermark is `min(perChannelWatermark)`, never `max`. Taking the max fires windows early on incomplete data and nothing crashes.
2. Records partition to exactly ONE downstream channel. Watermarks, barriers, and end-of-stream broadcast to ALL downstream channels.
3. Sources inject barriers on schedule regardless of data volume. A quiet channel that stops forwarding barriers deadlocks alignment downstream.
4. Sinks commit only on `NotifyCheckpointComplete`, never during snapshot. Committing at snapshot time can commit data belonging to a checkpoint that never completes, which yields duplicates on recovery.
5. Watermarks and barriers travel in-band as `StreamElement`, never on a side channel. In-band is what guarantees they stay ordered relative to records.
6. Fault injection is keyed to logical position (elements processed, barriers seen), never wall-clock time. Go's scheduler is not deterministic; the fault schedule must be.
7. Sources are seekable and deterministic. `Seek(n)` then read produces the identical sequence as reading from 0 and discarding the first n. Do not implement a source by advancing a held `rand.Rand`; derive element n as a pure function of `(seed, n)`.
8. A checkpoint is usable for recovery only after `_COMPLETE` is written. Write it last and atomically.

## Definitions

**Identical output** means the final sorted contents of the sink. It never means the emission order of the record stream. Delivery is at-least-once and ordering after recovery WILL differ from a clean run. A test that compares emission order is a broken test.

**Subtask** is one parallel instance of a vertex, identified by `(vertexID, index)`. Subtasks are the unit of scheduling, state, and failure.

## Scope

Not being built, at any point, in any phase: a DSL or fluent builder API, a plugin registry, a config file format, YAML, reflection-based serialization (`encoding/gob`, `encoding/json`) anywhere in the data path, a metrics abstraction layer, or an interface introduced to support a feature that is not yet being built.

If a change adds an abstraction layer, it is out of scope. If you are about to write a registry or a builder, stop and say so instead.

The public API surface stays under 10 exported types.

Dependencies require explicit approval. Permitted when their phase arrives: `cockroachdb/pebble`, `google.golang.org/grpc`, `google.golang.org/protobuf`. Nothing else, and everything must be cgo-free and ARM64-native. Do not add `golang.org/x/...` helpers; the stdlib equivalent is fine.

## Current phase

Phase 0. These do not exist yet and must not be invented early:

- `StateBackend` or any state abstraction (Phase 3)
- watermark generation, event-time extraction, timers, windows (Phase 2)
- barrier handling beyond the `KindBarrier` constant existing (Phase 3)
- batching on channels (Phase 1)
- checkpoint coordinator, recovery (Phase 3)
- gRPC or any cross-process transport (Phase 7)

If a task seems to require one of these, the task is out of phase. Say so rather than building it.

## Working agreement

- One file plus its test per session. Do not modify files outside the stated slice.
- When a test fails, state the root cause before proposing a fix. Do not iterate toward green by special-casing.
- Table-driven tests. From Phase 2 onward, every windowed computation asserts equality against the batch oracle in `test/oracle`.
- `make check` (vet, gofmt, `go test -race`) must pass before a commit.
- End each session by explaining the three subtlest decisions made and why.
