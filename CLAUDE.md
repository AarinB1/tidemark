# tidemark

Distributed stream processing engine in Go. Event-time windowing, watermark
propagation, Chandy-Lamport barrier checkpointing, exactly-once output under
worker failure.

Correctness is the deliverable. Throughput is secondary. Bugs in this system are
silent: a wrong watermark produces slightly incorrect window counts, not a
crash. Prefer the obvious implementation over the clever one everywhere.

## Invariants

Violating any of these produces a silent failure. Never "simplify" past one.

1. InputGate output watermark is `min(perChannelWatermark)`, never `max`. Taking
   the max fires windows early on incomplete data and nothing crashes.
2. Records partition to exactly one channel WITHIN an edge, across the subtasks
   of that edge's downstream vertex. When a vertex has several downstream
   vertices, each receives the full stream. Watermarks, barriers, and
   end-of-stream broadcast to every channel on every edge.
3. Sources inject barriers at a fixed element interval, not on a wall clock, and
   inject them regardless of data volume. Logical-position injection is what
   makes a recovered run reproducible from a seed. A quiet channel that stops
   forwarding barriers deadlocks alignment downstream.
4. Sinks commit only on `NotifyCheckpointComplete`, never during snapshot.
   Committing at snapshot time can commit data belonging to a checkpoint that
   never completes, which yields duplicates on recovery.
5. Watermarks and barriers travel in-band as `StreamElement`, never on a side
   channel. In-band is what guarantees they stay ordered relative to records.
6. Fault injection is keyed to logical position (elements processed, barriers
   seen), never wall-clock time. Go's scheduler is not deterministic; the fault
   schedule must be.
7. Sources are seekable and deterministic. `SeekTo(n)` then read produces the
   identical sequence as reading from 0 and discarding the first n. Do not
   implement a source by advancing a held `rand.Rand`; derive element n as a
   pure function of `(seed, n)`.
8. A checkpoint is usable for recovery only after `_COMPLETE` is written. Write
   it last and atomically.

## Definitions

**Identical output** means the final sorted contents of the sink. It never means
the emission order of the record stream. Delivery is at-least-once and ordering
after recovery WILL differ from a clean run. A test that compares emission order
is a broken test.

**Subtask** is one parallel instance of a vertex, identified by
`(vertexID, index)`. Subtasks are the unit of scheduling, state, and failure.

## Scope

Not being built, at any point, in any phase: a DSL or fluent builder API, a
plugin registry, a config file format, YAML, reflection-based serialization
(`encoding/gob`, `encoding/json`) anywhere in the data path, a metrics
abstraction layer, or an interface introduced to support a feature that is not
yet being built. The coordinator/worker split is a committed phase, so a
transport seam sized for it is in scope.

If a change adds an abstraction layer, it is out of scope. If you are about to
write a registry or a builder, stop and say so instead.

Every new exported interface in `pkg/core` requires a stated justification:
which concrete type it replaces, or which committed phase requires polymorphism
at that point. There are four today: `Context`, `Operator`, `Source`, `Sink`.
Data types are uncounted.

Dependencies require explicit approval. Permitted when their phase arrives:
`cockroachdb/pebble`, `google.golang.org/grpc`, `google.golang.org/protobuf`.
Nothing else, and everything must be cgo-free and ARM64-native. Do not add
`golang.org/x/...` helpers; the stdlib equivalent is fine.

## Working agreement

- Work proceeds in numbered steps. One step is one file plus its test. Complete
  a step, run `make check`, and commit before beginning the next.
- Do not modify files outside the current step's stated slice, including files
  belonging to a later step in the same task.
- If a step's stated slice turns out to be wrong, stop and report rather than
  widening it.
- If a file the task assumes exists is missing, stop and report. Do not infer
  its contents and create it.
- A correctness test must exercise a topology where the property it names can
  fail. A test built only on a single-input chain cannot pin an invariant about
  multiple inputs or multiple downstream vertices, however it is named.
- When a test fails, state the root cause before proposing a fix. Do not iterate
  toward green by special-casing.
- Table-driven tests. From Phase 2 onward, every windowed computation asserts
  equality against the batch oracle in `test/oracle`.
- Any decorator wrapping a `core.Source` must explicitly forward the methods the
  runtime dispatches on. Embedding the interface is not sufficient and silently
  disables parallelism.
- `make check` (vet, gofmt, `go test -race`) must pass before every commit.
- One commit per step. Do not squash.
- End each task by explaining the three subtlest decisions made and why.
