# tidemark

tidemark is a distributed stream processing engine written in Go. It processes
unbounded record streams in event time: watermarks propagate through the
dataflow graph to decide when a window is complete, Chandy-Lamport barriers cut
consistent distributed snapshots of operator state, and sinks commit only once a
checkpoint is confirmed, so output stays exactly-once even when a worker dies
mid-job. Correctness is the deliverable and throughput is secondary — windowed
results are validated against a batch oracle, and failures are injected at
deterministic logical positions rather than on a wall clock, so a run that
recovers from a crash is reproducible and its sink contents match a clean run.

Status: Phase 0
