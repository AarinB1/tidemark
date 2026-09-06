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

Status: Phase 6b.

## Benchmarks

Every published number comes from one reference machine, because `make
bench-check` compares a run against the committed baseline only when the two
machines' fingerprints match and skips with a message when they do not. The
reference machine is at least 16 cores with instance-store NVMe that the kernel
classifies as non-rotational. `make bench-full` reproduces the whole suite in
one command; `docs/BENCHMARKS.md` has the procedure and the requirements.

### Throughput

Pending measurement on reference hardware.

| query | parallelism | records/sec | scaling vs p=1 |
| --- | --- | --- | --- |
| identity | 1, 2, 4, 8, 16 | | |
| q0 | 1, 2, 4, 8, 16 | | |
| q1 | 1, 2, 4, 8, 16 | | |
| q2 | 1, 2, 4, 8, 16 | | |
| q5 | 1, 2, 4, 8, 16 | | |
| q7 | 1, 2, 4, 8, 16 | | |

The development container this was built in reports four cores, and a
three-vertex job at parallelism 1 already runs three goroutines, so its ceiling
is 4/3 and the 1.32x it measures is at that ceiling. Its run-to-run spread is
also 25 percent against a 15 percent regression threshold. Neither a scaling
curve nor a baseline can come from there.

### Recovery

Pending measurement on reference hardware. The measurement is the time to
**resume output**: from the call that restores a checkpoint to the first record
written to a sink.

| snapshot | entries | resume to first record |
| --- | --- | --- |
| 1 MiB | | |
| 10 MiB | | |
| 100 MiB | | |
| 1 GiB | | |

Recovery is I/O bound on the checkpoint read, and every block device in the
development container reports rotational, so every recovery report produced
there marks itself provisional. The mark is derived from the machine
fingerprint rather than set by a flag.

### State size

Measured, and valid from any machine: peak entry counts and checkpoint byte
counts are functions of the workload and the encoding, not of the hardware. The
same configuration produces the same numbers anywhere it fits in memory, which
is why this table is filled in and the two above are not.

q7, two million events, five-second tumbling windows, one millisecond of event
time per element:

| parallelism | auctions | peak entries | checkpoint state |
| --- | --- | --- | --- |
| 1 | 1,000 | 5,965 | 79.47 KiB |
| 2 | 1,000 | 397,846 | 15.56 MiB |
| 4 | 1,000 | 600,210 | 23.30 MiB |
| 1 | 100,000 | 26,621 | 344.53 KiB |
| 2 | 100,000 | 1,824,492 | 70.63 MiB |
| 4 | 100,000 | 2,723,906 | 105.80 MiB |

State is 41.0 bytes per entry for q7 and 33.0 for q5, at every cardinality and
every parallelism.

The hundredfold step between parallelism 1 and 4 is the engine and not the
workload. Source subtasks split the offset space into contiguous ranges and
event time increases with offset, so subtask 0 holds the input gate's watermark
minimum down until it exhausts and every later subtask's windows accumulate
unpurged. Any state size quoted from this engine has to be quoted with its
parallelism. The full table, the saturation behaviour of the cardinality dial,
and what configuration reaches a given state size are in `docs/BENCHMARKS.md`.
