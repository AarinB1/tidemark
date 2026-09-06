# Benchmarks

How tidemark is measured, what a published number requires, and which numbers
exist today.

## Reproducing the suite

```
git clone https://github.com/AarinB1/tidemark
cd tidemark
make bench-full
```

That writes three files beside each other:

| file | what it holds |
| --- | --- |
| `bench.json` | throughput: every query across the parallelism curve |
| `bench.state.json` | state size: peak entries and checkpoint bytes across the auction-cardinality axis |
| `bench.recovery.json` | recovery: time to resume output, one row per snapshot size |

Every file carries the machine fingerprint of the run that produced it. The
sweeps are configurable through the `BENCH_*` variables at the top of the
`bench-full` target; the defaults are what the reference machine runs.

Never run any of this under `-race`. The detector costs five to twenty times and
the number it produces means nothing.

## The reference machine

Every published number comes from one machine. That is not a preference, it is
enforced: `make bench-check` compares a run against `test/bench/baseline.json`
only when the two machines' fingerprints match, and skips with a message
naming both when they do not.

Requirements:

- **At least 16 physical cores.** The default sweep goes to parallelism 16, and
  a configuration whose parallelism exceeds the core count is flagged
  `oversubscribed` in the JSON so it cannot enter the record as a measurement of
  scaling. Note that a job runs one goroutine per *subtask*: a three-vertex
  chain at parallelism p is 3p runnable goroutines, so 16 cores is the point at
  which the flag stops firing rather than the point at which contention stops.
- **NVMe, and the kernel must classify it as non-rotational.** A recovery
  measurement is dominated by reading a checkpoint back. The recovery report
  marks itself provisional on any device that reports rotational or that the
  kernel will not classify, and that mark is derived from the fingerprint rather
  than set by a flag, so it cannot be forgotten. Instance-store NVMe on a
  c7i.4xlarge or equivalent is what this was sized for.
- **A quiet machine.** See the note on variance below.

The fingerprint is the CPU model, the core count, total memory, the rotational
flag of the device backing the working directory, the Go version and GOMAXPROCS.
All of them are compared; memory is compared to within a gibibyte, because
`MemTotal` drifts by pages across kernels and a machine has to keep matching
itself. **A Go toolchain bump invalidates the baseline** and the check will skip
until it is regenerated on the reference machine. That is deliberate: a compiler
change that moves throughput is indistinguishable from a regression.

### Generating the canonical baseline

1. `make bench-full` on the reference machine.
2. Copy `bench.json` back.
3. Add a `note` field saying which machine it came from and when.
4. Commit it as `test/bench/baseline.json`.

Do it once. From then on that machine is canonical, and `make bench-check` is a
real check there and an explicit skip everywhere else.

## What this repository can and cannot measure today

`test/bench/baseline.json` currently holds four Phase 6a numbers and **no
fingerprint**, which is the honest state: no fingerprint was recorded when they
were measured, so no machine can claim them, and a fingerprint-less baseline
matches nothing. Every `make bench-check` therefore skips with a message. The
canonical baseline replaces the file wholesale.

The development container this phase was built in reports four cores and every
real block device reports `ROTA=1`. Two consequences, both structural:

- **Throughput is not measurable there.** Three vertices run at parallelism 1,
  so the ceiling is 4/3 and the existing 1.32x measurement is already at it. Nor
  is the machine stable enough to be a tripwire: four consecutive two-million
  record runs at parallelism 1 spanned 445k to 555k rec/s, a 25 percent spread
  against a 15 percent regression threshold.
- **Recovery latency is not measurable there.** It is I/O bound on disks the
  kernel classifies as rotational, so every recovery report from that machine is
  marked provisional.

**State size is measurable there, and is measured.** Peak entry counts and
checkpoint byte counts are functions of the workload and the encoding, not of
the machine: the same configuration produces the same numbers anywhere it fits
in memory. That is why the table below is populated and the throughput and
recovery tables in the README are not.

## State size

Measured on q7, two million events, five-second tumbling windows, event time
stepping one millisecond per element, eight checkpoints per source subtask.
Peak entries is the greatest number of `KeyedState` entries the operator
subtasks held between them at one instant; checkpoint state is the largest
complete checkpoint's serialised operator state.

| parallelism | auctions | peak entries | checkpoint entries | checkpoint state | bytes/entry |
| --- | --- | --- | --- | --- | --- |
| 1 | 100 | 601 | 201 | 8.04 KiB | 41.0 |
| 2 | 100 | 40,778 | 40,202 | 1.57 MiB | 41.0 |
| 4 | 100 | 60,204 | 60,204 | 2.35 MiB | 41.0 |
| 1 | 1,000 | 5,965 | 1,985 | 79.47 KiB | 41.0 |
| 2 | 1,000 | 397,846 | 397,846 | 15.56 MiB | 41.0 |
| 4 | 1,000 | 600,210 | 595,820 | 23.30 MiB | 41.0 |
| 1 | 10,000 | 22,007 | 7,135 | 285.67 KiB | 41.0 |
| 2 | 10,000 | 1,480,936 | 1,480,936 | 57.91 MiB | 41.0 |
| 4 | 10,000 | 2,233,184 | 2,218,190 | 86.73 MiB | 41.0 |
| 1 | 100,000 | 26,621 | 8,605 | 344.53 KiB | 41.0 |
| 2 | 100,000 | 1,824,492 | 1,806,470 | 70.63 MiB | 41.0 |
| 4 | 100,000 | 2,723,906 | 2,705,870 | 105.80 MiB | 41.0 |

q5 over the same stream, at 1,000 auctions with a 1,250ms slide, for contrast:
22,852 peak entries at parallelism 1, 1,612,896 at 2, and 2,413,709 at 4, at
33.0 bytes per entry — four times q7's state per record, because each bid joins
four overlapping windows and the query has two windowed stages.

### Bytes per entry

41.0 for q7 and 33.0 for q5, identical at every cardinality and every
parallelism, which is what fixed-width keys and values predict and is the check
that the entries counted and the bytes measured came from the same thing.

q7 writes two entries per live `(auction, window)` pair. The aggregate is a
17-byte key (a prefix byte, an eight-byte auction id, an eight-byte window
start) with a 24-byte value, and the timer is a 25-byte key with an empty value;
each half of each entry carries a four-byte length prefix, giving 49 and 33
bytes and an average of 41. q5's aggregate value is an eight-byte count, so both
of its entry kinds are 33 bytes and the average has no spread at all.

### Why parallelism is an axis

Source subtasks split the offset space into contiguous ranges, and the Nexmark
source's event time increases with offset. Subtask 0 therefore covers the
earliest event times, and an input gate's watermark is the minimum across its
inputs, so no window can fire while subtask 0 is still emitting. Every later
subtask's windows accumulate unpurged until it exhausts, which happens near the
end of the run.

At parallelism 1 there is one range, event time advances monotonically, and a
window fires and purges as the watermark passes it. The result is the hundredfold
step in the table above: 5,965 peak entries at parallelism 1 against 600,210 at
parallelism 4, same workload, same cardinality.

Checkpoint size in this engine is therefore a function of parallelism in a way
it would not be in a system whose partitions overlap in event time. Any state
size quoted from this engine has to be quoted with its parallelism.

### The cardinality dial saturates

Raising `AuctionCardinality` stops buying state once the id space is large
against the number of bids a window holds. At five-second windows and one
millisecond per element a window holds 4,600 bids, so state at parallelism 4
grows 3.7x from 1,000 to 10,000 auctions and only 1.2x from 10,000 to 100,000.

At the top of that curve nearly every bid opens its own `(auction, window)` pair
and the maximum has nothing to compare against: the dial has stopped measuring
an aggregation. The ceiling at a fixed record count is
`entries <= 1.38 x records`, or about 57 bytes of state per event, which no
cardinality can exceed.

### Projecting a target state size

The measured points fit `entries = 2 x liveWindows x distinctAuctionsPerWindow`
to within 1.5 percent everywhere, where `liveWindows = (1 - 1/p) x records /
window` and `distinctAuctionsPerWindow = A(1 - e^(-B/A))` for `B = 0.92 x window`
bids per window.

Holding the shape that keeps q7 an aggregation — 1,000 auctions, five-second
windows, about 4.6 bids per pair — at parallelism 4 that is `0.297 x records`
entries, or 12.2 bytes of checkpoint per event:

| target | records | measured |
| --- | --- | --- |
| 100 MiB | 8.6M | 8.52M events produced 99.01 MiB |
| 500 MiB | 43.1M | projected |
| 1 GiB | 88.2M | projected |
| 1.2 GB | 98.5M | projected |

The 100 MiB row was run rather than projected, and it landed within one percent
of the model.

Two things follow. Reaching a gigabyte by raising cardinality alone is not
possible at a modest record count: at two million events the saturation ceiling
is about 113 MiB. Reaching it by raising the record count is ordinary — 1.2 GB
is roughly 100 million events, which is a standard Nexmark scale, at a
cardinality where each `(auction, window)` still holds about five bids. q5 gets
there on about 30 million events, because it holds four times the state per
event.

## Recovery

The harness runs a configuration to completion with checkpointing on, restores
from the highest complete checkpoint it left, and times from the restore call to
the first record written to a sink.

It measures **resuming output**, not loading a snapshot. The interval covers
reading the checkpoint, restoring every subtask's keyed state, seeking every
source to its resume offset, and running far enough to produce a record. It
excludes Go process startup and flag parsing, which are milliseconds and do not
scale with the checkpoint. Every report states that sentence in its `measures`
field, so a number cannot travel without its definition.

The first run is not killed. A crash leaves a set of complete checkpoints and a
run that finishes leaves the same set, because a checkpoint is durable at its
`_COMPLETE` marker; a fault injector would add nondeterminism about which
checkpoint is last without changing anything the restore path does.

Output is one row per snapshot size, so the result is a curve. No numbers are
published yet: see the reference machine requirements above.
