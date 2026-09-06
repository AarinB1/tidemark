package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync/atomic"
	"time"

	"github.com/AarinB1/tidemark/pkg/checkpoint"
	"github.com/AarinB1/tidemark/pkg/core"
	tmruntime "github.com/AarinB1/tidemark/pkg/runtime"
)

// The recovery harness: how long a job takes to RESUME OUTPUT after being
// restored from a checkpoint.
//
// # What the clock covers, and why it is phrased this way
//
// It starts at the call that restores and stops when the first record reaches a
// sink. "Resuming output" rather than "loading a snapshot", because the second
// invites the question of whether a database was opened or state was rebuilt,
// and answers neither. The first is unambiguous about what the interval
// contains: reading the checkpoint, restoring every subtask's keyed state,
// seeking every source to its resume offset, starting every goroutine, and
// running far enough to produce a record.
//
// It excludes Go process startup and flag parsing, which are a few milliseconds
// and do not scale with the checkpoint. The report says so in its Measures
// field rather than leaving a reader to assume either way.
//
// # Why the first run is not killed
//
// A crash leaves a set of complete checkpoints on disk. A run that finishes
// leaves the same set: a checkpoint is durable at its _COMPLETE marker and
// nothing about the run's later fate changes it. What a fault injector would
// add is nondeterminism about WHICH checkpoint is last, and the restore path
// does not care. So the first run is allowed to finish and the second restores
// from the highest complete checkpoint it left, which is the largest one and
// the one worth timing.
//
// # Why this machine cannot publish these numbers
//
// Every block device here reports rotational, and a restore is dominated by
// reading a checkpoint back. The report marks itself provisional for exactly
// that reason and carries the fingerprint, so a number from here can never be
// mistaken for one from the reference machine.

// RecoveryResult is one point on the curve.
type RecoveryResult struct {
	Config
	// CheckpointID is the checkpoint the second run restored from: the highest
	// complete one, which is also the largest.
	CheckpointID int64 `json:"checkpoint_id"`
	// SnapshotBytes is that checkpoint's serialised operator state, and
	// SnapshotEntries what it holds. The row is a point on a curve against
	// these, which is why they are here rather than only in the state report.
	SnapshotBytes   int64 `json:"snapshot_bytes"`
	SnapshotEntries int64 `json:"snapshot_entries"`
	DiskBytes       int64 `json:"disk_bytes"`
	// ResumeMillis is the measurement: restore called, to the first record at a
	// sink.
	ResumeMillis int64 `json:"resume_millis"`
	// RunMillis is the whole restored run, for context. A resume longer than
	// the run it belongs to is arithmetic that did not happen.
	RunMillis      int64 `json:"run_millis"`
	Oversubscribed bool  `json:"oversubscribed"`
}

// RecoveryReport is what the recovery sweep writes.
type RecoveryReport struct {
	Fingerprint Fingerprint `json:"fingerprint"`
	// Provisional marks timings that must not be published.
	//
	// True unless the device backing the checkpoints is classified solid state.
	// A restore reads a checkpoint back, so a rotational device -- or one the
	// kernel will not classify, which is every container overlay -- puts the
	// number on a different order of magnitude from the hardware anybody would
	// run this on. It is derived from the fingerprint rather than set by a flag
	// so that nobody can publish by forgetting.
	Provisional bool `json:"provisional"`
	// Measures says what the clock covered, in the file, because a latency
	// whose interval is not stated is a number two readers will read two ways.
	Measures string           `json:"measures"`
	Results  []RecoveryResult `json:"results"`
}

// measuresRecovery is the one-line description of the interval.
const measuresRecovery = "from the call that restores from a checkpoint to the first record written to a sink; " +
	"includes reading the checkpoint, restoring every subtask's keyed state, seeking every source to its " +
	"resume offset and running to the first output; excludes Go process startup and flag parsing"

// measureRecovery runs a configuration to completion with checkpointing on,
// then restores from what it left and times the resume.
//
// root is a directory this function owns and removes.
func measureRecovery(cfg Config, checkpointsPerSubtask int, root string) (RecoveryResult, error) {
	if checkpointsPerSubtask < 1 {
		return RecoveryResult{}, fmt.Errorf("checkpoints per subtask is %d, must be >= 1", checkpointsPerSubtask)
	}
	interval := barrierIntervalFor(cfg, checkpointsPerSubtask)

	// The run that produces the checkpoint. Discard, because nothing about its
	// output is measured.
	first, err := buildGraph(cfg, interval)
	if err != nil {
		return RecoveryResult{}, err
	}
	if err := tmruntime.RunWithOptions(context.Background(), first, tmruntime.Options{
		CheckpointRoot: root,
		Seed:           cfg.Seed,
	}); err != nil {
		return RecoveryResult{}, fmt.Errorf("%s: the run that writes the checkpoint: %w", cfg.label(), err)
	}

	res := RecoveryResult{Config: cfg, Oversubscribed: cfg.oversubscribed()}
	operators, err := operatorSubtasks(first)
	if err != nil {
		return RecoveryResult{}, err
	}
	id, ok, err := checkpoint.NewStorage(root).Latest()
	if err != nil {
		return RecoveryResult{}, fmt.Errorf("%s: %w", cfg.label(), err)
	}
	if !ok {
		return RecoveryResult{}, fmt.Errorf("%s: the run wrote no complete checkpoint, so there is "+
			"nothing to restore from", cfg.label())
	}
	sizes, err := scanCheckpoints(root, operators)
	if err != nil {
		return RecoveryResult{}, fmt.Errorf("%s: %w", cfg.label(), err)
	}
	for _, size := range sizes {
		if size.id == id {
			res.CheckpointID = size.id
			res.SnapshotBytes = size.stateBytes
			res.SnapshotEntries = size.entries
			res.DiskBytes = size.diskBytes
		}
	}

	// The restored run. It takes NO checkpoints of its own: writing them back
	// into the same root during the interval being timed would put checkpoint
	// writes inside a measurement of checkpoint reads.
	clock := &firstRecordClock{}
	second, err := buildGraphWithSink(cfg, interval, clock.newSink)
	if err != nil {
		return RecoveryResult{}, err
	}
	start := time.Now()
	clock.start(start)
	runErr := tmruntime.RunWithOptions(context.Background(), second, tmruntime.Options{
		RestoreFrom: root,
		Seed:        cfg.Seed,
	})
	elapsed := time.Since(start)
	if runErr != nil {
		return RecoveryResult{}, fmt.Errorf("%s: the restored run: %w", cfg.label(), runErr)
	}

	resume, ok := clock.resume()
	if !ok {
		return RecoveryResult{}, fmt.Errorf("%s: the restored run wrote no record, so there is no "+
			"resume to time: this configuration checkpointed at a position with nothing left to emit",
			cfg.label())
	}
	res.ResumeMillis = resume.Milliseconds()
	res.RunMillis = elapsed.Milliseconds()
	return res, nil
}

// firstRecordClock is when output resumed.
//
// One clock shared by every sink subtask, because the question is when the JOB
// resumed producing output and not when a particular subtask did. The winner is
// whichever subtask writes first, which is why the store is a compare-and-swap
// rather than a field per subtask.
type firstRecordClock struct {
	begin atomic.Int64
	first atomic.Int64
}

func (c *firstRecordClock) start(t time.Time) { c.begin.Store(t.UnixNano()) }

// observe records the first write. Called from every sink subtask's goroutine.
func (c *firstRecordClock) observe(t time.Time) {
	c.first.CompareAndSwap(0, t.UnixNano())
}

// resume is how long after the restore call the first record appeared.
func (c *firstRecordClock) resume() (time.Duration, bool) {
	first := c.first.Load()
	if first == 0 {
		return 0, false
	}
	return time.Duration(first - c.begin.Load()), true
}

// newSink is what graph.Vertex.NewSink is set to.
func (c *firstRecordClock) newSink() core.Sink { return &timingSink{clock: c} }

// timingSink is Discard that notices its first record.
//
// Every method is written out rather than embedding sinks.Discard. The runtime
// dispatches on all five, and an embedded value would satisfy the interface
// while leaving a reader to check which of them this type actually meant to
// change -- the same trap the source decorator rule in CLAUDE.md names.
//
// The clock is read once per record until it is set, and the record is
// otherwise thrown away: a recovery run measures when output resumed, not what
// the output was, and a sink that kept the records would put allocation into
// the interval.
type timingSink struct {
	clock *firstRecordClock
	seen  bool
}

var _ core.Sink = (*timingSink)(nil)

func (s *timingSink) Open(ctx core.Context) error { return nil }

func (s *timingSink) Write(rec *core.Record) error {
	if !s.seen {
		// time.Now on the first record only. Per record it would be a syscall
		// on the output path of the job being timed.
		s.seen = true
		s.clock.observe(time.Now())
	}
	return nil
}

func (s *timingSink) Snapshot(w io.Writer) error                        { return nil }
func (s *timingSink) NotifyCheckpointComplete(checkpointID int64) error { return nil }
func (s *timingSink) Close() error                                      { return nil }

// provisionalOn decides whether a machine's recovery timings may be published.
//
// Only a device the kernel classifies as solid state qualifies. Rotational is
// the obvious disqualification; "unknown" is disqualified too, and that is the
// case that matters in practice, because a container overlay has an anonymous
// device number and no rotational flag behind it. Treating unknown as
// publishable would make the default answer on exactly the machines that cannot
// produce the number.
//
// Derived from the fingerprint rather than set by a flag, so that publishing
// cannot happen by forgetting.
func provisionalOn(f Fingerprint) bool { return f.DiskRotational != diskSolidState }

// runRecoverySweep measures every configuration and prints the curve.
func runRecoverySweep(opts sweepOptions) (RecoveryReport, error) {
	configs, err := sweep(opts)
	if err != nil {
		return RecoveryReport{}, err
	}
	dir, err := os.Getwd()
	if err != nil {
		return RecoveryReport{}, fmt.Errorf("working directory: %w", err)
	}
	fingerprint := machineFingerprint(dir)
	report := RecoveryReport{
		Fingerprint: fingerprint,
		Provisional: provisionalOn(fingerprint),
		Measures:    measuresRecovery,
	}

	for _, cfg := range configs {
		if len(timerVertices(cfg.Query)) == 0 {
			// A query with no keyed state restores a set of source offsets
			// whatever the workload is, so its row would be a constant with a
			// snapshot size of zero on the axis the curve is drawn against.
			fmt.Printf("%s: skipped, this query holds no keyed state\n", cfg.label())
			continue
		}
		root, err := os.MkdirTemp(opts.stateDir, "tidemark-recovery-")
		if err != nil {
			return RecoveryReport{}, fmt.Errorf("checkpoint directory: %w", err)
		}
		res, err := measureRecovery(cfg, opts.checkpoints, root)
		if rmErr := os.RemoveAll(root); rmErr != nil && err == nil {
			err = rmErr
		}
		if err != nil {
			return RecoveryReport{}, err
		}
		report.Results = append(report.Results, res)
		fmt.Printf("%s: snapshot %s over %d entries, resumed output in %dms (run %dms)%s\n",
			res.label(), humanBytes(res.SnapshotBytes), res.SnapshotEntries,
			res.ResumeMillis, res.RunMillis, provisionalNote(report))
	}
	return report, nil
}

func provisionalNote(r RecoveryReport) string {
	if !r.Provisional {
		return ""
	}
	return " PROVISIONAL"
}

// printRecoveryTable prints one row per snapshot size, which is what makes the
// output a curve rather than a point.
func printRecoveryTable(r RecoveryReport) {
	if len(r.Results) == 0 {
		return
	}
	fmt.Printf("\nrecovery (%s)\n", r.Fingerprint)
	if r.Provisional {
		fmt.Printf("PROVISIONAL: the device backing these checkpoints reports rotational=%s. "+
			"A restore is dominated by reading the checkpoint back, so these timings do not belong "+
			"in any results table.\n", r.Fingerprint.DiskRotational)
	}
	fmt.Printf("measures: %s\n", r.Measures)
	fmt.Printf("%-6s %5s %10s %12s %14s %14s\n", "query", "p", "auctions", "snapshot", "entries", "resume (ms)")
	for _, res := range r.Results {
		fmt.Printf("%-6s %5d %10d %12s %14d %14d\n",
			res.Query, res.Parallelism, res.AuctionCardinality,
			humanBytes(res.SnapshotBytes), res.SnapshotEntries, res.ResumeMillis)
	}
}
