package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestFirstRecordClock pins what the measurement is: the moment the JOB resumed
// output, which is the first write by any sink subtask.
func TestFirstRecordClock(t *testing.T) {
	t.Run("nothing written is not a measurement of zero", func(t *testing.T) {
		clock := &firstRecordClock{}
		clock.start(time.Now())
		if _, ok := clock.resume(); ok {
			t.Error("a run that wrote no record reported a resume time")
		}
	})

	t.Run("the earliest write wins", func(t *testing.T) {
		begin := time.Unix(0, 1_000_000_000)
		clock := &firstRecordClock{}
		clock.start(begin)
		clock.observe(begin.Add(50 * time.Millisecond))
		// A later subtask's first write must not overwrite it: the job resumed
		// output at the first record, not at the last subtask to catch up.
		clock.observe(begin.Add(400 * time.Millisecond))

		got, ok := clock.resume()
		if !ok {
			t.Fatal("resume reported nothing after a write")
		}
		if got != 50*time.Millisecond {
			t.Errorf("resume = %v, want 50ms", got)
		}
	})
}

// TestTimingSinkObservesOnlyItsFirstRecord: time.Now on every record would put
// a syscall on the output path of the job being timed, and the second record is
// not the resume.
func TestTimingSinkObservesOnlyItsFirstRecord(t *testing.T) {
	begin := time.Unix(0, 1_000_000_000)
	clock := &firstRecordClock{}
	clock.start(begin)
	sink := clock.newSink()

	for range 3 {
		if err := sink.Write(nil); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	first := clock.first.Load()
	if first == 0 {
		t.Fatal("the sink wrote three records and the clock saw none")
	}
	if err := sink.Write(nil); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if clock.first.Load() != first {
		t.Error("a later record moved the resume time")
	}
}

// TestCheckpointParent pins that an empty --state-dir is the system temp
// directory, the same contract as os.MkdirTemp. Fingerprinting Getwd instead
// is how a tmpfs restore gets published as an NVMe number.
func TestCheckpointParent(t *testing.T) {
	if got := checkpointParent("/var/lib/tidemark"); got != "/var/lib/tidemark" {
		t.Errorf("checkpointParent(%q) = %q", "/var/lib/tidemark", got)
	}
	if got := checkpointParent(""); got != os.TempDir() {
		t.Errorf("empty --state-dir must be TempDir %q, got %q", os.TempDir(), got)
	}
}

func TestProvisionalOn(t *testing.T) {
	tests := []struct {
		name string
		disk string
		want bool
	}{
		{
			name: "solid state can be published",
			disk: diskSolidState,
			want: false,
		},
		{
			// Every real block device in the container this was built in
			// reports rotational, which is why recovery latency is not
			// measurable here.
			name: "rotational cannot",
			disk: diskRotational,
			want: true,
		},
		{
			// The case that decides the default. A container overlay has an
			// anonymous device number and no rotational flag behind it, so
			// treating unknown as publishable would publish from exactly the
			// machines that cannot produce the number.
			name: "unknown cannot",
			disk: diskUnknown,
			want: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := referenceFingerprint()
			f.DiskRotational = tc.disk
			if got := provisionalOn(f); got != tc.want {
				t.Errorf("provisionalOn(disk=%q) = %v, want %v", tc.disk, got, tc.want)
			}
		})
	}
}

// recoveryTestConfig is small enough for a test and still leaves a checkpoint
// with state in it to restore from.
func recoveryTestConfig(p int) Config {
	cfg := benchConfig(queryQ7, p)
	cfg.Records = 40000
	cfg.WindowMillis = 1000
	cfg.AuctionCardinality = 200
	return cfg
}

// TestMeasureRecoveryIsInternallyConsistent checks the measurement against
// itself. The timings are not assertable on any machine -- that is the whole
// premise of this phase -- but the relationships between them are.
func TestMeasureRecoveryIsInternallyConsistent(t *testing.T) {
	root := filepath.Join(t.TempDir(), "checkpoints")
	res, err := measureRecovery(recoveryTestConfig(2), 8, root)
	if err != nil {
		t.Fatalf("measureRecovery: %v", err)
	}

	if res.CheckpointID <= 0 {
		t.Errorf("restored from checkpoint %d", res.CheckpointID)
	}
	if res.SnapshotBytes <= 0 || res.SnapshotEntries <= 0 {
		t.Errorf("restored from an empty snapshot: %d bytes over %d entries",
			res.SnapshotBytes, res.SnapshotEntries)
	}
	if res.DiskBytes < res.SnapshotBytes {
		t.Errorf("the checkpoint directory is %d bytes and the state in it is %d",
			res.DiskBytes, res.SnapshotBytes)
	}
	if res.ResumeMillis < 0 {
		t.Errorf("resume is %dms", res.ResumeMillis)
	}
	// The resume is a prefix of the restored run, so it cannot be longer than
	// it. Both are truncated to milliseconds, so they may be equal.
	if res.ResumeMillis > res.RunMillis {
		t.Errorf("output resumed after %dms in a run that lasted %dms", res.ResumeMillis, res.RunMillis)
	}
}

// TestRecoveryCurveHasARowPerSnapshotSize is what makes the output a curve: two
// cardinalities, two snapshot sizes, two rows, and the larger workload's
// snapshot is the larger one.
func TestRecoveryCurveHasARowPerSnapshotSize(t *testing.T) {
	report, err := runRecoverySweep(sweepOptions{
		mode: modeRecovery, records: 40000, parallelism: "2", query: "q7",
		seed: 1, keys: 100, auctions: "50,500", window: 1000, slide: 500,
		checkpoints: 8, stateDir: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("runRecoverySweep: %v", err)
	}
	if len(report.Results) != 2 {
		t.Fatalf("expected a row per configuration, got %d", len(report.Results))
	}
	small, large := report.Results[0], report.Results[1]
	if small.AuctionCardinality != 50 || large.AuctionCardinality != 500 {
		t.Fatalf("rows are %d and %d auctions, want 50 and 500",
			small.AuctionCardinality, large.AuctionCardinality)
	}
	if large.SnapshotBytes <= small.SnapshotBytes {
		t.Errorf("500 auctions snapshotted %d bytes and 50 auctions %d",
			large.SnapshotBytes, small.SnapshotBytes)
	}
	if report.Measures == "" {
		t.Error("the report does not say what its clock covered")
	}
}

// TestRecoveryReportIsProvisionalOnThisMachine: the label is derived from the
// machine rather than set by a flag, so it cannot be forgotten. On a machine
// with a solid-state device it is absent, which is the point of deriving it.
func TestRecoveryReportIsProvisionalOnThisMachine(t *testing.T) {
	dir := t.TempDir()
	report, err := runRecoverySweep(sweepOptions{
		mode: modeRecovery, records: 20000, parallelism: "1", query: "q7",
		seed: 1, keys: 100, auctions: "100", window: 1000, slide: 500,
		checkpoints: 4, stateDir: dir,
	})
	if err != nil {
		t.Fatalf("runRecoverySweep: %v", err)
	}
	if want := provisionalOn(report.Fingerprint); report.Provisional != want {
		t.Errorf("provisional = %v on a machine reporting rotational=%q, want %v",
			report.Provisional, report.Fingerprint.DiskRotational, want)
	}
	left, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read %s: %v", dir, err)
	}
	if len(left) != 0 {
		t.Errorf("the sweep left %d entries under %s", len(left), dir)
	}
}

// TestRecoverySweepSkipsStatelessQueries: a query with no keyed state restores
// a set of source offsets whatever the workload is, so its row would sit at
// zero on the axis the curve is drawn against.
func TestRecoverySweepSkipsStatelessQueries(t *testing.T) {
	report, err := runRecoverySweep(sweepOptions{
		mode: modeRecovery, records: 5000, parallelism: "1", query: "q0,q2",
		seed: 1, keys: 100, auctions: "100", window: 1000, slide: 500,
		checkpoints: 4, stateDir: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("runRecoverySweep: %v", err)
	}
	if len(report.Results) != 0 {
		t.Errorf("the stateless queries produced %d rows", len(report.Results))
	}
}

func TestSuffixedJSONPath(t *testing.T) {
	tests := []struct {
		in, suffix, want string
	}{
		{in: "bench.json", suffix: "recovery", want: "bench.recovery.json"},
		{in: "out/bench.json", suffix: "state", want: "out/bench.state.json"},
		{in: "bench", suffix: "recovery", want: "bench.recovery"},
	}
	for _, tc := range tests {
		t.Run(tc.want, func(t *testing.T) {
			if got := suffixedJSONPath(tc.in, tc.suffix); got != tc.want {
				t.Errorf("suffixedJSONPath(%q, %q) = %q, want %q", tc.in, tc.suffix, got, tc.want)
			}
		})
	}
}
