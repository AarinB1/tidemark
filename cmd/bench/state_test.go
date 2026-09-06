package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/AarinB1/tidemark/pkg/state"
)

func TestBarrierIntervalFor(t *testing.T) {
	tests := []struct {
		name        string
		records     int64
		parallelism int
		checkpoints int
		want        int64
	}{
		{
			// Per SUBTASK: at parallelism 4 each subtask covers a quarter of the
			// offset space, so a job-wide spacing would give each of them two
			// barriers and the last checkpoint would land a quarter of the way
			// through the range that matters.
			name:    "per subtask, not per job",
			records: 800000, parallelism: 4, checkpoints: 8, want: 25000,
		},
		{
			name:    "parallelism one",
			records: 800000, parallelism: 1, checkpoints: 8, want: 100000,
		},
		{
			// Fewer records than barriers asked for: one barrier per element is
			// the tightest the engine can be given, and zero is not a setting.
			name:    "floors at one",
			records: 4, parallelism: 2, checkpoints: 8, want: 1,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := benchConfig(queryQ7, tc.parallelism)
			cfg.Records = tc.records
			if got := barrierIntervalFor(cfg, tc.checkpoints); got != tc.want {
				t.Errorf("barrierIntervalFor = %d, want %d", got, tc.want)
			}
		})
	}
}

// TestCountingStateTracksTheLiveCount covers the four transitions separately,
// because the two that must NOT move the count are the ones a naive decorator
// gets wrong: a Put over an existing key is an update, and a Delete of an
// absent key is a no-op. Either one counted would make the peak a count of
// operations rather than of entries.
func TestCountingStateTracksTheLiveCount(t *testing.T) {
	meter := &stateMeter{}
	st, err := meter.newState()
	if err != nil {
		t.Fatalf("newState: %v", err)
	}

	steps := []struct {
		name        string
		do          func()
		wantCurrent int64
		wantPeak    int64
	}{
		{
			name:        "first put inserts",
			do:          func() { st.Put([]byte("a"), []byte("1")) },
			wantCurrent: 1, wantPeak: 1,
		},
		{
			name:        "second key inserts",
			do:          func() { st.Put([]byte("b"), []byte("1")) },
			wantCurrent: 2, wantPeak: 2,
		},
		{
			name:        "put over an existing key updates",
			do:          func() { st.Put([]byte("a"), []byte("2")) },
			wantCurrent: 2, wantPeak: 2,
		},
		{
			name:        "delete removes",
			do:          func() { st.Delete([]byte("a")) },
			wantCurrent: 1, wantPeak: 2,
		},
		{
			name:        "delete of an absent key is a no-op",
			do:          func() { st.Delete([]byte("zzz")) },
			wantCurrent: 1, wantPeak: 2,
		},
		{
			// The peak is what a purge cycle makes interesting: state falls and
			// rises again, and the number worth reporting is the high-water
			// mark rather than where it ended.
			name:        "re-inserting does not beat the old peak",
			do:          func() { st.Put([]byte("c"), []byte("1")) },
			wantCurrent: 2, wantPeak: 2,
		},
		{
			name:        "a new high-water mark",
			do:          func() { st.Put([]byte("d"), []byte("1")) },
			wantCurrent: 3, wantPeak: 3,
		},
	}
	for _, step := range steps {
		t.Run(step.name, func(t *testing.T) {
			step.do()
			if got := meter.current.Load(); got != step.wantCurrent {
				t.Errorf("current = %d, want %d", got, step.wantCurrent)
			}
			if got := meter.peak.Load(); got != step.wantPeak {
				t.Errorf("peak = %d, want %d", got, step.wantPeak)
			}
		})
	}
}

// TestCountingStateSharesOneMeterAcrossSubtasks: the peak is the entries the
// job held BETWEEN its subtasks at one instant, which is the quantity a
// checkpoint's total bytes is a function of. Per-subtask meters would report
// the largest subtask instead.
func TestCountingStateSharesOneMeterAcrossSubtasks(t *testing.T) {
	meter := &stateMeter{}
	first, _ := meter.newState()
	second, _ := meter.newState()

	first.Put([]byte("a"), []byte("1"))
	second.Put([]byte("a"), []byte("1"))
	if got := meter.peak.Load(); got != 2 {
		t.Errorf("peak = %d, want 2: two subtasks each holding one key hold two between them", got)
	}
	// The same key in two subtasks is two entries: state is per subtask, so
	// there is no deduplication to do.
	if got := meter.current.Load(); got != 2 {
		t.Errorf("current = %d, want 2", got)
	}
}

func TestEntriesIn(t *testing.T) {
	tests := []struct {
		name    string
		entries int
	}{
		{name: "empty", entries: 0},
		{name: "one", entries: 1},
		{name: "many", entries: 1000},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			src := state.NewMemory()
			for i := range tc.entries {
				src.Put([]byte{byte(i >> 8), byte(i)}, []byte("value"))
			}
			var buf bytes.Buffer
			if err := state.WriteTo(src, &buf); err != nil {
				t.Fatalf("WriteTo: %v", err)
			}
			got, err := entriesIn(buf.Bytes())
			if err != nil {
				t.Fatalf("entriesIn: %v", err)
			}
			if got != int64(tc.entries) {
				t.Errorf("entriesIn = %d, want %d", got, tc.entries)
			}
		})
	}
}

func TestEntriesInRefusesSomethingElse(t *testing.T) {
	// A source subtask's payload is a resume offset, not a serialised
	// KeyedState. Decoding one as the other has to fail rather than produce a
	// plausible entry count, which is why scanCheckpoints reads only the
	// subtasks it knows are operators.
	if _, err := entriesIn([]byte("not a serialised keyed state")); err == nil {
		t.Fatal("entriesIn accepted a payload that is not keyed state")
	}
}

func TestDirBytes(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "nested"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	files := map[string]int{
		"a":            10,
		"nested/b":     20,
		"nested/c.txt": 30,
	}
	for name, size := range files {
		if err := os.WriteFile(filepath.Join(dir, name), make([]byte, size), 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	got, err := dirBytes(dir)
	if err != nil {
		t.Fatalf("dirBytes: %v", err)
	}
	if got != 60 {
		t.Errorf("dirBytes = %d, want 60", got)
	}
}

// stateTestConfig is a q7 configuration small enough for a test and shaped so
// the parallelism effect is visible: sixty windows over the record range, so at
// parallelism 2 the later subtask's windows pile up and at parallelism 1 they
// do not.
func stateTestConfig(p int) Config {
	cfg := benchConfig(queryQ7, p)
	cfg.Records = 60000
	cfg.WindowMillis = 1000
	cfg.AuctionCardinality = 200
	return cfg
}

// TestMeasureStateIsInternallyConsistent checks the arithmetic of one
// measurement against itself: the parts have to add up to the totals, the
// checkpoint cannot hold more entries than the run ever held, and the directory
// cannot be smaller than the state inside it.
func TestMeasureStateIsInternallyConsistent(t *testing.T) {
	root := filepath.Join(t.TempDir(), "checkpoints")
	res, err := measureState(stateTestConfig(2), 8, root)
	if err != nil {
		t.Fatalf("measureState: %v", err)
	}

	if res.Checkpoints == 0 {
		t.Fatal("no complete checkpoint was written; nothing below is measuring anything")
	}
	if res.PeakEntries <= 0 || res.Entries <= 0 || res.StateBytes <= 0 {
		t.Fatalf("empty measurement: %+v", res)
	}
	if res.Entries > res.PeakEntries {
		t.Errorf("a checkpoint holds %d entries and the run peaked at %d: a snapshot cannot hold more "+
			"than was ever live", res.Entries, res.PeakEntries)
	}
	if res.DiskBytes < res.StateBytes {
		t.Errorf("the directory is %d bytes and the state in it is %d: the framing and the metadata "+
			"cannot be negative", res.DiskBytes, res.StateBytes)
	}

	var bytesSum, entriesSum int64
	for _, sub := range res.PerSubtask {
		if sub.VertexID != "q7" {
			t.Errorf("subtask %s[%d] is not an operator subtask of this job", sub.VertexID, sub.Index)
		}
		bytesSum += sub.Bytes
		entriesSum += sub.Entries
	}
	if bytesSum != res.StateBytes || entriesSum != res.Entries {
		t.Errorf("per-subtask rows sum to %d bytes and %d entries, totals say %d and %d",
			bytesSum, entriesSum, res.StateBytes, res.Entries)
	}

	// q7 writes a 17-byte key with a 24-byte value for an aggregate and a
	// 25-byte key with no value for a timer, plus four bytes of length prefix
	// on each half of each. The average therefore has to land between the two,
	// and a number outside that band means the entries counted and the bytes
	// measured came from different things.
	if res.BytesPerEntry < 30 || res.BytesPerEntry > 55 {
		t.Errorf("bytes per entry = %.1f, which is outside what q7's key and value widths allow",
			res.BytesPerEntry)
	}
}

// TestStateGrowsWithParallelism is the finding this step exists to produce.
//
// Source subtasks split the offset space into contiguous ranges and the Nexmark
// source's event time increases with offset, so subtask 0 holds the gate's
// watermark minimum down until it exhausts and every later subtask's windows
// accumulate unpurged. At parallelism 1 there is one range, the watermark
// advances monotonically, and windows fire and purge as they are passed.
//
// The assertion is deliberately loose -- a multiple of three, where the
// measured ratio is closer to ten -- because the exact factor depends on the
// window count and this test is pinning the MECHANISM, not the number. A
// regression that made the two the same size would mean either the gate stopped
// taking the minimum or the source stopped splitting contiguously, and both are
// silent failures.
func TestStateGrowsWithParallelism(t *testing.T) {
	one, err := measureState(stateTestConfig(1), 8, filepath.Join(t.TempDir(), "p1"))
	if err != nil {
		t.Fatalf("measureState at parallelism 1: %v", err)
	}
	two, err := measureState(stateTestConfig(2), 8, filepath.Join(t.TempDir(), "p2"))
	if err != nil {
		t.Fatalf("measureState at parallelism 2: %v", err)
	}

	if two.PeakEntries < 3*one.PeakEntries {
		t.Errorf("peak state is %d entries at parallelism 1 and %d at parallelism 2; the contiguous "+
			"source ranges and the gate's minimum should make the second several times the first",
			one.PeakEntries, two.PeakEntries)
	}
	if two.StateBytes <= one.StateBytes {
		t.Errorf("checkpoint state is %d bytes at parallelism 1 and %d at parallelism 2", one.StateBytes, two.StateBytes)
	}
	// Bytes per entry is a property of the ENCODING, not of the parallelism, so
	// the two runs must agree on it. If they do not, one of the two counted
	// something the other did not.
	if diff := one.BytesPerEntry - two.BytesPerEntry; diff > 1 || diff < -1 {
		t.Errorf("bytes per entry is %.1f at parallelism 1 and %.1f at parallelism 2: the encoding "+
			"does not depend on parallelism", one.BytesPerEntry, two.BytesPerEntry)
	}
}

// TestStateSweepSkipsStatelessQueries: q0, q1 and q2 hold no keyed state, so a
// row for them would be a row of zeroes in a table about state size, and a
// reader would take it for a measurement.
func TestStateSweepSkipsStatelessQueries(t *testing.T) {
	report, err := runStateSweep(sweepOptions{
		mode: modeState, records: 5000, parallelism: "1", query: "q0,q1,q2",
		seed: 1, keys: 100, auctions: "100", window: 1000, slide: 500,
		checkpoints: 4, stateDir: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("runStateSweep: %v", err)
	}
	if len(report.Results) != 0 {
		t.Errorf("the stateless queries produced %d rows: %+v", len(report.Results), report.Results)
	}
}

// TestStateSweepRemovesItsCheckpoints: a sweep across a dozen configurations
// writes every checkpoint of every one of them, and the largest configurations
// are the point. Leaving them would need the disk of the whole sweep at once.
func TestStateSweepRemovesItsCheckpoints(t *testing.T) {
	dir := t.TempDir()
	report, err := runStateSweep(sweepOptions{
		mode: modeState, records: 20000, parallelism: "1", query: "q7",
		seed: 1, keys: 100, auctions: "100", window: 1000, slide: 500,
		checkpoints: 4, stateDir: dir,
	})
	if err != nil {
		t.Fatalf("runStateSweep: %v", err)
	}
	if len(report.Results) != 1 {
		t.Fatalf("expected one row, got %d", len(report.Results))
	}
	if report.Provisional {
		t.Error("the state report is marked provisional; state size is memory-bound and reproducible")
	}
	left, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read %s: %v", dir, err)
	}
	if len(left) != 0 {
		t.Errorf("the sweep left %d entries under %s", len(left), dir)
	}
}

func TestStateJSONPath(t *testing.T) {
	tests := []struct{ in, want string }{
		{in: "bench.json", want: "bench.state.json"},
		{in: "out/bench.json", want: "out/bench.state.json"},
		{in: "bench", want: "bench.state"},
	}
	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			if got := stateJSONPath(tc.in); got != tc.want {
				t.Errorf("stateJSONPath(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestRunRefusesAnUnknownMode(t *testing.T) {
	err := run(sweepOptions{mode: "latency", records: 10, parallelism: "1", query: "q7", auctions: "10"})
	if err == nil {
		t.Fatal("run accepted an unknown mode")
	}
}

func TestHumanBytes(t *testing.T) {
	tests := []struct {
		in   int64
		want string
	}{
		{in: 0, want: "0 B"},
		{in: 999, want: "999 B"},
		{in: 1 << 10, want: "1.00 KiB"},
		{in: 1 << 20, want: "1.00 MiB"},
		{in: 3 << 29, want: "1.50 GiB"},
	}
	for _, tc := range tests {
		t.Run(tc.want, func(t *testing.T) {
			if got := humanBytes(tc.in); got != tc.want {
				t.Errorf("humanBytes(%d) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}
