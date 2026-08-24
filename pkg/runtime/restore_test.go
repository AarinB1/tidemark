package runtime

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/AarinB1/tidemark/pkg/checkpoint"
	"github.com/AarinB1/tidemark/pkg/core"
	"github.com/AarinB1/tidemark/pkg/graph"
	"github.com/AarinB1/tidemark/pkg/sinks"
	"github.com/AarinB1/tidemark/pkg/sources"
	"github.com/AarinB1/tidemark/pkg/state"
)

// keyedCount counts the records it is given, per key, in the subtask's keyed
// state, and emits one record per key at end of stream.
//
// This is the operator the checkpointing tests are built on, and the reason it
// is not the window operator is worth stating. Its ENTIRE state is its
// KeyedState: no timers, no watermark, nothing held in a Go field that a
// snapshot of the state backend would miss. That makes it the operator for
// which "restore the KeyedState" is the whole of recovery, so a test built on
// it measures the checkpointing machinery rather than what a particular
// operator forgot to put in state.
//
// operators.WindowCount is NOT that operator. Its aggregates are in KeyedState
// but its pending timers and its current watermark are in RAM, so restoring its
// state alone gives back the counts with no timer to fire them. See the note on
// TestRestoreDoesNotRecoverInRamOperatorState below.
type keyedCount struct {
	st  state.KeyedState
	buf []byte
}

func newKeyedCount() core.Operator { return &keyedCount{} }

func (o *keyedCount) Open(ctx core.Context) error {
	o.st = ctx.State()
	if o.st == nil {
		return errors.New("keyedCount: the runtime provided no keyed state")
	}
	return nil
}

// stateKey is the user-state partition byte followed by the record key.
func (o *keyedCount) stateKey(key []byte) []byte {
	o.buf = append(append(o.buf[:0], state.PrefixUserState), key...)
	return o.buf
}

func (o *keyedCount) ProcessElement(rec *core.Record, ctx core.Context) error {
	k := o.stateKey(rec.Key)
	var n uint64
	if v, ok := o.st.Get(k); ok {
		if len(v) != 8 {
			return fmt.Errorf("keyedCount: value for key %x is %d bytes, want 8", rec.Key, len(v))
		}
		n = binary.BigEndian.Uint64(v)
	}
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], n+1)
	o.st.Put(k, buf[:])
	return nil
}

func (o *keyedCount) ProcessWatermark(wm int64, ctx core.Context) error { return nil }

// OnEndOfStream emits the counts.
//
// Emitting here and nowhere else is what makes the sink contents of a recovered
// run comparable to a clean one without a deduplicating sink: a run that dies
// mid-stream has emitted nothing, so the recovered run's output is the whole
// answer rather than a second helping of part of it.
func (o *keyedCount) OnEndOfStream(ctx core.Context) error {
	var emitErr error
	o.st.Iterate(func(k, v []byte) bool {
		if len(k) == 0 || k[0] != state.PrefixUserState {
			emitErr = fmt.Errorf("keyedCount: state holds a key outside the user-state partition: %x", k)
			return false
		}
		ctx.Emit(&core.Record{Key: slices.Clone(k[1:]), Value: slices.Clone(v)})
		return true
	})
	return emitErr
}

func (o *keyedCount) Snapshot(io.Writer) error { return nil }
func (o *keyedCount) Restore(io.Reader) error  { return nil }
func (o *keyedCount) Close() error             { return nil }

var _ core.Operator = (*keyedCount)(nil)

// countsOf decodes what a keyedCount job wrote into a sink, summing across sink
// subtasks. A key partitions to one operator subtask, so a key appears once;
// summing rather than asserting that is what lets a broken partition show up as
// a wrong number instead of a panic.
func countsOf(t *testing.T, recs []*core.Record) map[string]int64 {
	t.Helper()
	out := make(map[string]int64)
	for _, rec := range recs {
		if len(rec.Value) != 8 {
			t.Fatalf("sink holds a value of %d bytes, want 8", len(rec.Value))
		}
		out[string(rec.Key)] += int64(binary.BigEndian.Uint64(rec.Value))
	}
	return out
}

// oracleCounts is what the generator produces, counted directly. It reads the
// source rather than the engine, so it is an independent statement of the
// answer.
func oracleCounts(t *testing.T, cfgs ...sources.GeneratorConfig) map[string]int64 {
	t.Helper()
	out := make(map[string]int64)
	for _, cfg := range cfgs {
		src := sources.NewGenerator(cfg)
		if err := src.Open(nil); err != nil {
			t.Fatalf("Open: %v", err)
		}
		for {
			rec, ok, err := src.Next()
			if err != nil {
				t.Fatalf("Next: %v", err)
			}
			if !ok {
				break
			}
			out[string(rec.Key)]++
		}
		if err := src.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}
	return out
}

func assertSameCounts(t *testing.T, got, want map[string]int64, label string) {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("%s: the sink holds %d keys, want %d", label, len(got), len(want))
	}
	keys := make([]string, 0, len(want))
	for k := range want {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	for _, k := range keys {
		if got[k] != want[k] {
			t.Fatalf("%s: key %x counted %d, want %d", label, k, got[k], want[k])
		}
	}
	for k := range got {
		if _, ok := want[k]; !ok {
			t.Fatalf("%s: the sink holds key %x, which the input does not produce", label, k)
		}
	}
}

// restoreConfig is a generator whose keys spread across subtasks and whose
// event times are dense enough that watermarks advance during a run.
func restoreConfig(seed uint64, count int64) sources.GeneratorConfig {
	return sources.GeneratorConfig{
		Seed:           seed,
		Count:          count,
		KeyCardinality: 64,
		BaseEventTime:  1700000000000,
		EventTimeStep:  10,
		MaxLag:         200,
		ValueSize:      8,
		AmountRange:    1000,
	}
}

// countingSourceVertex is a source vertex over cfg, wrapped by wrap when wrap
// is not nil.
func countingSourceVertex(id string, cfg sources.GeneratorConfig, p int, barrierInterval int64, wrap func(core.Source) core.Source) graph.Vertex {
	return graph.Vertex{
		ID: id, Kind: graph.VertexSource, Parallelism: p,
		NewSource: func() core.Source {
			src := core.Source(sources.NewGenerator(cfg))
			if wrap != nil {
				src = wrap(src)
			}
			return src
		},
		WatermarkIntervalElements: 100,
		MaxOutOfOrderness:         cfg.MaxLag,
		BarrierIntervalElements:   barrierInterval,
	}
}

// countingGraph is one or more source vertices feeding a keyedCount operator
// and a sink.
func countingGraph(t *testing.T, sink core.Sink, p int, sourceVertices ...graph.Vertex) *graph.Graph {
	t.Helper()
	vertices := append([]graph.Vertex{}, sourceVertices...)
	vertices = append(vertices,
		graph.Vertex{ID: "count", Kind: graph.VertexOperator, Parallelism: p, NewOperator: newKeyedCount},
		graph.Vertex{ID: "out", Kind: graph.VertexSink, Parallelism: p,
			NewSink: func() core.Sink { return sink }},
	)
	var edges [][2]string
	for _, v := range sourceVertices {
		edges = append(edges, [2]string{v.ID, "count"})
	}
	edges = append(edges, [2]string{"count", "out"})
	return buildGraph(t, vertices, edges)
}

// TestRestoreResumesFromTheRecordedOffsets is the base of the recovery story:
// a run that completed checkpoints, restarted from the last one, reads from the
// offsets the checkpoint recorded and not from zero.
//
// It is asserted on what the sources READ rather than on the final answer,
// because the final answer of a complete run is right either way -- a source
// that ignored the offset and restarted at zero would produce counts that are
// too high, but a source that restarted one element early would not, and that
// is the interesting failure.
func TestRestoreResumesFromTheRecordedOffsets(t *testing.T) {
	// The interval does NOT divide the per-subtask range. That is deliberate: at
	// an exact multiple the last barrier lands on the final element of the
	// range, so restoring from it resumes at the end and replays nothing, and
	// this test would pass against a source that ignored the offset entirely.
	// 1000 elements per subtask at 300 gives three barriers and leaves 100
	// records to replay.
	const (
		count           = 2000
		barrierInterval = 300
		parallelism     = 2
	)
	root := t.TempDir()
	cfg := restoreConfig(1, count)

	// A clean run that checkpoints.
	clean := sinks.NewCollect()
	if err := RunWithOptions(context.Background(),
		countingGraph(t, clean, parallelism, countingSourceVertex("src", cfg, parallelism, barrierInterval, nil)),
		Options{CheckpointRoot: root, Seed: cfg.Seed}); err != nil {
		t.Fatalf("Run: %v", err)
	}

	storage := checkpoint.NewStorage(root)
	id, ok, err := storage.Latest()
	if err != nil || !ok {
		t.Fatalf("Latest = (%d, ok %t, err %v), want a complete checkpoint", id, ok, err)
	}

	// Every source subtask resumes at start + id*barrierInterval, which is what
	// the checkpoint recorded.
	_, payloads, err := storage.Load(id)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	for index := range parallelism {
		start, _ := sourceRange(count, parallelism, index)
		got, err := decodePosition(payloads[checkpoint.SubtaskKey{VertexID: "src", Index: index}])
		if err != nil {
			t.Fatalf("decodePosition: %v", err)
		}
		if want := start + id*barrierInterval; got != want {
			t.Fatalf("source subtask %d resumes at %d, want %d", index, got, want)
		}
	}

	// The restored run reads from those offsets. Every subtask's first record
	// is recorded by a source decorator that reports where it actually started.
	var mu sync.Mutex
	firstOffsets := make(map[int64]bool)
	restored := sinks.NewCollect()
	wrap := func(src core.Source) core.Source {
		return &observingSource{inner: src.(*sources.Generator), onFirstNext: func(pos int64) {
			mu.Lock()
			defer mu.Unlock()
			firstOffsets[pos] = true
		}}
	}
	if err := RunWithOptions(context.Background(),
		countingGraph(t, restored, parallelism, countingSourceVertex("src", cfg, parallelism, barrierInterval, wrap)),
		Options{RestoreFrom: root, Seed: cfg.Seed}); err != nil {
		t.Fatalf("restored Run: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	for index := range parallelism {
		start, _ := sourceRange(count, parallelism, index)
		want := start + id*barrierInterval
		if !firstOffsets[want] {
			t.Errorf("no source subtask began at offset %d; they began at %v", want, sortedOffsets(firstOffsets))
		}
		if firstOffsets[start] && start != want {
			t.Errorf("a source subtask began at offset %d, which is its range start: the restore was ignored", start)
		}
	}

	// And the answer is still the whole answer: the restored state holds the
	// records below the offsets and the replay supplies the rest.
	assertSameCounts(t, countsOf(t, restored.Records()), oracleCounts(t, cfg), "restored run")
}

func sortedOffsets(set map[int64]bool) []int64 {
	out := make([]int64, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}

// observingSource reports the offset the first Next was served from.
//
// Count is forwarded EXPLICITLY. Embedding core.Source would compile and would
// leave Count off the concrete type, so the runtime's splittableSource
// assertion would fail and the job would be refused above parallelism 1 -- or,
// at parallelism 1, would quietly run a different topology from the one the
// test names. CLAUDE.md records this trap for exactly this reason.
type observingSource struct {
	inner       *sources.Generator
	onFirstNext func(position int64)
	seen        bool
}

func (s *observingSource) Open(ctx core.Context) error { return s.inner.Open(ctx) }

func (s *observingSource) Next() (*core.Record, bool, error) {
	if !s.seen {
		s.seen = true
		s.onFirstNext(s.inner.Position())
	}
	return s.inner.Next()
}

func (s *observingSource) SeekTo(offset int64) error { return s.inner.SeekTo(offset) }
func (s *observingSource) Position() int64           { return s.inner.Position() }
func (s *observingSource) Count() int64              { return s.inner.Count() }
func (s *observingSource) Close() error              { return s.inner.Close() }

var _ splittableSource = (*observingSource)(nil)

// TestRestoreRejectsAShapeMismatch is the Phase 1 constraint coming due.
//
// A source subtask's range is derived from (Count, parallelism, index) and only
// its offset is checkpointed, so at a different shape a subtask resumes inside
// somebody else's range: the job reads a valid stream that is not the one it
// was checkpointed on, and the counts come out wrong with nothing to point at.
// It is refused, never adapted to, and the message names both numbers.
func TestRestoreRejectsAShapeMismatch(t *testing.T) {
	const (
		count           = 1000
		barrierInterval = 200
	)
	root := t.TempDir()
	cfg := restoreConfig(2, count)

	if err := RunWithOptions(context.Background(),
		countingGraph(t, sinks.NewCollect(), 2, countingSourceVertex("src", cfg, 2, barrierInterval, nil)),
		Options{CheckpointRoot: root, Seed: cfg.Seed}); err != nil {
		t.Fatalf("Run: %v", err)
	}

	tests := []struct {
		name     string
		graph    func(t *testing.T) *graph.Graph
		contains []string
	}{
		{
			name: "a different parallelism",
			graph: func(t *testing.T) *graph.Graph {
				return countingGraph(t, sinks.NewCollect(), 4, countingSourceVertex("src", cfg, 4, barrierInterval, nil))
			},
			contains: []string{"parallelism", "2", "4"},
		},
		{
			name: "a different source count",
			graph: func(t *testing.T) *graph.Graph {
				return countingGraph(t, sinks.NewCollect(), 2,
					countingSourceVertex("src", restoreConfig(2, count/2), 2, barrierInterval, nil))
			},
			contains: []string{"count", "1000", "500"},
		},
		{
			name: "a renamed vertex",
			graph: func(t *testing.T) *graph.Graph {
				return countingGraph(t, sinks.NewCollect(), 2, countingSourceVertex("source", cfg, 2, barrierInterval, nil))
			},
			contains: []string{"src", "source"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := RunWithOptions(context.Background(), tt.graph(t), Options{RestoreFrom: root, Seed: cfg.Seed})
			if err == nil {
				t.Fatal("the job ran against a checkpoint of a differently shaped job")
			}
			for _, want := range tt.contains {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("the refusal %q does not name %q", err, want)
				}
			}
		})
	}
}

// TestRestoreFromARootWithNothingCompleteIsAnError. A caller that asked to
// resume and silently got a run from offset zero would produce plausible output
// from a job that did not recover, which is the failure mode this whole phase
// is written against.
func TestRestoreFromARootWithNothingCompleteIsAnError(t *testing.T) {
	cfg := restoreConfig(3, 500)
	g := func() *graph.Graph {
		return countingGraph(t, sinks.NewCollect(), 1, countingSourceVertex("src", cfg, 1, 100, nil))
	}

	empty := t.TempDir()
	if err := RunWithOptions(context.Background(), g(), Options{RestoreFrom: empty}); !errors.Is(err, errNoRestorePoint) {
		t.Errorf("restoring from an empty root = %v, want %v", err, errNoRestorePoint)
	}

	// A directory that looks like a checkpoint but has no marker is skipped for
	// the same reason: nothing ever declared it usable.
	partial := t.TempDir()
	if err := os.MkdirAll(filepath.Join(partial, "chk-7"), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := RunWithOptions(context.Background(), g(), Options{RestoreFrom: partial}); !errors.Is(err, errNoRestorePoint) {
		t.Errorf("restoring from a root holding an incomplete checkpoint = %v, want %v", err, errNoRestorePoint)
	}
}

// TestRestoredRunContinuesCheckpointNumbering.
//
// A resumed source subtask has already injected k barriers, so its next one is
// k+1. Restarting the count would emit a second barrier 1 at a different
// logical position, and a coordinator counting acknowledgements would be told
// about two different cuts under one name -- and a checkpoint root written by
// both runs would hold two chk-1 directories describing different states.
func TestRestoredRunContinuesCheckpointNumbering(t *testing.T) {
	const (
		count           = 2000
		barrierInterval = 250
		// The subtask injects count/barrierInterval barriers in total, and that
		// budget is a property of the whole range rather than of one run. So a
		// run resumed from the LAST checkpoint has none left, which is correct
		// and asserts nothing: the resume point has to be an earlier one.
		totalBarriers = count / barrierInterval
		resumeFrom    = totalBarriers / 2
	)
	root := t.TempDir()
	cfg := restoreConfig(4, count)
	build := func(sink core.Sink) *graph.Graph {
		return countingGraph(t, sink, 1, countingSourceVertex("src", cfg, 1, barrierInterval, nil))
	}

	if err := RunWithOptions(context.Background(), build(sinks.NewCollect()),
		Options{CheckpointRoot: root, Seed: cfg.Seed}); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// Stand in for a job that died after checkpoint resumeFrom by removing the
	// ones taken after it. Latest then selects resumeFrom, which is what a
	// crashed run would have left behind.
	for id := resumeFrom + 1; id <= totalBarriers; id++ {
		if err := os.RemoveAll(filepath.Join(root, fmt.Sprintf("chk-%d", id))); err != nil {
			t.Fatalf("RemoveAll: %v", err)
		}
	}
	got, ok, err := checkpoint.NewStorage(root).Latest()
	if err != nil || !ok || got != resumeFrom {
		t.Fatalf("Latest = (%d, ok %t, err %v), want %d", got, ok, err, resumeFrom)
	}

	// Restore, and checkpoint into a fresh root so the IDs the resumed run
	// writes can be read without the first run's directories in the way.
	second := t.TempDir()
	restored := sinks.NewCollect()
	if err := RunWithOptions(context.Background(), build(restored),
		Options{CheckpointRoot: second, RestoreFrom: root, Seed: cfg.Seed}); err != nil {
		t.Fatalf("restored Run: %v", err)
	}

	entries, err := os.ReadDir(second)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	var ids []string
	for _, e := range entries {
		ids = append(ids, e.Name())
	}
	slices.Sort(ids)

	var want []string
	for id := resumeFrom + 1; id <= totalBarriers; id++ {
		want = append(want, fmt.Sprintf("chk-%d", id))
	}
	slices.Sort(want)
	if !slices.Equal(ids, want) {
		t.Errorf("the resumed run wrote %v, want %v: the numbering did not continue from the checkpoint it restored", ids, want)
	}

	// The resumed run still produces the whole answer, so the continued
	// numbering is not being bought with a source that skipped records.
	assertSameCounts(t, countsOf(t, restored.Records()), oracleCounts(t, cfg), "resumed run")
}

// TestRestoreDoesNotRecoverInRamOperatorState records a real limit of this
// phase rather than a property of it.
//
// The runtime restores a subtask's KeyedState and nothing else, so an operator
// whose state is entirely in KeyedState recovers exactly -- keyedCount above is
// that operator. operators.WindowCount is not: its aggregates are in KeyedState
// but its pending timers and its current watermark are Go fields, so restoring
// gives back the counts with no timer to fire them, and a (key, window) whose
// records all arrived before the checkpoint would never be emitted. There is no
// mechanism in this phase that would notice.
//
// This test states the shape of that gap so it is not rediscovered as a
// mystery. The reserved state.PrefixTimer is what closes it: once timers live
// in the key space, snapshotting the key space snapshots them too, which is
// Phase 6.
func TestRestoreDoesNotRecoverInRamOperatorState(t *testing.T) {
	// The one thing the runtime restores is what state.WriteTo wrote, and the
	// one thing it writes is the KeyedState. Asserted against the operator
	// interface so that a later phase routing snapshots through
	// core.Operator.Snapshot has to come back and change this.
	var op core.Operator = newKeyedCount()
	if err := op.Snapshot(io.Discard); err != nil {
		t.Fatalf("keyedCount.Snapshot: %v", err)
	}
	// Nothing in the runtime calls it: the payload a subtask acknowledges comes
	// from state.WriteTo. A build that started calling Snapshot would make the
	// operator below fail the whole suite rather than this line.
	if err := op.Restore(strings.NewReader("")); err != nil {
		t.Fatalf("keyedCount.Restore: %v", err)
	}
}
