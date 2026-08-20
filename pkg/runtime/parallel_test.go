package runtime

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/AarinB1/tidemark/pkg/core"
	"github.com/AarinB1/tidemark/pkg/graph"
	"github.com/AarinB1/tidemark/pkg/operators"
	"github.com/AarinB1/tidemark/pkg/sinks"
	"github.com/AarinB1/tidemark/pkg/sources"
)

// buildGraph assembles vertices and edges, failing the test on a rejection.
func buildGraph(t *testing.T, vertices []graph.Vertex, edges [][2]string) *graph.Graph {
	t.Helper()
	g := graph.New()
	for _, v := range vertices {
		if err := g.AddVertex(v); err != nil {
			t.Fatalf("AddVertex(%s): %v", v.ID, err)
		}
	}
	for _, e := range edges {
		if err := g.Connect(e[0], e[1]); err != nil {
			t.Fatalf("Connect(%s, %s): %v", e[0], e[1], err)
		}
	}
	return g
}

// parallelChain is source -> identity map -> sink, every vertex at p.
//
// Every vertex scales together. A source pinned at 1 feeding p map subtasks
// would exercise the source rather than the shuffle, and would pass whatever
// the partitioner did.
func parallelChain(t *testing.T, cfg sources.GeneratorConfig, p int, sink core.Sink) *graph.Graph {
	t.Helper()
	return buildGraph(t, []graph.Vertex{
		{ID: "src", Kind: graph.VertexSource, Parallelism: p,
			NewSource: func() core.Source { return sources.NewGenerator(cfg) }},
		{ID: "id", Kind: graph.VertexOperator, Parallelism: p, NewOperator: identity},
		{ID: "out", Kind: graph.VertexSink, Parallelism: p,
			NewSink: func() core.Sink { return sink }},
	}, [][2]string{{"src", "id"}, {"id", "out"}})
}

// TestRunAtParallelismDeliversEveryRecord is the first thing step 4 has to buy:
// with P source subtasks reading disjoint ranges, P operator subtasks behind a
// shuffle, and P sink subtasks, every record still arrives exactly once.
func TestRunAtParallelismDeliversEveryRecord(t *testing.T) {
	const count = 10000
	for _, p := range parallelismLevels {
		t.Run(itoa(int64(p)), func(t *testing.T) {
			collect := sinks.NewCollect()
			g := parallelChain(t, testGeneratorConfig(count), p, collect)
			if err := Run(context.Background(), g); err != nil {
				t.Fatalf("Run: %v", err)
			}
			if got := len(collect.Records()); got != count {
				t.Errorf("sink holds %d records, want %d", got, count)
			}
		})
	}
}

// TestRunSendsAKeyToOneSinkSubtask is the property Phase 3 keyed state depends
// on. Each sink subtask gets its own Collect, so the test can see where a key
// landed rather than only that it arrived.
func TestRunSendsAKeyToOneSinkSubtask(t *testing.T) {
	const (
		p     = 4
		count = 5000
	)
	cfg := testGeneratorConfig(count)

	var next atomic.Int64
	collects := make([]*sinks.Collect, p)
	for i := range collects {
		collects[i] = sinks.NewCollect()
	}

	g := buildGraph(t, []graph.Vertex{
		{ID: "src", Kind: graph.VertexSource, Parallelism: p,
			NewSource: func() core.Source { return sources.NewGenerator(cfg) }},
		{ID: "id", Kind: graph.VertexOperator, Parallelism: p, NewOperator: identity},
		{ID: "out", Kind: graph.VertexSink, Parallelism: p,
			NewSink: func() core.Sink { return collects[next.Add(1)-1] }},
	}, [][2]string{{"src", "id"}, {"id", "out"}})

	if err := Run(context.Background(), g); err != nil {
		t.Fatalf("Run: %v", err)
	}

	where := make(map[string]int)
	total := 0
	for i, c := range collects {
		for _, rec := range c.Records() {
			total++
			k := string(rec.Key)
			if seen, ok := where[k]; ok && seen != i {
				t.Fatalf("key %x reached sink subtask %d and sink subtask %d", rec.Key, seen, i)
			}
			where[k] = i
		}
	}
	if total != count {
		t.Errorf("%d records reached the sinks, want %d", total, count)
	}
	if len(where) < 2 {
		t.Errorf("every key landed on one subtask (%d distinct keys); the partitioner is not spreading", len(where))
	}
}

// recordingGenerator counts Closes without hiding the generator behind
// core.Source.
//
// The embedded type is the concrete *sources.Generator rather than the
// interface, which is what keeps Count promoted. A decorator that embedded
// core.Source would expose only that interface's methods, so the runtime's
// assertion for a splittable source would fail and the job would be refused at
// parallelism > 1. That refusal is the right behaviour, since the alternative
// is every subtask reading the whole input, but it is worth knowing that
// wrapping a source is where it bites.
type recordingGenerator struct {
	*sources.Generator
	closes *atomic.Int64
}

func (s *recordingGenerator) Close() error {
	s.closes.Add(1)
	return s.Generator.Close()
}

// recordingFailingSource is splittable and fails once a subtask reaches failAt,
// so exactly one subtask of a parallel source vertex reports.
type recordingFailingSource struct {
	count  int64
	failAt int64
	pos    int64
	closes *atomic.Int64
}

func (s *recordingFailingSource) Open(core.Context) error { return nil }

func (s *recordingFailingSource) Next() (*core.Record, bool, error) {
	if s.pos == s.failAt {
		return nil, false, errSourceFailed
	}
	if s.pos >= s.count {
		return nil, false, nil
	}
	s.pos++
	return &core.Record{Key: []byte("k"), Value: []byte("v"), EventTime: s.pos}, true, nil
}

func (s *recordingFailingSource) SeekTo(offset int64) error { s.pos = offset; return nil }
func (s *recordingFailingSource) Position() int64           { return s.pos }
func (s *recordingFailingSource) Count() int64              { return s.count }

func (s *recordingFailingSource) Close() error {
	s.closes.Add(1)
	return nil
}

// TestRunAtParallelismCallsCloseExactlyOncePerSubtask holds the Phase 0 error
// contract at P subtasks. Close runs once per subtask on every path, and
// core.Operator documents that guarantee whether or not the subtask completed;
// Phase 3 attaches state teardown to it.
func TestRunAtParallelismCallsCloseExactlyOncePerSubtask(t *testing.T) {
	tests := []struct {
		name string
		fail bool
	}{
		{name: "clean run"},
		{name: "source fails", fail: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const p = 4
			var srcCloses, opCloses, sinkCloses atomic.Int64

			newSource := func() core.Source {
				if tt.fail {
					// failAt lands in subtask 0's range, [0, 250), so one
					// subtask fails and the rest are cancelled mid-stream.
					return &recordingFailingSource{count: 1000, failAt: 5, closes: &srcCloses}
				}
				return &recordingGenerator{
					Generator: sources.NewGenerator(testGeneratorConfig(1000)),
					closes:    &srcCloses,
				}
			}

			g := buildGraph(t, []graph.Vertex{
				{ID: "src", Kind: graph.VertexSource, Parallelism: p, NewSource: newSource},
				{ID: "id", Kind: graph.VertexOperator, Parallelism: p, NewOperator: func() core.Operator {
					return &closeRecordingOperator{Operator: identity(), closes: &opCloses}
				}},
				{ID: "out", Kind: graph.VertexSink, Parallelism: p, NewSink: func() core.Sink {
					return &closeRecordingSink{Sink: sinks.NewCollect(), closes: &sinkCloses}
				}},
			}, [][2]string{{"src", "id"}, {"id", "out"}})

			err := Run(context.Background(), g)
			switch {
			case tt.fail && !errors.Is(err, errSourceFailed):
				t.Fatalf("Run = %v, want %v", err, errSourceFailed)
			case !tt.fail && err != nil:
				t.Fatalf("Run: %v", err)
			}

			// Run returns only after every subtask goroutine has unwound, so
			// these counts are final rather than sampled.
			for _, c := range []struct {
				name  string
				count int64
			}{
				{"source", srcCloses.Load()},
				{"operator", opCloses.Load()},
				{"sink", sinkCloses.Load()},
			} {
				if c.count != p {
					t.Errorf("%s Close ran %d times, want exactly %d, one per subtask", c.name, c.count, p)
				}
			}
		})
	}
}

// TestRunAtParallelismReportsTheFailingSubtask holds the rest of the error
// contract: the failing subtask is the one that reports, downstream subtasks
// that see a closed input with no end-of-stream exit quietly, and the first
// error into the buffered channel is what Run returns.
func TestRunAtParallelismReportsTheFailingSubtask(t *testing.T) {
	const p = 8
	g := buildGraph(t, []graph.Vertex{
		{ID: "src", Kind: graph.VertexSource, Parallelism: p,
			NewSource: func() core.Source { return sources.NewGenerator(testGeneratorConfig(50000)) }},
		{ID: "id", Kind: graph.VertexOperator, Parallelism: p, NewOperator: identity},
		{ID: "out", Kind: graph.VertexSink, Parallelism: p,
			NewSink: func() core.Sink { return &failingSink{failAt: 10} }},
	}, [][2]string{{"src", "id"}, {"id", "out"}})

	if err := Run(context.Background(), g); !errors.Is(err, errSinkFailed) {
		t.Fatalf("Run = %v, want %v", err, errSinkFailed)
	}
}

// TestRunAtParallelismUnwindsOnCancellation checks a job at P subtasks still
// comes apart when its context is cancelled. Every subtask and every gate
// forwarder must be gone by the time Run returns.
func TestRunAtParallelismUnwindsOnCancellation(t *testing.T) {
	const p = 4
	ctx, cancel := context.WithCancel(context.Background())

	var sinkCloses atomic.Int64
	g := buildGraph(t, []graph.Vertex{
		{ID: "src", Kind: graph.VertexSource, Parallelism: p,
			NewSource: func() core.Source { return sources.NewGenerator(testGeneratorConfig(1000000)) }},
		{ID: "id", Kind: graph.VertexOperator, Parallelism: p, NewOperator: identity},
		{ID: "out", Kind: graph.VertexSink, Parallelism: p, NewSink: func() core.Sink {
			return &closeRecordingSink{Sink: sinks.NewDiscard(), closes: &sinkCloses}
		}},
	}, [][2]string{{"src", "id"}, {"id", "out"}})

	done := make(chan error, 1)
	go func() { done <- Run(ctx, g) }()
	cancel()

	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}
	if got := sinkCloses.Load(); got != p {
		t.Errorf("sink Close ran %d times, want %d", got, p)
	}
}

// TestRunRejectsAnUnkeyedRecord pins the skew guard end to end. Routing every
// unkeyed record to subtask 0 would produce correct results slowly, with
// nothing pointing at the cause, so it is an error instead.
func TestRunRejectsAnUnkeyedRecord(t *testing.T) {
	g := buildGraph(t, []graph.Vertex{
		{ID: "src", Kind: graph.VertexSource, Parallelism: 2,
			NewSource: func() core.Source { return sources.NewGenerator(testGeneratorConfig(100)) }},
		{ID: "strip", Kind: graph.VertexOperator, Parallelism: 2, NewOperator: func() core.Operator {
			return operators.NewMap(func(r *core.Record) (*core.Record, error) {
				return &core.Record{Value: r.Value, EventTime: r.EventTime}, nil
			})
		}},
		{ID: "out", Kind: graph.VertexSink, Parallelism: 2,
			NewSink: func() core.Sink { return sinks.NewDiscard() }},
	}, [][2]string{{"src", "strip"}, {"strip", "out"}})

	if err := Run(context.Background(), g); err == nil {
		t.Fatal("Run partitioned a record with no key")
	}
}

// TestWireCreatesOneChannelPerSubtaskPair checks the P*Q wiring directly, since
// getting it wrong at P*Q channels is easy and the symptom downstream is a
// duplicated or missing record rather than a failure.
func TestWireCreatesOneChannelPerSubtaskPair(t *testing.T) {
	tests := []struct {
		name      string
		sourceP   int
		operatorP int
		sinkP     int
	}{
		{name: "one to one", sourceP: 1, operatorP: 1, sinkP: 1},
		{name: "widening", sourceP: 2, operatorP: 4, sinkP: 8},
		{name: "narrowing", sourceP: 8, operatorP: 4, sinkP: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := buildGraph(t, []graph.Vertex{
				{ID: "src", Kind: graph.VertexSource, Parallelism: tt.sourceP,
					NewSource: func() core.Source { return sources.NewGenerator(testGeneratorConfig(1)) }},
				{ID: "id", Kind: graph.VertexOperator, Parallelism: tt.operatorP, NewOperator: identity},
				{ID: "out", Kind: graph.VertexSink, Parallelism: tt.sinkP,
					NewSink: func() core.Sink { return sinks.NewDiscard() }},
			}, [][2]string{{"src", "id"}, {"id", "out"}})

			order, err := g.TopoOrder()
			if err != nil {
				t.Fatalf("TopoOrder: %v", err)
			}
			inputs, outputs := wire(g, order)

			for i := range tt.sourceP {
				if got := len(outputs[subtaskID{"src", i}]); got != tt.operatorP {
					t.Errorf("src[%d] has %d outputs, want %d", i, got, tt.operatorP)
				}
			}
			for j := range tt.operatorP {
				if got := len(inputs[subtaskID{"id", j}]); got != tt.sourceP {
					t.Errorf("id[%d] has %d inputs, want %d", j, got, tt.sourceP)
				}
				if got := len(outputs[subtaskID{"id", j}]); got != tt.sinkP {
					t.Errorf("id[%d] has %d outputs, want %d", j, got, tt.sinkP)
				}
			}
			for k := range tt.sinkP {
				if got := len(inputs[subtaskID{"out", k}]); got != tt.operatorP {
					t.Errorf("out[%d] has %d inputs, want %d", k, got, tt.operatorP)
				}
				if got := len(outputs[subtaskID{"out", k}]); got != 0 {
					t.Errorf("out[%d] has %d outputs, want none", k, got)
				}
			}

			// Every channel appears once as somebody's output and once as
			// somebody's input: one producer and one consumer each.
			producers := make(map[any]int)
			for _, outs := range outputs {
				for _, o := range outs {
					producers[o]++
				}
			}
			consumers := make(map[any]int)
			for _, ins := range inputs {
				for _, in := range ins {
					consumers[in]++
				}
			}
			if len(producers) != len(consumers) {
				t.Fatalf("%d channels are written and %d are read", len(producers), len(consumers))
			}
			for ch, n := range producers {
				if n != 1 {
					t.Errorf("a channel has %d producers, want exactly 1", n)
				}
				if consumers[ch] != 1 {
					t.Errorf("a channel has %d consumers, want exactly 1", consumers[ch])
				}
			}
		})
	}
}
