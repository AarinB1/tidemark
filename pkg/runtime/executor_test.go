package runtime

import (
	"context"
	"errors"
	"math"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/AarinB1/tidemark/pkg/core"
	"github.com/AarinB1/tidemark/pkg/graph"
	"github.com/AarinB1/tidemark/pkg/operators"
	"github.com/AarinB1/tidemark/pkg/sinks"
	"github.com/AarinB1/tidemark/pkg/sources"
	"github.com/AarinB1/tidemark/pkg/transport"
)

func testGeneratorConfig(count int64) sources.GeneratorConfig {
	return sources.GeneratorConfig{
		Seed:           7,
		Count:          count,
		KeyCardinality: 16,
		BaseEventTime:  1700000000000,
		EventTimeStep:  10,
		MaxLag:         50,
		ValueSize:      8,
	}
}

// chain builds source "src" -> map "id" -> sink "out".
func chain(t *testing.T, newSource func() core.Source, newSink func() core.Sink) *graph.Graph {
	t.Helper()
	g := graph.New()
	vertices := []graph.Vertex{
		{ID: "src", Kind: graph.VertexSource, Parallelism: 1, NewSource: newSource},
		{ID: "id", Kind: graph.VertexOperator, Parallelism: 1, NewOperator: func() core.Operator {
			return operators.NewMap(func(r *core.Record) (*core.Record, error) { return r, nil })
		}},
		{ID: "out", Kind: graph.VertexSink, Parallelism: 1, NewSink: newSink},
	}
	for _, v := range vertices {
		if err := g.AddVertex(v); err != nil {
			t.Fatalf("AddVertex(%s): %v", v.ID, err)
		}
	}
	for _, e := range [][2]string{{"src", "id"}, {"id", "out"}} {
		if err := g.Connect(e[0], e[1]); err != nil {
			t.Fatalf("Connect(%s, %s): %v", e[0], e[1], err)
		}
	}
	return g
}

func TestRunChainDeliversEveryRecord(t *testing.T) {
	counts := []int64{1, 100, 5000}
	for _, count := range counts {
		t.Run(itoa(count), func(t *testing.T) {
			collect := sinks.NewCollect()
			g := chain(t,
				func() core.Source { return sources.NewGenerator(testGeneratorConfig(count)) },
				func() core.Sink { return collect },
			)
			if err := Run(context.Background(), g); err != nil {
				t.Fatalf("Run: %v", err)
			}
			if got := int64(len(collect.Records())); got != count {
				t.Errorf("sink holds %d records, want %d", got, count)
			}
		})
	}
}

// TestRunFiltersAndDrops checks that the executor delivers whatever the
// operator emits, not whatever the source produced.
func TestRunFiltersAndDrops(t *testing.T) {
	const count = 1000
	collect := sinks.NewCollect()

	g := graph.New()
	vertices := []graph.Vertex{
		{ID: "src", Kind: graph.VertexSource, Parallelism: 1, NewSource: func() core.Source {
			return sources.NewGenerator(testGeneratorConfig(count))
		}},
		{ID: "even", Kind: graph.VertexOperator, Parallelism: 1, NewOperator: func() core.Operator {
			return operators.NewFilter(func(r *core.Record) bool { return r.EventTime%2 == 0 })
		}},
		{ID: "out", Kind: graph.VertexSink, Parallelism: 1, NewSink: func() core.Sink { return collect }},
	}
	for _, v := range vertices {
		if err := g.AddVertex(v); err != nil {
			t.Fatalf("AddVertex: %v", err)
		}
	}
	for _, e := range [][2]string{{"src", "even"}, {"even", "out"}} {
		if err := g.Connect(e[0], e[1]); err != nil {
			t.Fatalf("Connect: %v", err)
		}
	}

	if err := Run(context.Background(), g); err != nil {
		t.Fatalf("Run: %v", err)
	}

	got := collect.Records()
	if len(got) == 0 || int64(len(got)) == count {
		t.Fatalf("filter passed %d of %d records; expected a strict subset", len(got), count)
	}
	for _, r := range got {
		if r.EventTime%2 != 0 {
			t.Fatalf("record with odd event time %d reached the sink", r.EventTime)
		}
	}
}

var errSourceFailed = errors.New("source failed")

// failingSource produces records until failAt, then returns an error.
type failingSource struct {
	failAt int64
	pos    int64
}

func (s *failingSource) Open(core.Context) error { return nil }

func (s *failingSource) Next() (*core.Record, bool, error) {
	if s.pos == s.failAt {
		return nil, false, errSourceFailed
	}
	s.pos++
	return &core.Record{Key: []byte("k"), Value: []byte("v"), EventTime: s.pos}, true, nil
}

func (s *failingSource) SeekTo(offset int64) error { s.pos = offset; return nil }
func (s *failingSource) Position() int64           { return s.pos }
func (s *failingSource) Close() error              { return nil }

func TestRunPropagatesSourceError(t *testing.T) {
	collect := sinks.NewCollect()
	g := chain(t,
		func() core.Source { return &failingSource{failAt: 5} },
		func() core.Sink { return collect },
	)
	err := Run(context.Background(), g)
	if !errors.Is(err, errSourceFailed) {
		t.Fatalf("Run = %v, want %v", err, errSourceFailed)
	}
}

var errSinkFailed = errors.New("sink failed")

type failingSink struct{ failAt, seen int }

func (s *failingSink) Open(core.Context) error { return nil }

func (s *failingSink) Write(*core.Record) error {
	s.seen++
	if s.seen > s.failAt {
		return errSinkFailed
	}
	return nil
}

func (s *failingSink) Close() error { return nil }

// TestRunPropagatesSinkError also exercises the unwinding path: the source and
// operator are upstream of the failure and may be blocked in Send when it
// happens.
func TestRunPropagatesSinkError(t *testing.T) {
	g := chain(t,
		func() core.Source { return sources.NewGenerator(testGeneratorConfig(50000)) },
		func() core.Sink { return &failingSink{failAt: 10} },
	)
	err := Run(context.Background(), g)
	if !errors.Is(err, errSinkFailed) {
		t.Fatalf("Run = %v, want %v", err, errSinkFailed)
	}
}

// blockingSink parks in Write until release is closed. It exists so the test can
// hold the whole pipeline in a known state: with the sink stalled, the channels
// fill and both upstream goroutines end up blocked in Send.
type blockingSink struct {
	release chan struct{}
	entered chan struct{}
	once    bool
}

func (s *blockingSink) Open(core.Context) error { return nil }

func (s *blockingSink) Write(*core.Record) error {
	if !s.once {
		s.once = true
		close(s.entered)
	}
	<-s.release
	return nil
}

func (s *blockingSink) Close() error { return nil }

// closeRecording wraps a source or operator so the test can observe exactly
// when the runtime finished with it. Close runs on every exit path, so the flag
// flipping means that goroutine has unwound.
type closeRecordingSource struct {
	core.Source
	closed *atomic.Bool
}

func (s *closeRecordingSource) Close() error {
	s.closed.Store(true)
	return s.Source.Close()
}

type closeRecordingOperator struct {
	core.Operator
	closed *atomic.Bool
}

func (o *closeRecordingOperator) Close() error {
	o.closed.Store(true)
	return o.Operator.Close()
}

// TestRunCancelledMidRun asserts that cancelling the context unwinds goroutines
// blocked in Send. The sink parks in Write, which backs the pipeline up until
// both upstream vertices are blocked sending; cancellation must get them out
// while the sink is still parked, so nothing here can be explained by the
// pipeline simply draining.
//
// The whole test runs inside a synctest bubble. synctest.Wait returns only once
// every other goroutine is durably blocked, and synctest.Test fails on a
// deadlock rather than hanging, so "no goroutine left behind" is an assertion
// and not a sleep.
func TestRunCancelledMidRun(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sink := &blockingSink{
			release: make(chan struct{}),
			entered: make(chan struct{}),
		}
		var srcClosed, opClosed atomic.Bool

		// More records than the two channels can buffer, so the source and the
		// operator are genuinely blocked in Send rather than finished.
		count := int64(4 * transport.DefaultCapacity)

		g := graph.New()
		vertices := []graph.Vertex{
			{ID: "src", Kind: graph.VertexSource, Parallelism: 1, NewSource: func() core.Source {
				return &closeRecordingSource{
					Source: sources.NewGenerator(testGeneratorConfig(count)),
					closed: &srcClosed,
				}
			}},
			{ID: "id", Kind: graph.VertexOperator, Parallelism: 1, NewOperator: func() core.Operator {
				return &closeRecordingOperator{
					Operator: operators.NewMap(func(r *core.Record) (*core.Record, error) { return r, nil }),
					closed:   &opClosed,
				}
			}},
			{ID: "out", Kind: graph.VertexSink, Parallelism: 1, NewSink: func() core.Sink { return sink }},
		}
		for _, v := range vertices {
			if err := g.AddVertex(v); err != nil {
				t.Fatalf("AddVertex: %v", err)
			}
		}
		for _, e := range [][2]string{{"src", "id"}, {"id", "out"}} {
			if err := g.Connect(e[0], e[1]); err != nil {
				t.Fatalf("Connect: %v", err)
			}
		}

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			done <- Run(ctx, g)
		}()

		<-sink.entered
		synctest.Wait()
		select {
		case err := <-done:
			t.Fatalf("Run returned %v before cancellation", err)
		default:
		}
		if srcClosed.Load() || opClosed.Load() {
			t.Fatalf("a vertex exited before cancellation: source %v, operator %v",
				srcClosed.Load(), opClosed.Load())
		}

		cancel()
		synctest.Wait()

		// The sink is still parked in user code, so the pipeline has not
		// drained. Both upstream vertices must nevertheless be gone: they can
		// only have got there by Send returning ctx.Err().
		if !srcClosed.Load() {
			t.Error("the source is still blocked in Send after cancellation")
		}
		if !opClosed.Load() {
			t.Error("the operator is still blocked in Send after cancellation")
		}

		// The runtime cannot interrupt user code, so the test releases the sink
		// the way a real sink's own IO would eventually return.
		close(sink.release)

		err := <-done
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run = %v, want context.Canceled", err)
		}
	})
}

func TestRunRejectsInvalidGraph(t *testing.T) {
	g := graph.New()
	if err := g.AddVertex(graph.Vertex{ID: "src", Kind: graph.VertexSource, Parallelism: 1,
		NewSource: func() core.Source { return sources.NewGenerator(testGeneratorConfig(1)) }}); err != nil {
		t.Fatalf("AddVertex: %v", err)
	}
	if err := Run(context.Background(), g); err == nil {
		t.Fatal("Run accepted a graph with no sink")
	}
}

// TestRunRejectsMultiInputAndFanOut pins the Phase 0 boundary: merging inputs
// needs the input gate and splitting outputs needs the partitioner, both of
// which arrive in Phase 1. Approximating either produces wrong results rather
// than a failure, so Run refuses.
func TestRunRejectsMultiInputAndFanOut(t *testing.T) {
	newSource := func() core.Source { return sources.NewGenerator(testGeneratorConfig(10)) }
	newSink := func() core.Sink { return sinks.NewCollect() }

	tests := []struct {
		name     string
		vertices []graph.Vertex
		edges    [][2]string
	}{
		{
			name: "two inputs",
			vertices: []graph.Vertex{
				{ID: "a", Kind: graph.VertexSource, Parallelism: 1, NewSource: newSource},
				{ID: "b", Kind: graph.VertexSource, Parallelism: 1, NewSource: newSource},
				{ID: "out", Kind: graph.VertexSink, Parallelism: 1, NewSink: newSink},
			},
			edges: [][2]string{{"a", "out"}, {"b", "out"}},
		},
		{
			name: "two outputs",
			vertices: []graph.Vertex{
				{ID: "src", Kind: graph.VertexSource, Parallelism: 1, NewSource: newSource},
				{ID: "x", Kind: graph.VertexSink, Parallelism: 1, NewSink: newSink},
				{ID: "y", Kind: graph.VertexSink, Parallelism: 1, NewSink: newSink},
			},
			edges: [][2]string{{"src", "x"}, {"src", "y"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := graph.New()
			for _, v := range tt.vertices {
				if err := g.AddVertex(v); err != nil {
					t.Fatalf("AddVertex: %v", err)
				}
			}
			for _, e := range tt.edges {
				if err := g.Connect(e[0], e[1]); err != nil {
					t.Fatalf("Connect: %v", err)
				}
			}
			if err := Run(context.Background(), g); err == nil {
				t.Fatal("Run accepted a topology that needs Phase 1")
			}
		})
	}
}

func TestOpContextInitialWatermark(t *testing.T) {
	oc := newOpContext(context.Background(), nil)
	if got := oc.CurrentWatermark(); got != math.MinInt64 {
		t.Errorf("CurrentWatermark = %d, want math.MinInt64", got)
	}
}

// TestOpContextEmitHoldsSendError checks that a failed Send is not lost. Emit
// has no error return, so an error dropped here would truncate the output with
// no report anywhere.
func TestOpContextEmitHoldsSendError(t *testing.T) {
	ch := transport.NewChannel(1)
	ctx, cancel := context.WithCancel(context.Background())
	oc := newOpContext(ctx, []*transport.Channel{ch})

	oc.Emit(&core.Record{Key: []byte("a")})
	if err := oc.takeErr(); err != nil {
		t.Fatalf("first Emit: %v", err)
	}

	cancel()
	oc.Emit(&core.Record{Key: []byte("b")})
	if err := oc.takeErr(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Emit into a cancelled context held %v, want context.Canceled", err)
	}
	if err := oc.takeErr(); err != nil {
		t.Errorf("takeErr did not clear the error: %v", err)
	}
}

func itoa(v int64) string {
	if v == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	return string(buf[i:])
}
