package runtime

import (
	"context"
	"errors"
	"io"
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
		AmountRange:    1000,
	}
}

// identity is the operator every chain below uses when the test is about the
// runtime rather than about the operator.
func identity() core.Operator {
	return operators.NewMap(func(r *core.Record) (*core.Record, error) { return r, nil })
}

// chain builds source "src" -> map "id" -> sink "out".
func chain(t *testing.T, newSource func() core.Source, newSink func() core.Sink) *graph.Graph {
	t.Helper()
	return chainWith(t, newSource, identity, newSink)
}

// chainWith is chain with the middle vertex supplied, for tests that need to
// observe the operator itself.
func chainWith(t *testing.T, newSource func() core.Source, newOperator func() core.Operator, newSink func() core.Sink) *graph.Graph {
	t.Helper()
	g := graph.New()
	vertices := []graph.Vertex{
		{ID: "src", Kind: graph.VertexSource, Parallelism: 1,
			WatermarkIntervalElements: testWatermarkInterval,
			BarrierIntervalElements:   testBarrierInterval,
			NewSource:                 newSource},
		{ID: "id", Kind: graph.VertexOperator, Parallelism: 1, NewOperator: newOperator},
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
		{ID: "src", Kind: graph.VertexSource, Parallelism: 1,
			WatermarkIntervalElements: testWatermarkInterval,
			BarrierIntervalElements:   testBarrierInterval,
			NewSource: func() core.Source {
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

func (s *failingSink) Snapshot(io.Writer) error             { return nil }
func (s *failingSink) NotifyCheckpointComplete(int64) error { return nil }
func (s *failingSink) Close() error                         { return nil }

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

func (s *blockingSink) Snapshot(io.Writer) error             { return nil }
func (s *blockingSink) NotifyCheckpointComplete(int64) error { return nil }
func (s *blockingSink) Close() error                         { return nil }

// closeRecording wraps a source, operator or sink so the test can observe
// exactly when the runtime finished with it. Close runs on every exit path, so
// a non-zero count means that goroutine has unwound. The count, rather than a
// flag, is what lets a test distinguish "closed" from "closed twice".
type closeRecordingSource struct {
	core.Source
	closes *atomic.Int64
}

func (s *closeRecordingSource) Close() error {
	s.closes.Add(1)
	return s.Source.Close()
}

type closeRecordingOperator struct {
	core.Operator
	closes *atomic.Int64
}

func (o *closeRecordingOperator) Close() error {
	o.closes.Add(1)
	return o.Operator.Close()
}

type closeRecordingSink struct {
	core.Sink
	closes *atomic.Int64
}

func (s *closeRecordingSink) Close() error {
	s.closes.Add(1)
	return s.Sink.Close()
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
		var srcCloses, opCloses atomic.Int64

		// More records than the two channels can buffer, so the source and the
		// operator are genuinely blocked in Send rather than finished.
		count := int64(4 * transport.DefaultCapacity)

		g := graph.New()
		vertices := []graph.Vertex{
			{ID: "src", Kind: graph.VertexSource, Parallelism: 1,
				WatermarkIntervalElements: testWatermarkInterval,
				BarrierIntervalElements:   testBarrierInterval,
				NewSource: func() core.Source {
					return &closeRecordingSource{
						Source: sources.NewGenerator(testGeneratorConfig(count)),
						closes: &srcCloses,
					}
				}},
			{ID: "id", Kind: graph.VertexOperator, Parallelism: 1, NewOperator: func() core.Operator {
				return &closeRecordingOperator{
					Operator: identity(),
					closes:   &opCloses,
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
		if srcCloses.Load() != 0 || opCloses.Load() != 0 {
			t.Fatalf("a vertex exited before cancellation: source closed %d times, operator %d times",
				srcCloses.Load(), opCloses.Load())
		}

		cancel()
		synctest.Wait()

		// The sink is still parked in user code, so the pipeline has not
		// drained. Both upstream vertices must nevertheless be gone: they can
		// only have got there by Send returning ctx.Err().
		if srcCloses.Load() == 0 {
			t.Error("the source is still blocked in Send after cancellation")
		}
		if opCloses.Load() == 0 {
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
		WatermarkIntervalElements: testWatermarkInterval,
		BarrierIntervalElements:   testBarrierInterval,
		NewSource:                 func() core.Source { return sources.NewGenerator(testGeneratorConfig(1)) }}); err != nil {
		t.Fatalf("AddVertex: %v", err)
	}
	if err := Run(context.Background(), g); err == nil {
		t.Fatal("Run accepted a graph with no sink")
	}
}

func TestOpContextInitialWatermark(t *testing.T) {
	oc := newOpContext(context.Background(), subtaskID{vertexID: "op", index: 0}, nil)
	if got := oc.CurrentWatermark(); got != math.MinInt64 {
		t.Errorf("CurrentWatermark = %d, want math.MinInt64", got)
	}
}

// TestRunClosesDownstreamOnUpstreamFailure covers the quiet exit path. When an
// upstream vertex fails, the runtime closes its output channel, so a downstream
// Recv returns ok=false with no end-of-stream element. That vertex reports
// nothing, because the failed one is the one that reports; core.Operator
// nevertheless documents Close as running exactly once whether or not the
// subtask completed, and Phase 3 attaches state to that guarantee.
//
// Failing before the first record is the case that pins the quiet exit itself:
// the operator emits nothing, so Recv returning ok=false is the only way it can
// leave its loop. Failing mid-stream races the operator's own Emit against the
// cancellation and may leave by either path, which is exactly why Close has to
// run on both.
func TestRunClosesDownstreamOnUpstreamFailure(t *testing.T) {
	tests := []struct {
		name   string
		failAt int64
	}{
		{name: "before any record", failAt: 0},
		{name: "mid stream", failAt: 5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var opCloses, sinkCloses atomic.Int64

			g := chainWith(t,
				func() core.Source { return &failingSource{failAt: tt.failAt} },
				func() core.Operator {
					return &closeRecordingOperator{Operator: identity(), closes: &opCloses}
				},
				func() core.Sink {
					return &closeRecordingSink{Sink: sinks.NewCollect(), closes: &sinkCloses}
				},
			)

			// Run returns only after every vertex goroutine has unwound, so the
			// counts below are final rather than sampled.
			if err := Run(context.Background(), g); !errors.Is(err, errSourceFailed) {
				t.Fatalf("Run = %v, want %v", err, errSourceFailed)
			}
			if got := opCloses.Load(); got != 1 {
				t.Errorf("operator Close ran %d times, want exactly 1", got)
			}
			if got := sinkCloses.Load(); got != 1 {
				t.Errorf("sink Close ran %d times, want exactly 1", got)
			}
		})
	}
}

// TestOpContextEmitStopsAfterFirstFailure pins the bound on an operator that
// emits in a loop. The runtime only collects the stash between operator calls,
// so once a Send has failed every remaining Emit in that call must be a no-op:
// otherwise the operator grinds through a Send per record that cannot succeed,
// and a later Send that did succeed would clear the error that explains why the
// job is stopping.
func TestOpContextEmitStopsAfterFirstFailure(t *testing.T) {
	const capacity = 4
	ch := transport.NewChannel(capacity)
	ctx, cancel := context.WithCancel(context.Background())
	oc := newOpContext(ctx, subtaskID{vertexID: "op", index: 0}, transport.NewWriter([][]transport.Output{{ch}}))

	// Fill the buffer first, so the Emit after cancellation has nowhere to put
	// its record and fails on the cancelled context rather than racing it.
	for range capacity {
		oc.Emit(&core.Record{Key: []byte("before")})
	}
	if err := oc.takeErr(); err != nil {
		t.Fatalf("filling the buffer: %v", err)
	}

	cancel()
	oc.Emit(&core.Record{Key: []byte("fails")})
	if !errors.Is(oc.err, context.Canceled) {
		t.Fatalf("Emit into a cancelled context held %v, want context.Canceled", oc.err)
	}

	// Drain the buffer and put a live context back, which removes the only
	// source of nondeterminism here: Send selects between delivering and
	// observing cancellation, and with both ready the choice is random. Against
	// a live context and an empty buffer, an Emit that still reached Send
	// delivers its record and overwrites the stash with nil, every time.
	for range capacity {
		if _, ok := ch.Recv(); !ok {
			t.Fatal("Recv reported closure on a channel holding records")
		}
	}
	oc.ctx = context.Background()
	// The loop is the case that matters: this is an operator emitting per input
	// record, with room for every one of them.
	for range capacity {
		oc.Emit(&core.Record{Key: []byte("after")})
	}

	if !errors.Is(oc.err, context.Canceled) {
		t.Errorf("a later Emit replaced the first error with %v", oc.err)
	}
	ch.Close()
	for {
		e, ok := ch.Recv()
		if !ok {
			break
		}
		t.Errorf("an Emit after a failed Send reached the channel with key %q", e.Record.Key)
	}
	if err := oc.takeErr(); !errors.Is(err, context.Canceled) {
		t.Errorf("takeErr = %v, want the first error", err)
	}
	if err := oc.takeErr(); err != nil {
		t.Errorf("takeErr did not clear the error: %v", err)
	}
}

// TestOpContextEmitHoldsSendError checks that a failed Send is not lost. Emit
// has no error return, so an error dropped here would truncate the output with
// no report anywhere.
func TestOpContextEmitHoldsSendError(t *testing.T) {
	ch := transport.NewChannel(1)
	ctx, cancel := context.WithCancel(context.Background())
	oc := newOpContext(ctx, subtaskID{vertexID: "op", index: 0}, transport.NewWriter([][]transport.Output{{ch}}))

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
