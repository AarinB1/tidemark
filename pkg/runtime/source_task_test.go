package runtime

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/AarinB1/tidemark/pkg/core"
	"github.com/AarinB1/tidemark/pkg/graph"
	"github.com/AarinB1/tidemark/pkg/sinks"
	"github.com/AarinB1/tidemark/pkg/sources"
	"github.com/AarinB1/tidemark/pkg/transport"
)

// parallelismLevels is the set every equivalence assertion in Phase 1 runs at.
var parallelismLevels = []int{1, 2, 4, 8}

// Every source vertex must state both per-subtask intervals, so these are what
// the graph-level tests in this package use wherever the interval itself is not
// the thing under test. Large enough that they cost nothing on the long runs;
// the tests that are about injection set their own.
const (
	testWatermarkInterval = 1000
	testBarrierInterval   = 1000
)

func TestSourceRangePartitionsTheOffsetSpace(t *testing.T) {
	tests := []struct {
		name        string
		count       int64
		parallelism int
		want        [][2]int64
	}{
		{
			name:        "one subtask takes everything",
			count:       10,
			parallelism: 1,
			want:        [][2]int64{{0, 10}},
		},
		{
			name:        "exact division",
			count:       8,
			parallelism: 4,
			want:        [][2]int64{{0, 2}, {2, 4}, {4, 6}, {6, 8}},
		},
		{
			// The remainder lands in the later subtasks rather than all in the
			// last one, and no subtask is more than one element off the mean.
			name:        "remainder without a special case",
			count:       10,
			parallelism: 4,
			want:        [][2]int64{{0, 2}, {2, 5}, {5, 7}, {7, 10}},
		},
		{
			name:        "more subtasks than records leaves empty ranges",
			count:       3,
			parallelism: 8,
			want:        [][2]int64{{0, 0}, {0, 0}, {0, 1}, {1, 1}, {1, 1}, {1, 2}, {2, 2}, {2, 3}},
		},
		{
			name:        "empty input",
			count:       0,
			parallelism: 4,
			want:        [][2]int64{{0, 0}, {0, 0}, {0, 0}, {0, 0}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for i, want := range tt.want {
				start, end := sourceRange(tt.count, tt.parallelism, i)
				if start != want[0] || end != want[1] {
					t.Errorf("sourceRange(%d, %d, %d) = [%d, %d), want [%d, %d)",
						tt.count, tt.parallelism, i, start, end, want[0], want[1])
				}
			}
		})
	}
}

// TestSourceRangeIsAPartition asserts the three properties every later claim in
// Phase 1 rests on: subtask 0 starts at 0, each range begins where the previous
// one ended, and the last one ends at count. Together they say the ranges are
// contiguous, non-overlapping, and exhaustive, which is what makes the union of
// P subtasks equal to the whole input.
func TestSourceRangeIsAPartition(t *testing.T) {
	counts := []int64{0, 1, 3, 10, 1000, 99991}
	for _, count := range counts {
		for _, p := range parallelismLevels {
			prevEnd := int64(0)
			for i := range p {
				start, end := sourceRange(count, p, i)
				if start != prevEnd {
					t.Errorf("count=%d p=%d subtask %d starts at %d, previous range ended at %d",
						count, p, i, start, prevEnd)
				}
				if end < start {
					t.Errorf("count=%d p=%d subtask %d has range [%d, %d), which runs backwards",
						count, p, i, start, end)
				}
				prevEnd = end
			}
			if prevEnd != count {
				t.Errorf("count=%d p=%d: the last range ends at %d, want %d", count, p, prevEnd, count)
			}
		}
	}
}

// TestSourceLoopUnionMatchesASingleSubtask is the property step 7's equivalence
// test rests on: the records the P subtasks of a source vertex read, taken
// together, are exactly the records one subtask reads for the same seed. If
// this fails, no amount of correct shuffling downstream can produce identical
// sink contents.
func TestSourceLoopUnionMatchesASingleSubtask(t *testing.T) {
	counts := []int64{1, 7, 1000, 10007}
	for _, count := range counts {
		t.Run(itoa(count), func(t *testing.T) {
			cfg := testGeneratorConfig(count)
			want := readSource(t, cfg, nil)

			for _, p := range parallelismLevels {
				var got []*core.Record
				for i := range p {
					got = append(got, readSubtaskRange(t, cfg, p, i)...)
				}
				assertSameRecords(t, got, want, "parallelism %d", p)
			}
		})
	}
}

// TestSourceLoopRangesDoNotOverlap checks the stronger claim underneath the
// union: each subtask reads a distinct contiguous slice, so the union holds
// because nothing is read twice rather than because two duplications cancelled
// out. Event time is strictly increasing in the offset here, which makes it a
// faithful stand-in for the offset itself.
func TestSourceLoopRangesDoNotOverlap(t *testing.T) {
	cfg := testGeneratorConfig(1000)
	cfg.MaxLag = 0

	for _, p := range parallelismLevels {
		var prevLast int64 = -1
		total := 0
		for i := range p {
			recs := readSubtaskRange(t, cfg, p, i)
			total += len(recs)
			if len(recs) == 0 {
				continue
			}
			if recs[0].EventTime <= prevLast {
				t.Errorf("parallelism %d: subtask %d starts at event time %d, but subtask %d ended at %d",
					p, i, recs[0].EventTime, i-1, prevLast)
			}
			for j := 1; j < len(recs); j++ {
				if recs[j].EventTime <= recs[j-1].EventTime {
					t.Fatalf("parallelism %d: subtask %d read event times out of order at %d", p, i, j)
				}
			}
			prevLast = recs[len(recs)-1].EventTime
		}
		if int64(total) != cfg.Count {
			t.Errorf("parallelism %d: subtasks read %d records in total, want %d", p, total, cfg.Count)
		}
	}
}

// TestSourceLoopRejectsAnUnsplittableSourceAtParallelism checks the boundary on
// a source with no Count. Reading the whole input in every subtask would
// duplicate every record P times and report nothing, so the request is refused.
func TestSourceLoopRejectsAnUnsplittableSourceAtParallelism(t *testing.T) {
	// countlessSource has no Count method, so it is not splittable.
	err := sourceLoop(context.Background(), &countlessSource{limit: 10}, 2, 0, noWatermarks(), noBarriers(),
		func(*core.Record) error { return nil }, unexpectedWatermark(t), unexpectedBarrier(t))
	if err == nil {
		t.Fatal("sourceLoop split a source that does not report a Count")
	}

	// At parallelism 1 there is nothing to divide, so the same source is read
	// to exhaustion.
	var seen int
	if err := sourceLoop(context.Background(), &countlessSource{limit: 10}, 1, 0, noWatermarks(), noBarriers(), func(*core.Record) error {
		seen++
		return nil
	}, unexpectedWatermark(t), unexpectedBarrier(t)); err != nil {
		t.Fatalf("sourceLoop at parallelism 1: %v", err)
	}
	if seen != 10 {
		t.Errorf("sourceLoop read %d records from an unsplittable source, want 10", seen)
	}
}

func TestSourceLoopStopsOnContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	src := sources.NewGenerator(testGeneratorConfig(100))
	if err := src.Open(nil); err != nil {
		t.Fatalf("Open: %v", err)
	}
	err := sourceLoop(ctx, src, 1, 0, noWatermarks(), noBarriers(), func(*core.Record) error {
		t.Error("sourceLoop emitted a record against a cancelled context")
		return nil
	}, unexpectedWatermark(t), unexpectedBarrier(t))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("sourceLoop = %v, want context.Canceled", err)
	}
}

func TestSourceLoopReturnsTheEmitError(t *testing.T) {
	errEmit := errors.New("emit failed")

	src := sources.NewGenerator(testGeneratorConfig(100))
	if err := src.Open(nil); err != nil {
		t.Fatalf("Open: %v", err)
	}
	var seen int
	err := sourceLoop(context.Background(), src, 1, 0, noWatermarks(), noBarriers(), func(*core.Record) error {
		seen++
		if seen == 3 {
			return errEmit
		}
		return nil
	}, unexpectedWatermark(t), unexpectedBarrier(t))
	if !errors.Is(err, errEmit) {
		t.Fatalf("sourceLoop = %v, want %v", err, errEmit)
	}
	if seen != 3 {
		t.Errorf("sourceLoop kept reading after emit failed: %d records", seen)
	}
}

// countlessSource is a bounded source that does not report a Count, standing in
// for the unbounded sources of later phases.
type countlessSource struct {
	limit int64
	pos   int64
}

func (s *countlessSource) Open(core.Context) error { return nil }

func (s *countlessSource) Next() (*core.Record, bool, error) {
	if s.pos >= s.limit {
		return nil, false, nil
	}
	s.pos++
	return &core.Record{Key: []byte("k"), EventTime: s.pos}, true, nil
}

func (s *countlessSource) SeekTo(offset int64) error { s.pos = offset; return nil }
func (s *countlessSource) Position() int64           { return s.pos }
func (s *countlessSource) Close() error              { return nil }

// readSubtaskRange drives sourceLoop for one subtask and returns what it read.
func readSubtaskRange(t *testing.T, cfg sources.GeneratorConfig, parallelism, index int) []*core.Record {
	t.Helper()
	src := sources.NewGenerator(cfg)
	if err := src.Open(nil); err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() {
		if err := src.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()

	var out []*core.Record
	err := sourceLoop(context.Background(), src, parallelism, index, noWatermarks(), noBarriers(), func(rec *core.Record) error {
		out = append(out, rec)
		return nil
	}, unexpectedWatermark(t), unexpectedBarrier(t))
	if err != nil {
		t.Fatalf("sourceLoop(p=%d, i=%d): %v", parallelism, index, err)
	}
	return out
}

// assertSameRecords compares two record sets by sorted contents. Order is never
// the assertion: the union of P subtasks is a set, and Phase 1's whole claim is
// about contents rather than sequence.
func assertSameRecords(t *testing.T, got, want []*core.Record, format string, args ...any) {
	t.Helper()
	label := fmt.Sprintf(format, args...)

	gotSorted := slices.Clone(got)
	wantSorted := slices.Clone(want)
	slices.SortFunc(gotSorted, compareRecords)
	slices.SortFunc(wantSorted, compareRecords)

	if len(gotSorted) != len(wantSorted) {
		t.Fatalf("%s: got %d records, want %d", label, len(gotSorted), len(wantSorted))
	}
	for i := range wantSorted {
		if compareRecords(gotSorted[i], wantSorted[i]) != 0 {
			t.Fatalf("%s: record %d: got %+v, want %+v", label, i, gotSorted[i], wantSorted[i])
		}
	}
}

// hidingDecorator wraps a source by embedding the core.Source interface, which
// promotes only that interface's methods. Count is not among them, so this type
// does not satisfy splittableSource however splittable the source inside it is.
//
// This is the accident the comment on splittableSource is about, written the
// way somebody would write it by hand.
type hidingDecorator struct {
	core.Source
}

// forwardingDecorator wraps a source the same way but forwards Count, which is
// all it takes to stay splittable.
type forwardingDecorator struct {
	core.Source
	count int64
}

func (s *forwardingDecorator) Count() int64 { return s.count }

// TestRunRefusesADecoratorThatHidesCount pins the rule that a decorator must
// forward Count explicitly.
//
// The two rows are the point. The first alone would pass for any reason a job
// might fail; the second, over the same source, same graph, and same
// parallelism, differing only in whether Count is forwarded, is what shows the
// refusal is about the missing Count and not something incidental to wrapping.
//
// Phase 4 wraps sources to inject faults at logical positions. A wrapper that
// dropped Count would force the fault suite to parallelism 1, where the
// concurrency bugs it exists to find do not occur, and it would still pass.
func TestRunRefusesADecoratorThatHidesCount(t *testing.T) {
	const (
		count = 500
		p     = 2
	)
	cfg := testGeneratorConfig(count)

	tests := []struct {
		name      string
		newSource func() core.Source
		wantErr   bool
	}{
		{
			name: "decorator hides Count",
			newSource: func() core.Source {
				return &hidingDecorator{Source: sources.NewGenerator(cfg)}
			},
			wantErr: true,
		},
		{
			name: "decorator forwards Count",
			newSource: func() core.Source {
				return &forwardingDecorator{Source: sources.NewGenerator(cfg), count: count}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			collect := sinks.NewCollect()
			g := buildGraph(t, []graph.Vertex{
				{ID: "src", Kind: graph.VertexSource, Parallelism: p,
					WatermarkIntervalElements: testWatermarkInterval,
					BarrierIntervalElements:   testBarrierInterval,
					NewSource:                 tt.newSource},
				{ID: "id", Kind: graph.VertexOperator, Parallelism: p, NewOperator: identity},
				{ID: "out", Kind: graph.VertexSink, Parallelism: p,
					NewSink: func() core.Sink { return collect }},
			}, [][2]string{{"src", "id"}, {"id", "out"}})

			err := Run(context.Background(), g)
			if !tt.wantErr {
				if err != nil {
					t.Fatalf("Run: %v", err)
				}
				// The forwarding decorator is not merely accepted: its subtasks
				// read disjoint ranges and together produce the whole source.
				assertSameRecords(t, collect.Records(), readSource(t, cfg, nil),
					"a decorator that forwards Count at parallelism %d", p)
				return
			}
			if err == nil {
				t.Fatal("Run split a source whose decorator does not report a Count")
			}
			// The message has to name the vertex. A refusal that said only
			// "source does not report a Count" would leave a job with several
			// source vertices with nothing to point at.
			if !strings.Contains(err.Error(), "src") {
				t.Errorf("Run = %v, want an error naming the src vertex", err)
			}
			if !strings.Contains(err.Error(), "Count") {
				t.Errorf("Run = %v, want an error naming the missing Count", err)
			}
		})
	}
}

// runWatermarkGenerator feeds eventTimes through a generator and returns, for
// each emission, the index of the record that produced it and the value.
func runWatermarkGenerator(intervalElements, maxOutOfOrderness int64, eventTimes []int64) (at []int, values []int64) {
	g := newWatermarkGenerator(intervalElements, maxOutOfOrderness)
	for i, t := range eventTimes {
		if wm, ok := g.onRecord(t); ok {
			at = append(at, i)
			values = append(values, wm)
		}
	}
	return at, values
}

// ramp returns n event times starting at base and rising by step.
func ramp(base, step int64, n int) []int64 {
	out := make([]int64, n)
	for i := range out {
		out[i] = base + int64(i)*step
	}
	return out
}

// TestWatermarkGeneratorEmitsOnTheElementInterval pins the interval to a count
// of records rather than to elapsed time.
//
// The assertion is the exact record index of every emission, not that "some
// watermarks arrived". A ticker-based generator satisfies the weaker claim on
// every run and satisfies this one on none of them, which is the point:
// invariant 6 says the fault schedule and everything a recovered run is
// compared against must be a function of logical position, and a watermark that
// lands after a different record on each run makes that false with no test to
// show for it.
func TestWatermarkGeneratorEmitsOnTheElementInterval(t *testing.T) {
	tests := []struct {
		name     string
		interval int64
		records  int
		wantAt   []int
	}{
		{name: "every record", interval: 1, records: 4, wantAt: []int{0, 1, 2, 3}},
		{name: "every third", interval: 3, records: 10, wantAt: []int{2, 5, 8}},
		{name: "interval longer than the input", interval: 100, records: 20, wantAt: nil},
		{name: "exactly one interval", interval: 5, records: 5, wantAt: []int{4}},
		// A zero or negative interval turns generation off rather than
		// emitting on every record or dividing by zero.
		{name: "disabled by zero", interval: 0, records: 10, wantAt: nil},
		{name: "disabled by a negative", interval: -1, records: 10, wantAt: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			at, _ := runWatermarkGenerator(tt.interval, 0, ramp(1000, 10, tt.records))
			if !slices.Equal(at, tt.wantAt) {
				t.Errorf("watermarks emitted after records %v, want %v", at, tt.wantAt)
			}
		})
	}
}

// TestWatermarkGeneratorValueIsMaxSeenMinusLagMinusOne pins the value exactly.
//
// The minus one is the whole contract: a watermark asserts that no element with
// event time <= w will arrive, so with out-of-orderness bounded by L the
// largest safe claim after observing maxSeen is maxSeen-L-1. Emitting maxSeen-L
// instead is off by one in the direction that fires a window before its last
// element arrives, and nothing crashes.
func TestWatermarkGeneratorValueIsMaxSeenMinusLagMinusOne(t *testing.T) {
	tests := []struct {
		name       string
		lag        int64
		eventTimes []int64
		want       []int64
	}{
		{
			name:       "in order, no lag",
			lag:        0,
			eventTimes: []int64{100, 200, 300, 400},
			want:       []int64{99, 199, 299, 399},
		},
		{
			name:       "in order, with lag",
			lag:        50,
			eventTimes: []int64{100, 200, 300, 400},
			want:       []int64{49, 149, 249, 349},
		},
		{
			// The value tracks the MAXIMUM observed, not the most recent. A
			// generator that used the last event time would walk the watermark
			// backwards on every out-of-order record.
			name:       "out of order tracks the maximum",
			lag:        10,
			eventTimes: []int64{100, 500, 200, 300, 900, 400},
			want:       []int64{89, 489, 889},
		},
		{
			name:       "negative event times",
			lag:        5,
			eventTimes: []int64{-1000, -900, -800},
			want:       []int64{-1006, -906, -806},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, got := runWatermarkGenerator(1, tt.lag, tt.eventTimes)
			if !slices.Equal(got, tt.want) {
				t.Errorf("watermarks %v, want %v", got, tt.want)
			}
		})
	}
}

// TestWatermarkGeneratorNeverDecreases feeds badly out-of-order input and
// asserts the emitted sequence is strictly increasing.
//
// core.Operator.ProcessWatermark documents wm as monotonically non-decreasing,
// and the gate and every window operator downstream are written against that.
// The input here is deliberately worse than the generator's bounded lag ever
// produces, because the property has to hold on the source's own behaviour
// rather than on the source's input being well behaved.
func TestWatermarkGeneratorNeverDecreases(t *testing.T) {
	eventTimes := []int64{
		5000, 100, 4000, 6000, 200, 5500, 7000, 50, 6500, 8000,
		300, 7500, 9000, 1000, 8500, 10000, 400, 9500, 11000, 600,
	}
	for _, interval := range []int64{1, 2, 3, 7} {
		_, got := runWatermarkGenerator(interval, 25, eventTimes)
		for i := 1; i < len(got); i++ {
			if got[i] <= got[i-1] {
				t.Errorf("interval %d: watermark %d (%d) did not advance on %d",
					interval, i, got[i], got[i-1])
			}
		}
		if len(got) == 0 {
			t.Errorf("interval %d: no watermark was emitted at all", interval)
		}
	}
}

// TestWatermarkGeneratorSkipsAnUnchangedValue is the other half of the
// monotonicity rule. Reaching an interval boundary is not enough: the value has
// to have moved. Re-emitting an unchanged watermark broadcasts to every
// downstream channel and tells nobody anything, and a gate that saw it would
// recompute a minimum that cannot have changed.
func TestWatermarkGeneratorSkipsAnUnchangedValue(t *testing.T) {
	// One high event time, then a long tail of lower ones. Only the first
	// interval boundary can produce anything; every later one sees the same
	// maximum.
	eventTimes := []int64{9000, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110}
	at, got := runWatermarkGenerator(2, 0, eventTimes)

	want := []int64{8999}
	if !slices.Equal(got, want) {
		t.Fatalf("watermarks %v at records %v, want %v from the first boundary only", got, at, want)
	}

	// The counter still resets on every boundary, so a later record that does
	// move the maximum is emitted at the next boundary rather than immediately.
	// Records 12 and 13 below are the seventh boundary.
	eventTimes = append(eventTimes, 20000, 20001)
	at, got = runWatermarkGenerator(2, 0, eventTimes)
	if !slices.Equal(got, []int64{8999, 20000}) || !slices.Equal(at, []int{1, 13}) {
		t.Errorf("watermarks %v at records %v, want [8999 20000] at records [1 13]", got, at)
	}
}

// TestWatermarkGeneratorClampsInsteadOfWrapping covers the subtraction at the
// bottom of the int64 range.
//
// An event time near MinInt64 minus a lag wraps to a large POSITIVE watermark,
// which claims every event in history has arrived and fires every open window
// at once. The result is a plausible-looking wrong answer with no error
// anywhere, which is the exact failure mode CLAUDE.md opens by naming. Clamped,
// the value stays at MinInt64, which is never strictly greater than the initial
// last-emitted value, so nothing is emitted at all.
func TestWatermarkGeneratorClampsInsteadOfWrapping(t *testing.T) {
	_, got := runWatermarkGenerator(1, 1000, []int64{math.MinInt64 + 10, math.MinInt64 + 20})
	if len(got) != 0 {
		t.Errorf("watermarks %v from event times at the bottom of the range, want none", got)
	}

	// The clamp must not swallow a value that does fit. One record above the
	// floor and the generator emits normally again.
	_, got = runWatermarkGenerator(1, 1000, []int64{math.MinInt64 + 10, 0})
	if !slices.Equal(got, []int64{-1001}) {
		t.Errorf("watermarks %v, want [-1001]", got)
	}
}

// TestSubFloorClampsBothEnds checks the helper directly, including the rows
// where nothing is clamped. A saturating subtraction that returned MinInt64 too
// eagerly would silence a source whose event times are merely large and
// negative rather than at the boundary.
func TestSubFloorClampsBothEnds(t *testing.T) {
	tests := []struct {
		a, b, want int64
	}{
		{a: 0, b: 0, want: 0},
		{a: 100, b: 1, want: 99},
		{a: -100, b: 1, want: -101},
		{a: -100, b: -1, want: -99},
		{a: math.MinInt64, b: 1, want: math.MinInt64},
		{a: math.MinInt64 + 5, b: 10, want: math.MinInt64},
		{a: math.MaxInt64, b: -1, want: math.MaxInt64},
		{a: math.MaxInt64 - 5, b: -10, want: math.MaxInt64},
		{a: math.MinInt64, b: math.MinInt64, want: 0},
		{a: math.MaxInt64, b: math.MaxInt64, want: 0},
	}
	for _, tt := range tests {
		if got := subFloor(tt.a, tt.b); got != tt.want {
			t.Errorf("subFloor(%d, %d) = %d, want %d", tt.a, tt.b, got, tt.want)
		}
	}
}

// noWatermarks disables generation, for the tests that are about the range
// logic rather than about event time.
func noWatermarks() watermarkGenerator { return newWatermarkGenerator(0, 0) }

// unexpectedWatermark is the emitter those tests pass, so "generation is
// disabled" is asserted at every call site rather than assumed.
func unexpectedWatermark(t *testing.T) func(int64) error {
	return func(wm int64) error {
		t.Helper()
		t.Errorf("a source with watermark generation disabled emitted watermark %d", wm)
		return nil
	}
}

// noBarriers is the interval that disables injection, for the tests about the
// range logic or about event time rather than about checkpoints. graph.Validate
// refuses a source configured this way; sourceLoop still has to behave, because
// a loop that divided by the interval unconditionally would panic here rather
// than in the validator where the mistake is.
func noBarriers() int64 { return 0 }

// unexpectedBarrier is the matching emitter.
func unexpectedBarrier(t *testing.T) func(*core.Barrier) error {
	return func(b *core.Barrier) error {
		t.Helper()
		t.Errorf("a source with barrier injection disabled injected checkpoint %d", b.CheckpointID)
		return nil
	}
}

// element is one thing a source subtask put on its outputs, in the order it did
// so.
//
// Records, watermarks and barriers travel in ONE stream (invariant 5), so the
// interleaving is part of what has to be asserted: a harness that collected the
// three into separate lists could not tell a barrier that landed after the
// right number of elements from one that landed anywhere at all. The kind is
// carried rather than a pair of booleans so that "not a watermark" cannot
// quietly come to mean "a record".
type element struct {
	kind      core.ElementKind
	eventTime int64
	// barrier fields, meaningful only when kind is KindBarrier.
	checkpointID int64
	timestamp    int64
}

func (e element) isRecord() bool    { return e.kind == core.KindRecord }
func (e element) isWatermark() bool { return e.kind == core.KindWatermark }
func (e element) isBarrier() bool   { return e.kind == core.KindBarrier }

// runSourceSubtaskLoop drives sourceLoop over a generator and returns the
// merged element stream one subtask produced.
func runSourceSubtaskLoop(t *testing.T, cfg sources.GeneratorConfig, wm watermarkGenerator, barrierInterval int64, parallelism, index int) []element {
	t.Helper()
	src := sources.NewGenerator(cfg)
	if err := src.Open(nil); err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() {
		if err := src.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()

	var out []element
	err := sourceLoop(context.Background(), src, parallelism, index, wm, barrierInterval,
		func(rec *core.Record) error {
			out = append(out, element{kind: core.KindRecord, eventTime: rec.EventTime})
			return nil
		},
		func(t int64) error {
			out = append(out, element{kind: core.KindWatermark, eventTime: t})
			return nil
		},
		func(b *core.Barrier) error {
			out = append(out, element{kind: core.KindBarrier, checkpointID: b.CheckpointID, timestamp: b.Timestamp})
			return nil
		})
	if err != nil {
		t.Fatalf("sourceLoop(p=%d, i=%d): %v", parallelism, index, err)
	}
	return out
}

// TestSourceLoopPutsWatermarksInBandBehindTheirRecords pins the interleaving.
//
// Every watermark must sit immediately after the interval-th record, in the
// same stream, and must be no greater than maxSeen-lag-1 over everything before
// it. A side channel would satisfy neither claim, and invariant 5 exists
// because a watermark that overtook the records it bounds fires their windows
// without them.
func TestSourceLoopPutsWatermarksInBandBehindTheirRecords(t *testing.T) {
	const (
		count    = 200
		interval = 25
		lag      = 50
	)
	cfg := testGeneratorConfig(count)
	cfg.MaxLag = lag

	got := runSourceSubtaskLoop(t, cfg, newWatermarkGenerator(interval, lag), noBarriers(), 1, 0)

	records, watermarks := 0, 0
	maxSeen := int64(math.MinInt64)
	for i, e := range got {
		if e.isRecord() {
			records++
			if e.eventTime > maxSeen {
				maxSeen = e.eventTime
			}
			continue
		}
		watermarks++
		// Position: a watermark follows the interval-th record, so exactly
		// interval records precede it since the previous one.
		if records%interval != 0 {
			t.Errorf("element %d is a watermark after %d records, not a multiple of %d", i, records, interval)
		}
		if want := maxSeen - lag - 1; e.eventTime != want {
			t.Errorf("watermark %d after %d records is %d, want %d", watermarks, records, e.eventTime, want)
		}
	}
	if records != count {
		t.Errorf("the subtask emitted %d records, want %d", records, count)
	}
	if watermarks != count/interval {
		t.Errorf("the subtask emitted %d watermarks, want %d", watermarks, count/interval)
	}
}

// TestSourceLoopReturnsTheWatermarkEmitError checks that a failed watermark
// broadcast stops the subtask rather than being dropped.
//
// A dropped watermark is invisible: the records keep flowing, the downstream
// gate's minimum stops advancing, every window stays open until end of input,
// and the job still produces the right answer at the end. It would only show up
// in Phase 6 as unexplained state growth.
func TestSourceLoopReturnsTheWatermarkEmitError(t *testing.T) {
	errWatermark := errors.New("watermark broadcast failed")

	src := sources.NewGenerator(testGeneratorConfig(100))
	if err := src.Open(nil); err != nil {
		t.Fatalf("Open: %v", err)
	}
	var records int
	err := sourceLoop(context.Background(), src, 1, 0, newWatermarkGenerator(10, 0), noBarriers(),
		func(*core.Record) error {
			records++
			return nil
		},
		func(int64) error { return errWatermark }, unexpectedBarrier(t))
	if !errors.Is(err, errWatermark) {
		t.Fatalf("sourceLoop = %v, want %v", err, errWatermark)
	}
	if records != 10 {
		t.Errorf("sourceLoop read %d records before the first watermark failed, want 10", records)
	}
}

// TestSourceLoopWatermarksStaircaseAcrossSubtasks pins the consequence recorded
// on watermarkGenerator, so that a later change to the offset split shows up
// here rather than as an unexplained number in Phase 6.
//
// Source subtasks take contiguous offset ranges and event time rises with the
// offset, so subtask 0's watermarks are the earliest and subtask P-1's the
// latest, with no overlap between them. The downstream gate takes the minimum,
// so event time advances in a staircase pinned by subtask 0 rather than
// smoothly, and window state peaks near the size of the whole dataset. That is
// correct and is not to be fixed by striding the split.
func TestSourceLoopWatermarksStaircaseAcrossSubtasks(t *testing.T) {
	const (
		count    = 1000
		interval = 50
		p        = 4
	)
	cfg := testGeneratorConfig(count)
	cfg.MaxLag = 0

	var prevMax int64 = math.MinInt64
	for i := range p {
		var got []int64
		for _, e := range runSourceSubtaskLoop(t, cfg, newWatermarkGenerator(interval, 0), noBarriers(), p, i) {
			if e.isWatermark() {
				got = append(got, e.eventTime)
			}
		}
		if len(got) == 0 {
			t.Fatalf("subtask %d emitted no watermark over %d records", i, count/p)
		}
		if got[0] <= prevMax {
			t.Errorf("subtask %d starts at watermark %d, but subtask %d already reached %d: the ranges overlap",
				i, got[0], i-1, prevMax)
		}
		prevMax = got[len(got)-1]
	}
}

// watermarkRecorder collects, per operator instance, the watermarks the runtime
// delivered to it.
type watermarkRecorder struct {
	mu   sync.Mutex
	seen [][]int64
}

// newOperator hands out one recording operator per subtask. The runtime calls
// the factory from each subtask's own goroutine, so allocating the slot is
// locked.
func (r *watermarkRecorder) newOperator() core.Operator {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.seen = append(r.seen, nil)
	// The embedded operator is set explicitly. A decorator that left it nil
	// compiles, satisfies core.Operator, and panics on the first promoted call.
	return &watermarkRecordingOperator{Operator: identity(), rec: r, slot: len(r.seen) - 1}
}

func (r *watermarkRecorder) record(slot int, wm int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.seen[slot] = append(r.seen[slot], wm)
}

type watermarkRecordingOperator struct {
	core.Operator
	rec  *watermarkRecorder
	slot int
}

func (o *watermarkRecordingOperator) ProcessWatermark(wm int64, ctx core.Context) error {
	o.rec.record(o.slot, wm)
	return o.Operator.ProcessWatermark(wm, ctx)
}

// TestRunBroadcastsWatermarksToEveryChannelOnEveryEdge is the invariant 2 claim
// for watermarks, on the only topology where it can fail.
//
// One source feeding two operator vertices at parallelism 2 gives four operator
// subtasks over two edges. A watermark must reach all four. A chain could not
// distinguish broadcast from partition, because with one downstream subtask on
// one edge the two are the same thing; here, partitioning within an edge would
// give each subtask of a vertex half the watermarks, and partitioning across
// edges would give one branch all of them and the other none.
//
// The assertion is the exact sequence at every subtask, not a count. Under a
// partitioning writer each subtask still receives watermarks, just not all of
// them, so anything weaker than equality passes under the bug.
func TestRunBroadcastsWatermarksToEveryChannelOnEveryEdge(t *testing.T) {
	const (
		count    = 400
		interval = 50
		p        = 2
	)
	cfg := testGeneratorConfig(count)

	var left, right watermarkRecorder
	g := buildGraph(t, []graph.Vertex{
		{ID: "src", Kind: graph.VertexSource, Parallelism: 1,
			BarrierIntervalElements:   testBarrierInterval,
			NewSource:                 func() core.Source { return sources.NewGenerator(cfg) },
			WatermarkIntervalElements: interval,
			MaxOutOfOrderness:         cfg.MaxLag},
		{ID: "left", Kind: graph.VertexOperator, Parallelism: p, NewOperator: left.newOperator},
		{ID: "right", Kind: graph.VertexOperator, Parallelism: p, NewOperator: right.newOperator},
		{ID: "out", Kind: graph.VertexSink, Parallelism: 1,
			NewSink: func() core.Sink { return sinks.NewDiscard() }},
	}, [][2]string{{"src", "left"}, {"src", "right"}, {"left", "out"}, {"right", "out"}})

	if err := Run(context.Background(), g); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// The source is at parallelism 1, so what it emitted is exactly what one
	// subtask's generator produces over the whole input.
	var want []int64
	for _, e := range runSourceSubtaskLoop(t, cfg, newWatermarkGenerator(interval, cfg.MaxLag), noBarriers(), 1, 0) {
		if e.isWatermark() {
			want = append(want, e.eventTime)
		}
	}
	if len(want) == 0 {
		t.Fatal("the generator produced no watermark; the test asserts nothing")
	}
	// Each operator subtask also sees its own gate's end-of-input flush, which
	// no source emits. It is appended here rather than tolerated by a looser
	// comparison, so this test still pins the exact sequence.
	want = append(want, math.MaxInt64)

	for _, branch := range []struct {
		name string
		rec  *watermarkRecorder
	}{{"left", &left}, {"right", &right}} {
		if len(branch.rec.seen) != p {
			t.Fatalf("%s built %d operators, want %d", branch.name, len(branch.rec.seen), p)
		}
		for i, got := range branch.rec.seen {
			if !slices.Equal(got, want) {
				t.Errorf("%s[%d] saw watermarks %v, want every one of %v", branch.name, i, got, want)
			}
		}
	}
}

// barriersOf reduces an element stream to the barriers in it.
func barriersOf(elems []element) []element {
	var out []element
	for _, e := range elems {
		if e.isBarrier() {
			out = append(out, e)
		}
	}
	return out
}

// TestMaxBarriersIsTheSameForEverySubtask is invariant 3's deadlock guard,
// stated on the arithmetic before any stream exists.
//
// Contiguous ranges are equal in length only up to integer division. Every
// subtask must inject the SAME number of barriers, because a downstream
// operator aligning on barrier k waits for one from every live input and there
// is no timeout: a subtask that never sends barrier k stops the job with no
// error anywhere. The budget therefore comes from the floor of count/P, which
// every range is at least as long as.
func TestMaxBarriersIsTheSameForEverySubtask(t *testing.T) {
	tests := []struct {
		name        string
		count       int64
		parallelism int
		interval    int64
		want        int64
	}{
		{name: "exact division", count: 1000, parallelism: 4, interval: 50, want: 5},
		// Ranges of 2, 3, 2, 3. The floor is 2, so nobody injects at all.
		{name: "remainder below the interval", count: 10, parallelism: 4, interval: 5, want: 0},
		// Ranges of 250, 251, 250, 250. Subtask 1 reaches its 250th element
		// like the rest and stops there; its 251st carries no barrier.
		{name: "remainder above the interval", count: 1001, parallelism: 4, interval: 100, want: 2},
		{name: "one subtask", count: 1000, parallelism: 1, interval: 100, want: 10},
		{name: "interval longer than the input", count: 10, parallelism: 1, interval: 100, want: 0},
		{name: "empty input", count: 0, parallelism: 4, interval: 10, want: 0},
		{name: "more subtasks than records", count: 3, parallelism: 8, interval: 1, want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := maxBarriers(tt.count, tt.parallelism, tt.interval, true)
			if got != tt.want {
				t.Fatalf("maxBarriers(count=%d, p=%d, interval=%d) = %d, want %d",
					tt.count, tt.parallelism, tt.interval, got, tt.want)
			}
			// Every subtask's range is at least as long as the budget needs,
			// which is the property the number is chosen for.
			for i := range tt.parallelism {
				start, end := sourceRange(tt.count, tt.parallelism, i)
				if need := got * tt.interval; end-start < need {
					t.Errorf("subtask %d holds %d elements but is asked for %d barriers, which needs %d",
						i, end-start, got, need)
				}
			}
		})
	}

	// A source with no Count is uncapped. seekToRange refuses such a source
	// above parallelism 1, so there is no second subtask to agree with.
	if got := maxBarriers(0, 1, 10, false); got != unboundedBarriers {
		t.Errorf("maxBarriers for an unbounded source = %d, want %d", got, unboundedBarriers)
	}
}

// TestEverySourceSubtaskInjectsTheSameBarrierCount is the same claim through
// the loop rather than the arithmetic, at three parallelisms.
//
// The count that matters is per SUBTASK and not per vertex: a vertex total
// would be satisfied by one subtask injecting all of them, which is exactly the
// arrangement that deadlocks alignment.
func TestEverySourceSubtaskInjectsTheSameBarrierCount(t *testing.T) {
	const interval = 25
	// 1001 is deliberately not a multiple of any parallelism below, so the
	// ranges differ in length in every row.
	counts := []int64{1001, 1000, 37}

	for _, count := range counts {
		cfg := testGeneratorConfig(count)
		for _, p := range []int{1, 2, 4} {
			t.Run(fmt.Sprintf("count%d/p%d", count, p), func(t *testing.T) {
				want := maxBarriers(count, p, interval, true)
				for i := range p {
					got := barriersOf(runSourceSubtaskLoop(t, cfg, noWatermarks(), interval, p, i))
					if int64(len(got)) != want {
						t.Errorf("subtask %d of %d injected %d barriers, want %d: a downstream operator aligning on the last one waits forever",
							i, p, len(got), want)
					}
				}
			})
		}
	}
}

// TestBarriersLandAtTheSpecifiedElementPositions pins where a barrier sits in
// the stream, which is what invariants 3 and 6 are about.
//
// Barrier k follows the k*interval-th element of the SUBTASK's own range, in
// the same stream as the records (invariant 5). Position, not count: a
// generator that injected the right number of barriers at the wrong places
// would make a recovered run replay from a different point, and the only
// symptom is a diff in the output that looks like a windowing bug.
func TestBarriersLandAtTheSpecifiedElementPositions(t *testing.T) {
	const (
		count    = 400
		interval = 25
		p        = 2
	)
	cfg := testGeneratorConfig(count)

	for i := range p {
		got := runSourceSubtaskLoop(t, cfg, noWatermarks(), interval, p, i)

		records, barriers := 0, 0
		for j, e := range got {
			if e.isRecord() {
				records++
				continue
			}
			if !e.isBarrier() {
				t.Fatalf("subtask %d element %d is a %s, but watermarks are disabled here", i, j, e.kind)
			}
			barriers++
			if records != barriers*interval {
				t.Errorf("subtask %d: barrier %d follows %d records, want %d",
					i, barriers, records, barriers*interval)
			}
		}
		start, end := sourceRange(count, p, i)
		if int64(records) != end-start {
			t.Errorf("subtask %d emitted %d records, want its range of %d", i, records, end-start)
		}
		if want := maxBarriers(count, p, interval, true); int64(barriers) != want {
			t.Fatalf("subtask %d injected %d barriers, want %d", i, barriers, want)
		}
	}
}

// TestCheckpointIDsAreContiguousFromOne pins the numbering.
//
// A checkpoint coordinator in Phase 3b matches a barrier to the checkpoint it
// belongs to by ID. An ID that started at zero would collide with the zero
// value of an int64 field, and a gap would leave a checkpoint that every
// subtask waits on and none ever announces.
func TestCheckpointIDsAreContiguousFromOne(t *testing.T) {
	const (
		count    = 500
		interval = 10
		p        = 2
	)
	cfg := testGeneratorConfig(count)

	for i := range p {
		got := barriersOf(runSourceSubtaskLoop(t, cfg, noWatermarks(), interval, p, i))
		if len(got) == 0 {
			t.Fatalf("subtask %d injected no barrier; the test asserts nothing", i)
		}
		for k, e := range got {
			if want := int64(k + 1); e.checkpointID != want {
				t.Fatalf("subtask %d barrier %d carries checkpoint ID %d, want %d",
					i, k, e.checkpointID, want)
			}
		}
	}
}

// TestBarrierTimestampIsTheLastWatermarkAndNotAClock pins the metadata field,
// which exists to be looked at and not to be computed from.
//
// It carries the watermark that source subtask most recently emitted, which is
// a function of (seed, offset) and therefore the same on a replay. A wall clock
// there would be different on every run and would quietly make a snapshot
// comparison in Phase 3b fail for a reason that has nothing to do with state.
//
// Before the subtask's first watermark there is nothing to carry, and the
// generator's initial value stands: MinInt64 says "no watermark yet" rather
// than claiming 1970.
func TestBarrierTimestampIsTheLastWatermarkAndNotAClock(t *testing.T) {
	const (
		count             = 300
		barrierInterval   = 20
		watermarkInterval = 50
	)
	cfg := testGeneratorConfig(count)

	got := runSourceSubtaskLoop(t, cfg, newWatermarkGenerator(watermarkInterval, cfg.MaxLag), barrierInterval, 1, 0)

	lastWatermark := int64(math.MinInt64)
	barriers, afterFirstWatermark := 0, 0
	for _, e := range got {
		switch {
		case e.isWatermark():
			lastWatermark = e.eventTime
		case e.isBarrier():
			barriers++
			if e.timestamp != lastWatermark {
				t.Errorf("barrier %d carries timestamp %d, want the last emitted watermark %d",
					e.checkpointID, e.timestamp, lastWatermark)
			}
			if lastWatermark != math.MinInt64 {
				afterFirstWatermark++
			}
		}
	}
	if barriers == 0 {
		t.Fatal("no barrier was injected; the test asserts nothing")
	}
	if afterFirstWatermark == 0 {
		t.Error("every barrier arrived before the first watermark, so the timestamp was never compared against a real value")
	}

	// The same subtask replayed produces the same timestamps. That is the whole
	// claim: it is deterministic, which a clock is not.
	again := barriersOf(runSourceSubtaskLoop(t, cfg, newWatermarkGenerator(watermarkInterval, cfg.MaxLag), barrierInterval, 1, 0))
	first := barriersOf(got)
	if len(again) != len(first) {
		t.Fatalf("a replay injected %d barriers, want %d", len(again), len(first))
	}
	for k := range first {
		if again[k].timestamp != first[k].timestamp {
			t.Errorf("barrier %d carried timestamp %d on one run and %d on a replay",
				first[k].checkpointID, first[k].timestamp, again[k].timestamp)
		}
	}
}

// TestSourceSubtaskBroadcastsBarriersToEveryChannel is the invariant 2 claim
// for barriers, on the only shape where it can fail.
//
// Two downstream vertices, each at parallelism 2, gives one source subtask four
// output channels in two groups. A barrier must reach all four. A record
// partitions WITHIN a group and copies ACROSS groups; a barrier does neither,
// it goes everywhere, and the two mistakes look different: partitioning within
// a group sends barrier k to one subtask of a vertex, and partitioning across
// groups sends it to one branch. Either way some subtask aligns on a checkpoint
// whose barrier never arrives, alignment has no timeout, and the job stops
// producing output with nothing to point at.
//
// The assertion is the exact sequence of checkpoint IDs on every channel, not a
// count: under a partitioning writer each channel still receives barriers, just
// not all of them.
//
// A chain cannot state this. With one downstream vertex at parallelism 1 there
// is one channel, and broadcast and partition are the same thing.
func TestSourceSubtaskBroadcastsBarriersToEveryChannel(t *testing.T) {
	const (
		count    = 400
		interval = 50
		width    = 2
	)
	cfg := testGeneratorConfig(count)

	// Two groups of two: one group per downstream vertex, one channel per
	// subtask of it. This is the shape wire() builds for a source feeding two
	// operator vertices at parallelism 2.
	groups := make([][]transport.Output, 2)
	chans := make([][]*transport.Channel, len(groups))
	for gi := range groups {
		for range width {
			ch := transport.NewChannel(transport.DefaultCapacity)
			chans[gi] = append(chans[gi], ch)
			groups[gi] = append(groups[gi], ch)
		}
	}

	v := graph.Vertex{
		ID: "src", Kind: graph.VertexSource, Parallelism: 1,
		NewSource:                 func() core.Source { return sources.NewGenerator(cfg) },
		WatermarkIntervalElements: testWatermarkInterval,
		MaxOutOfOrderness:         cfg.MaxLag,
		BarrierIntervalElements:   interval,
	}

	// Drained concurrently: the subtask blocks once a channel fills, and it
	// closes every output on the way out, which is what ends each drain.
	type drained struct {
		barriers []int64
		records  int
	}
	results := make([][]drained, len(chans))
	var wg sync.WaitGroup
	for gi := range chans {
		results[gi] = make([]drained, len(chans[gi]))
		for ci, ch := range chans[gi] {
			wg.Add(1)
			go func() {
				defer wg.Done()
				var d drained
				for {
					e, ok := ch.Recv()
					if !ok {
						results[gi][ci] = d
						return
					}
					switch e.Kind {
					case core.KindRecord:
						d.records++
					case core.KindBarrier:
						d.barriers = append(d.barriers, e.Barrier.CheckpointID)
					}
				}
			}()
		}
	}

	if err := runSubtask(context.Background(), v, 0, nil, groups); err != nil {
		t.Fatalf("runSubtask: %v", err)
	}
	wg.Wait()

	var want []int64
	for k := int64(1); k <= maxBarriers(count, 1, interval, true); k++ {
		want = append(want, k)
	}
	if len(want) == 0 {
		t.Fatal("the source injected no barrier; the test asserts nothing")
	}

	for gi := range results {
		for ci, d := range results[gi] {
			if !slices.Equal(d.barriers, want) {
				t.Errorf("group %d channel %d saw checkpoints %v, want every one of %v", gi, ci, d.barriers, want)
			}
		}
	}

	// The records did NOT go everywhere, which is what says the assertion above
	// is about broadcast rather than about the writer sending everything to
	// every channel. Each group receives the whole stream, partitioned across
	// its channels.
	for gi := range results {
		total := 0
		for ci, d := range results[gi] {
			if d.records == 0 {
				t.Errorf("group %d channel %d received no record at all", gi, ci)
			}
			if d.records == count {
				t.Errorf("group %d channel %d received all %d records; they are meant to partition within a group", gi, ci, count)
			}
			total += d.records
		}
		if total != count {
			t.Errorf("group %d received %d records in total, want %d", gi, total, count)
		}
	}
}
