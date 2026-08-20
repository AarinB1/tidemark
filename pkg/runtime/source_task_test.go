package runtime

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/AarinB1/tidemark/pkg/core"
	"github.com/AarinB1/tidemark/pkg/graph"
	"github.com/AarinB1/tidemark/pkg/sinks"
	"github.com/AarinB1/tidemark/pkg/sources"
)

// parallelismLevels is the set every equivalence assertion in Phase 1 runs at.
var parallelismLevels = []int{1, 2, 4, 8}

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
	err := sourceLoop(context.Background(), &countlessSource{limit: 10}, 2, 0, func(*core.Record) error { return nil })
	if err == nil {
		t.Fatal("sourceLoop split a source that does not report a Count")
	}

	// At parallelism 1 there is nothing to divide, so the same source is read
	// to exhaustion.
	var seen int
	if err := sourceLoop(context.Background(), &countlessSource{limit: 10}, 1, 0, func(*core.Record) error {
		seen++
		return nil
	}); err != nil {
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
	err := sourceLoop(ctx, src, 1, 0, func(*core.Record) error {
		t.Error("sourceLoop emitted a record against a cancelled context")
		return nil
	})
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
	err := sourceLoop(context.Background(), src, 1, 0, func(*core.Record) error {
		seen++
		if seen == 3 {
			return errEmit
		}
		return nil
	})
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
	err := sourceLoop(context.Background(), src, parallelism, index, func(rec *core.Record) error {
		out = append(out, rec)
		return nil
	})
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
				{ID: "src", Kind: graph.VertexSource, Parallelism: p, NewSource: tt.newSource},
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
