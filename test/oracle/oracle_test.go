package oracle

import (
	"slices"
	"strings"
	"testing"

	"github.com/AarinB1/tidemark/pkg/core"
	"github.com/AarinB1/tidemark/pkg/sources"
)

// fixture is the hand-written input every expectation below was computed from
// with a pen. Twenty records over three keys, chosen to land on both sides of
// every boundary that matters: the first millisecond of a window, its last, the
// first of the next, and the same three below zero, where Go's truncated
// remainder disagrees with the floored one.
//
// The expected counts are written out by hand and by nothing else. This is the
// one file in the repository where that has to be true: the oracle is what
// every other answer is checked against, so an expectation computed by any code
// here would only prove the code agrees with itself.
var fixture = []struct {
	key       string
	eventTime int64
}{
	{"a", 0},    // 1
	{"a", 1},    // 2
	{"a", 99},   // 3
	{"a", 100},  // 4
	{"a", 199},  // 5
	{"a", 200},  // 6
	{"b", 0},    // 7
	{"b", 50},   // 8
	{"b", 150},  // 9
	{"b", 250},  // 10
	{"b", 250},  // 11
	{"c", -1},   // 12
	{"c", -100}, // 13
	{"c", -101}, // 14
	{"c", 0},    // 15
	{"a", -1},   // 16
	{"a", 250},  // 17
	{"b", -250}, // 18
	{"c", 99},   // 19
	{"c", 100},  // 20
}

func fixtureRecords() []*core.Record {
	out := make([]*core.Record, len(fixture))
	for i, f := range fixture {
		out[i] = &core.Record{Key: []byte(f.key), Value: []byte("v"), EventTime: f.eventTime}
	}
	return out
}

// TestOracleAgainstAHandComputedFixture is the base of the whole phase.
//
// Tumbling, size 100. Worked through by hand, record by record:
//
//	a: -1 in [-100, 0); 0, 1, 99 in [0, 100); 100, 199 in [100, 200);
//	   200, 250 in [200, 300)
//	b: -250 in [-300, -200); 0, 50 in [0, 100); 150 in [100, 200);
//	   250, 250 in [200, 300)
//	c: -101 in [-200, -100); -100, -1 in [-100, 0); 0, 99 in [0, 100);
//	   100 in [100, 200)
//
// The rows below zero are the ones a truncated remainder gets wrong. -1 belongs
// to [-100, 0) and -101 to [-200, -100); computed as eventTime - eventTime%100
// they land in [0, 100) and [-100, 0) instead, one window too high in each
// case. Every timestamp in a realistic test is positive, where the two agree.
//
// Sliding, size 100, slide 50, is the same twenty records against the two
// windows each of them falls in, worked out the same way.
func TestOracleAgainstAHandComputedFixture(t *testing.T) {
	tests := []struct {
		name string
		spec Spec
		want []Triple
	}{
		{
			name: "tumbling 100",
			spec: Spec{Size: 100, Slide: 100},
			want: []Triple{
				{"a", -100, 1},
				{"a", 0, 3},
				{"a", 100, 2},
				{"a", 200, 2},
				{"b", -300, 1},
				{"b", 0, 2},
				{"b", 100, 1},
				{"b", 200, 2},
				{"c", -200, 1},
				{"c", -100, 2},
				{"c", 0, 2},
				{"c", 100, 1},
			},
		},
		{
			name: "sliding 100 by 50",
			spec: Spec{Size: 100, Slide: 50},
			want: []Triple{
				{"a", -100, 1},
				{"a", -50, 3},
				{"a", 0, 3},
				{"a", 50, 2},
				{"a", 100, 2},
				{"a", 150, 2},
				{"a", 200, 2},
				{"a", 250, 1},
				{"b", -300, 1},
				{"b", -250, 1},
				{"b", -50, 1},
				{"b", 0, 2},
				{"b", 50, 1},
				{"b", 100, 1},
				{"b", 150, 1},
				{"b", 200, 2},
				{"b", 250, 2},
				{"c", -200, 1},
				{"c", -150, 2},
				{"c", -100, 2},
				{"c", -50, 2},
				{"c", 0, 2},
				{"c", 50, 2},
				{"c", 100, 1},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			counts, err := countRecords(fixtureRecords(), tt.spec)
			if err != nil {
				t.Fatalf("countRecords: %v", err)
			}
			got := Sorted(counts)
			if !slices.Equal(got, tt.want) {
				t.Fatalf("got %d rows, want %d\n got: %v\nwant: %v", len(got), len(tt.want), got, tt.want)
			}

			// Every record contributes to exactly size/slide windows, so the
			// counts must sum to that multiple of the input. This catches a
			// whole class of assignment error the row-by-row comparison would
			// only catch if the fixture happened to cover it.
			var total int64
			for _, tr := range got {
				total += tr.Count
			}
			if want := int64(len(fixture)) * tt.spec.Size / tt.spec.Slide; total != want {
				t.Errorf("the counts sum to %d, want %d", total, want)
			}
		})
	}
}

// TestOracleIsIndependentOfInputOrder pins the claim that there is no sort and
// no ordering assumption.
//
// The engine reads its input through P subtasks and a shuffle, so records reach
// an aggregate in an order that varies between runs. An oracle that quietly
// depended on order would disagree with the engine at parallelism 4 and agree
// at 1, which reads like a runtime bug.
func TestOracleIsIndependentOfInputOrder(t *testing.T) {
	spec := Spec{Size: 100, Slide: 25}
	recs := fixtureRecords()

	want, err := countRecords(recs, spec)
	if err != nil {
		t.Fatalf("countRecords: %v", err)
	}

	reversed := slices.Clone(recs)
	slices.Reverse(reversed)
	got, err := countRecords(reversed, spec)
	if err != nil {
		t.Fatalf("countRecords reversed: %v", err)
	}
	if !slices.Equal(Sorted(got), Sorted(want)) {
		t.Error("the oracle gave a different answer for the same records in the opposite order")
	}

	// And an interleaving that is neither, so the claim is not just about
	// reversal.
	shuffled := make([]*core.Record, 0, len(recs))
	for i := range recs {
		shuffled = append(shuffled, recs[(i*7)%len(recs)])
	}
	got, err = countRecords(shuffled, spec)
	if err != nil {
		t.Fatalf("countRecords shuffled: %v", err)
	}
	if !slices.Equal(Sorted(got), Sorted(want)) {
		t.Error("the oracle gave a different answer for the same records interleaved")
	}
}

// TestCountsMatchesCountRecordsOverTheSameInput bridges the two entry points.
//
// The hand fixture above proves the assignment and the accumulation. This
// proves that Counts, which regenerates the input rather than being handed it,
// reads exactly the records the generator produces and feeds them through the
// same path. Without it the exported function everything else calls would be
// untested.
func TestCountsMatchesCountRecordsOverTheSameInput(t *testing.T) {
	specs := []Spec{
		{Size: 1000, Slide: 1000},
		{Size: 1000, Slide: 250},
	}
	cfgs := []sources.GeneratorConfig{
		testConfig(1, 500),
		testConfig(2, 5000),
		// A single key, so every record lands in the same windows and a
		// mis-keyed lookup cannot hide behind spread.
		func() sources.GeneratorConfig {
			c := testConfig(3, 1000)
			c.KeyCardinality = 1
			return c
		}(),
		// Event times that run below zero, which is where the floored
		// remainder matters.
		func() sources.GeneratorConfig {
			c := testConfig(4, 1000)
			c.BaseEventTime = -5000
			return c
		}(),
	}

	for _, spec := range specs {
		for _, cfg := range cfgs {
			want, err := Counts(cfg, spec)
			if err != nil {
				t.Fatalf("Counts: %v", err)
			}
			got, err := countRecords(readAll(t, cfg), spec)
			if err != nil {
				t.Fatalf("countRecords: %v", err)
			}
			if !slices.Equal(Sorted(got), Sorted(want)) {
				t.Errorf("seed %d size %d slide %d: Counts and countRecords disagree",
					cfg.Seed, spec.Size, spec.Slide)
			}
			if len(want) == 0 {
				t.Errorf("seed %d size %d slide %d produced no windows", cfg.Seed, spec.Size, spec.Slide)
			}
		}
	}
}

// TestCountsSumsToTheInput is the invariant that holds for any input at all:
// every record contributes exactly size/slide counts, so nothing is dropped and
// nothing is counted twice.
func TestCountsSumsToTheInput(t *testing.T) {
	for _, spec := range []Spec{
		{Size: 1000, Slide: 1000},
		{Size: 1000, Slide: 500},
		{Size: 1000, Slide: 100},
	} {
		cfg := testConfig(9, 3000)
		counts, err := Counts(cfg, spec)
		if err != nil {
			t.Fatalf("Counts: %v", err)
		}
		var total int64
		for _, n := range counts {
			total += n
		}
		if want := cfg.Count * spec.Size / spec.Slide; total != want {
			t.Errorf("size %d slide %d: counts sum to %d, want %d", spec.Size, spec.Slide, total, want)
		}
	}
}

// TestSpecIsChecked mirrors the window operator's constructor. A specification
// the engine refuses must not silently produce an oracle answer to compare
// against, because the comparison would then be against a computation the
// engine never performs.
func TestSpecIsChecked(t *testing.T) {
	tests := []struct {
		name string
		spec Spec
		want string
	}{
		{name: "tumbling", spec: Spec{Size: 100, Slide: 100}},
		{name: "size a multiple of slide", spec: Spec{Size: 100, Slide: 20}},
		{name: "size not a multiple of slide", spec: Spec{Size: 100, Slide: 30}, want: "not a multiple"},
		{name: "slide larger than size", spec: Spec{Size: 100, Slide: 150}, want: "not a multiple"},
		{name: "zero size", spec: Spec{Size: 0, Slide: 100}, want: "size"},
		{name: "negative size", spec: Spec{Size: -100, Slide: 100}, want: "size"},
		{name: "zero slide", spec: Spec{Size: 100, Slide: 0}, want: "slide"},
		{name: "negative slide", spec: Spec{Size: 100, Slide: -50}, want: "slide"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Counts(testConfig(1, 10), tt.spec)
			switch {
			case tt.want == "" && err != nil:
				t.Fatalf("Counts = %v, want the specification accepted", err)
			case tt.want != "" && err == nil:
				t.Fatalf("Counts accepted size %d slide %d", tt.spec.Size, tt.spec.Slide)
			case tt.want != "" && !strings.Contains(err.Error(), tt.want):
				t.Errorf("error %q does not name %q", err, tt.want)
			}
		})
	}
}

// TestCompareTriplesOrdersByKeyThenWindowThenCount pins the comparison the
// equivalence tests report their first difference through.
func TestCompareTriplesOrdersByKeyThenWindowThenCount(t *testing.T) {
	in := []Triple{
		{"b", 0, 1},
		{"a", 100, 1},
		{"a", 0, 2},
		{"a", 0, 1},
		{"a", -100, 9},
	}
	want := []Triple{
		{"a", -100, 9},
		{"a", 0, 1},
		{"a", 0, 2},
		{"a", 100, 1},
		{"b", 0, 1},
	}
	got := slices.Clone(in)
	slices.SortFunc(got, CompareTriples)
	if !slices.Equal(got, want) {
		t.Errorf("sorted to %v, want %v", got, want)
	}
}

// testConfig is a generator the oracle can read. MaxLag is non-zero so the
// input is genuinely out of order, which is what makes the no-sort claim worth
// something.
func testConfig(seed uint64, count int64) sources.GeneratorConfig {
	return sources.GeneratorConfig{
		Seed:           seed,
		Count:          count,
		KeyCardinality: 8,
		BaseEventTime:  1700000000000,
		EventTimeStep:  10,
		MaxLag:         200,
		ValueSize:      8,
	}
}

// readAll drains a generator into a slice, for the one test that needs both
// entry points over the same records.
func readAll(t *testing.T, cfg sources.GeneratorConfig) []*core.Record {
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
	for {
		rec, ok, err := src.Next()
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if !ok {
			return out
		}
		out = append(out, rec)
	}
}
