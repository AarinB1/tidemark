package chaos

import (
	"flag"
	"fmt"
	"strings"
	"sync"
	"testing"
)

// seeds is how many schedules the suite runs, and it is contiguous from 1 on
// purpose.
//
// Contiguous and starting at one so that a failure names a seed somebody can
// rerun directly:
//
//	go test ./test/chaos -run 'TestChaosSuite/all/seed137' -count=1
//
// The entire design of this phase exists so that one integer reproduces a
// failure. A suite that drew its seeds from a clock, or from a hash of the
// build, would report a number that means nothing on the machine reading it.
//
// The default is the twenty-five-schedule subset rather than the full five
// hundred, because `go test ./...` runs this package under -race as part of
// `make check` before every commit. The race detector costs five to twenty
// times, and five hundred schedules under it fit no reasonable budget. The
// Makefile's chaos target raises it; see the note there on the two shapes.
//
// The subset is a SUBSET and not a sample: the schedules are a pure function of
// the seeds and the seeds are the same twenty-five every time, so the census
// floors that hold on five hundred hold on twenty-five for a reason rather than
// by luck. Both were measured; see SuiteFloor.
var seeds = flag.Int("chaos.seeds", 25, "how many contiguous seeds, from 1, the chaos suite runs")

// TestChaosSuite runs one schedule per seed and holds the census and the
// timing aggregate to their floors.
//
// # Concurrency
//
// Schedules run in parallel, capped by -test.parallel, which defaults to
// GOMAXPROCS. The concurrency is ACROSS schedules and never within one: each
// subtest builds its own graph, its own sinks, its own checkpoint root and its
// own injector, and shares nothing mutable with any other. What the two do
// share is the batch oracle and the graph the schedules are drawn against, both
// of which are computed once and never written to again.
//
// A job is itself concurrent and this does not pretend otherwise. What a seed
// fixes is the SCHEDULE -- which subtask is aborted, and at which logical
// position -- and not the order two inputs of a gate happen to deliver their
// barriers. Whether a fault lands is therefore still the Go scheduler's to
// decide, and the census is precisely the measurement of how often it did.
//
// # What is asserted
//
// Every schedule compares its sink against the batch oracle, which is the
// divergence check and the headline. The census floor and the timing baseline
// are what stop a green suite from being a green suite that reached nowhere.
func TestChaosSuite(t *testing.T) {
	requested := *seeds
	if requested < 1 {
		t.Fatalf("-chaos.seeds is %d; the suite has nothing to run", requested)
	}

	var mu sync.Mutex
	results := make([]Result, 0, requested)

	// The inner t.Run does not return until every parallel child has finished,
	// which is what makes the aggregation below safe without a WaitGroup.
	t.Run("all", func(t *testing.T) {
		for seed := int64(1); seed <= int64(requested); seed++ {
			name := fmt.Sprintf("seed%d", seed)
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				// The subtest's NAME is the reproduction instruction, so it is
				// asserted rather than assumed. A suite that ran its schedules
				// in one flat test, or named them by their position in a slice
				// the scheduler reordered, would report a failure nobody could
				// rerun -- and from the outside it would look exactly like this
				// one.
				if !strings.HasSuffix(t.Name(), "/"+name) {
					t.Fatalf("this schedule runs as %q, which does not end in %q: a failure here "+
						"would not say which seed to rerun", t.Name(), name)
				}
				res := RunSchedule(t, seed)
				mu.Lock()
				results = append(results, res)
				mu.Unlock()
			})
		}
	})

	// Reported before the floors are checked. A run that failed a floor is a
	// run whose census is the thing to read, and printing it after a t.Fatalf
	// would print it never.
	var census Census
	var timing TimingAggregate
	for _, res := range results {
		census.Add(res)
		timing.AddTiming(res)
	}
	t.Log(census.Table())
	t.Log(timing.Table())

	if err := census.Check(SuiteFloor(requested)); err != nil {
		t.Errorf("%v\n\nzero divergence over %d schedules says nothing on its own; it says something "+
			"in proportion to how many of them put a fault somewhere a bug could have surfaced", err, requested)
	}
	if err := timing.Check(SuiteTimingBaseline(requested)); err != nil {
		t.Errorf("%v\n\nthe batch oracle is blind to both of these: it compares contents, and the "+
			"end-of-input flush fixes the contents up however late the watermark was", err)
	}
}
