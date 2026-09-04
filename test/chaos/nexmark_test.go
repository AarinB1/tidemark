package chaos

import (
	"flag"
	"fmt"
	"strings"
	"sync"
	"testing"
)

// nexmarkSeeds is how many schedules the Nexmark suite runs per query,
// contiguous from 1 so that a failure names a seed somebody can rerun directly:
//
//	go test ./test/chaos -run 'TestNexmarkChaosSuite/q5/seed137' -count=1
//
// The default is small because there are FIVE queries behind it and `go test
// ./...` runs this package under -race as part of `make check` before every
// commit. The full five hundred runs WITHOUT the race detector:
//
//	go test ./test/chaos -run TestNexmarkChaosSuite -count=1 -v \
//		-chaos.nexmark.seeds=500 -timeout 30m
//
// The subset is a SUBSET and not a sample: the schedules are a pure function of
// the seeds and the seeds are the same ten every time, so a census fraction
// that holds on ten holds on ten for a reason rather than by luck. Both were
// measured; see the phase report.
var nexmarkSeeds = flag.Int("chaos.nexmark.seeds", 10,
	"how many contiguous seeds, from 1, the Nexmark chaos suite runs per query")

// TestNexmarkChaosSuite runs the seeded fault schedules against each of the
// five queries and holds each query's census to its floor.
//
// # What is asserted
//
// Every schedule compares its COMMITTED sink against the batch oracle for its
// query, which is the divergence check and the headline, and asserts
// exactly-once multiplicity while it is there. The census floors are what stop
// a green suite from being a green suite that reached nowhere.
//
// # The census is per query, not pooled
//
// The five queries have different topologies, so a seed draws a different
// schedule for each and the faults land in different places. q5 in particular
// has four operator vertices to q0's one, so it holds more state and its faults
// have more places to be. Pooling the five would average that away and report a
// number belonging to no workload.
func TestNexmarkChaosSuite(t *testing.T) {
	requested := *nexmarkSeeds
	if requested < 1 {
		t.Fatalf("-chaos.nexmark.seeds is %d; the suite has nothing to run", requested)
	}

	for _, query := range nexQueries {
		t.Run(query, func(t *testing.T) {
			var mu sync.Mutex
			results := make([]Result, 0, requested)

			// The inner t.Run does not return until every parallel child has
			// finished, which is what makes the aggregation below safe without
			// a WaitGroup.
			t.Run("all", func(t *testing.T) {
				for seed := int64(1); seed <= int64(requested); seed++ {
					name := fmt.Sprintf("seed%d", seed)
					t.Run(name, func(t *testing.T) {
						t.Parallel()
						// The subtest's NAME is the reproduction instruction,
						// so it is asserted rather than assumed.
						if !strings.HasSuffix(t.Name(), "/"+name) {
							t.Fatalf("this schedule runs as %q, which does not end in %q: a failure here "+
								"would not say which seed to rerun", t.Name(), name)
						}
						res := RunNexmarkSchedule(t, seed, query)
						mu.Lock()
						results = append(results, res)
						mu.Unlock()
					})
				}
			})

			var census Census
			for _, res := range results {
				census.Add(res)
			}
			// Reported before the floor is checked: a run that failed a floor
			// is a run whose census is the thing to read, and printing it after
			// a Fatalf would print it never.
			t.Log(query + census.Table())

			if err := census.Check(NexmarkFloor(requested, query)); err != nil {
				t.Errorf("%s: %v\n\nzero divergence over %d schedules says nothing on its own; it says "+
					"something in proportion to how many of them put a fault somewhere a bug could have surfaced",
					query, err, requested)
			}
			// A query with no windows must report EXACTLY zero pending windows.
			// Its floor drops that check, so this is what stops the drop from
			// hiding a census that silently stopped counting.
			if len(nexTimerVertices(query)) == 0 && census.PendingWindowsTotal != 0 {
				t.Errorf("%s holds no event-time timers and the census counted %d pending windows",
					query, census.PendingWindowsTotal)
			}
		})
	}
}

// NexmarkFloor is the floor one query's census is held to.
//
// The four fractions that describe the SCHEDULE -- how often a fault fired, how
// often a schedule aborted, how often a resume came from a real checkpoint, how
// often an alignment fault landed inside a window -- are the Phase 5 numbers
// unchanged. They are properties of the fault machinery and the barrier skew,
// both of which this workload keeps, so lowering them for a new workload would
// be lowering the bar rather than measuring it.
//
// The pending-window fraction is the one that differs, and only for the queries
// that have no windows. q0, q1 and q2 hold no state at all: there is no timer
// to lose across a checkpoint and therefore nothing for that fraction to
// measure. Dropping it there is not a floor lowered, it is a floor that does
// not apply -- and the suite asserts separately that those queries report
// exactly zero, so the drop cannot hide a census that stopped counting.
//
// For q5 and q7 the full Phase 5 floor applies, including the pending-window
// one. See the phase report for the measured numbers; if one of them ever stops
// holding for a query, the thing to do is report it, not to lower it here.
func NexmarkFloor(schedules int, query string) Floor {
	f := SuiteFloor(schedules)
	if len(nexTimerVertices(query)) == 0 {
		f.PendingWindowFraction = 0
	}
	return f
}
