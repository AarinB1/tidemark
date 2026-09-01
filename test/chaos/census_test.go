package chaos

import (
	"strings"
	"testing"
)

// TestCensusGuardRefusesACensusThatWasNotPopulated.
//
// This is the assertion that exists because of the Phase 3c WalkDir scan: a
// census that recorded nothing satisfies no fraction, and every floor
// comparison over it would be a comparison against zero out of zero. The guard
// has to fire on that before any fraction is looked at, or the suite reports
// five failures with one cause -- or, worse, passes because a zero denominator
// was read as "nothing to complain about".
func TestCensusGuardRefusesACensusThatWasNotPopulated(t *testing.T) {
	for _, tc := range []struct {
		name    string
		census  Census
		floor   Floor
		wantSub string
	}{
		{
			name:    "empty census against a suite that ran 500",
			census:  Census{},
			floor:   Floor{Schedules: 500},
			wantSub: "examined 0 schedules and the suite ran 500",
		},
		{
			name:    "census short of the suite",
			census:  Census{Schedules: 499},
			floor:   Floor{Schedules: 500},
			wantSub: "examined 499 schedules and the suite ran 500",
		},
		{
			name:    "census ahead of the suite",
			census:  Census{Schedules: 501},
			floor:   Floor{Schedules: 500},
			wantSub: "examined 501 schedules and the suite ran 500",
		},
		{
			name:    "nothing requested and nothing examined",
			census:  Census{},
			floor:   Floor{},
			wantSub: "examined no schedules at all",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.census.Check(tc.floor)
			if err == nil {
				t.Fatalf("Check passed a census of %d schedules against a suite of %d",
					tc.census.Schedules, tc.floor.Schedules)
			}
			if !strings.Contains(err.Error(), tc.wantSub) {
				t.Errorf("Check = %q, want it to mention %q", err, tc.wantSub)
			}
		})
	}
}

// TestCensusFloorsFailWhenTheSuiteReachesNothing.
//
// One row per floor, each a census that clears every other floor and falls
// below exactly one. A floor that no census can fall below is a floor that
// occupies the space where a real one would go, so each is demonstrated
// failing on its own rather than as a group.
func TestCensusFloorsFailWhenTheSuiteReachesNothing(t *testing.T) {
	// A census that clears every floor below, as the baseline each row spoils
	// exactly one field of.
	healthy := func() Census {
		c := Census{
			Schedules:                  100,
			SchedulesWithFaults:        70,
			SchedulesThatAborted:       60,
			Resumes:                    80,
			ResumesFromCheckpoint:      70,
			RestartsFromZero:           10,
			SchedulesWithPendingWindow: 50,
			PendingWindowsTotal:        900,
			PendingWindowsMax:          40,
			AlignmentInsideWindow:      20,
			AlignmentCompletedFirst:    5,
			AlignmentNeverOpened:       5,
		}
		c.FaultsScheduled = [triggerKindCount]int{60, 40, 30}
		c.FaultsFired = [triggerKindCount]int{55, 35, 20}
		return c
	}
	floor := Floor{
		Schedules:                     100,
		FiredFraction:                 0.5,
		AbortedFraction:               0.4,
		CheckpointResumeFraction:      0.5,
		PendingWindowFraction:         0.3,
		AlignmentInsideWindowFraction: 0.4,
	}
	if err := healthy().Check(floor); err != nil {
		t.Fatalf("the baseline census does not clear the floor, so no row below spoils anything: %v", err)
	}

	for _, tc := range []struct {
		name    string
		spoil   func(*Census)
		wantSub string
	}{
		{"faults never fire", func(c *Census) { c.FaultsFired = [triggerKindCount]int{1, 0, 0} }, "faults that fired"},
		{"no schedule aborts", func(c *Census) { c.SchedulesThatAborted = 1 }, "schedules that aborted"},
		{"every resume restarts from zero", func(c *Census) {
			c.ResumesFromCheckpoint, c.RestartsFromZero = 0, 80
		}, "resumes from a complete checkpoint"},
		{"no cut holds a pending window", func(c *Census) {
			c.SchedulesWithPendingWindow, c.PendingWindowsTotal, c.PendingWindowsMax = 0, 0, 0
		}, "schedules with a pending window at a cut"},
		{"no alignment fault lands inside a window", func(c *Census) {
			c.AlignmentInsideWindow, c.AlignmentNeverOpened = 0, 25
		}, "alignment faults inside a window"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := healthy()
			tc.spoil(&c)
			err := c.Check(floor)
			if err == nil {
				t.Fatalf("Check passed a census where %s", tc.name)
			}
			if !strings.Contains(err.Error(), tc.wantSub) {
				t.Errorf("Check = %q, want it to name %q", err, tc.wantSub)
			}
		})
	}
}

// TestCensusAddCountsWhatItIsGiven.
func TestCensusAddCountsWhatItIsGiven(t *testing.T) {
	var c Census
	c.Add(Result{Seed: 1})
	c.Add(Result{
		Seed:   2,
		Faults: []Fault{{VertexID: "srcA", Trigger: TriggerAfterElements, N: 5}},
		Outcomes: []FaultOutcome{
			{Fault: Fault{VertexID: "srcA", Trigger: TriggerAfterElements, N: 5}, Fired: true},
		},
		Recoveries: []Recovery{{FromCheckpoint: true, CheckpointID: 2, PendingWindows: 17}},
	})
	c.Add(Result{
		Seed:   3,
		Faults: []Fault{{VertexID: "window", Trigger: TriggerDuringAlignment, N: 2, Inputs: 3}},
		Outcomes: []FaultOutcome{
			{Fault: Fault{VertexID: "window", Trigger: TriggerDuringAlignment, N: 2, Inputs: 3},
				Alignment: AlignmentCompletedFirst},
		},
	})
	c.Add(Result{
		Seed:       4,
		Faults:     []Fault{{VertexID: "out", Trigger: TriggerAfterBarrier, N: 1}},
		Outcomes:   []FaultOutcome{{Fault: Fault{VertexID: "out", Trigger: TriggerAfterBarrier, N: 1}, Fired: true}},
		Recoveries: []Recovery{{FromCheckpoint: false}},
	})

	for _, tc := range []struct {
		name      string
		got, want int
	}{
		{"schedules", c.Schedules, 4},
		{"with faults", c.SchedulesWithFaults, 3},
		{"aborted", c.SchedulesThatAborted, 2},
		{"element faults scheduled", c.FaultsScheduled[TriggerAfterElements], 1},
		{"element faults fired", c.FaultsFired[TriggerAfterElements], 1},
		{"alignment faults scheduled", c.FaultsScheduled[TriggerDuringAlignment], 1},
		{"alignment faults fired", c.FaultsFired[TriggerDuringAlignment], 0},
		{"alignment completed first", c.AlignmentCompletedFirst, 1},
		{"resumes", c.Resumes, 2},
		{"resumes from a checkpoint", c.ResumesFromCheckpoint, 1},
		{"restarts from zero", c.RestartsFromZero, 1},
		{"schedules with a pending window", c.SchedulesWithPendingWindow, 1},
		{"pending windows total", c.PendingWindowsTotal, 17},
		{"pending windows max", c.PendingWindowsMax, 17},
	} {
		if tc.got != tc.want {
			t.Errorf("%s = %d, want %d", tc.name, tc.got, tc.want)
		}
	}

	table := c.Table()
	for _, want := range []string{"chaos census over 4 schedules", "during-alignment", "pending (key, window) pairs, max at a cut"} {
		if !strings.Contains(table, want) {
			t.Errorf("the table does not mention %q:\n%s", want, table)
		}
	}
}

// TestPendingWindowsAtIsNonZeroForThisWorkload.
//
// The pending count is the census field the whole floor rests on, and it is
// computed by reading a checkpoint's timer partition. If this workload never
// produced a cut with a complete-but-unfired (key, window), the floor would be
// unreachable and the suite would fail for a reason that has nothing to do with
// the runtime. Asserted against a clean run's own checkpoints, where nothing
// interferes.
func TestPendingWindowsAtIsNonZeroForThisWorkload(t *testing.T) {
	storage, ids := cleanRunCheckpointIDs(t)
	if len(ids) == 0 {
		t.Fatal("the clean run completed no checkpoint")
	}
	best := 0
	for _, id := range ids {
		pending, err := pendingWindowsAt(storage, id)
		if err != nil {
			t.Fatalf("pendingWindowsAt(%d): %v", id, err)
		}
		t.Logf("checkpoint %d holds %d complete but unfired (key, window) pairs", id, pending)
		if pending > best {
			best = pending
		}
	}
	if best == 0 {
		t.Error("no checkpoint of a clean run holds a (key, window) with all its records behind the cut and " +
			"its firing watermark ahead of it, so nothing this suite recovers from depends on a timer surviving")
	}
}

// TestSuiteFloorIsBelowWhatTheSuiteObserves.
//
// The floor is a written-down constant, and the thing that can go wrong with
// one is that it drifts above what the suite actually reaches and starts
// failing for no reason, or is set so far below that nothing can trip it. This
// pins it against the observations recorded on SuiteFloor: each floor must be
// under the worst of them, and not by more than half of it.
//
// It is a test of the CONSTANT, not of a run. A run of five hundred schedules
// takes half a minute and is the suite's own job; this is the thirty
// microseconds that says somebody editing the number cannot quietly turn the
// assertion off.
func TestSuiteFloorIsBelowWhatTheSuiteObserves(t *testing.T) {
	f := SuiteFloor(500)
	for _, tc := range []struct {
		name      string
		floor     float64
		worstSeen float64
	}{
		{"faults that fired", f.FiredFraction, 0.805},
		{"schedules that aborted", f.AbortedFraction, 0.714},
		{"resumes from a real checkpoint", f.CheckpointResumeFraction, 0.600},
		{"schedules with a pending window", f.PendingWindowFraction, 0.586},
		{"alignment faults inside a window", f.AlignmentInsideWindowFraction, 1.000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.floor >= tc.worstSeen {
				t.Errorf("the floor is %.3f and the worst run observed %.3f: it will fail on a healthy suite",
					tc.floor, tc.worstSeen)
			}
			if tc.floor < tc.worstSeen/2 {
				t.Errorf("the floor is %.3f against a worst observation of %.3f: it is far enough below "+
					"that a suite reaching half of what it reaches today would still pass", tc.floor, tc.worstSeen)
			}
		})
	}
	if f.Schedules != 500 {
		t.Errorf("SuiteFloor(500).Schedules = %d, want 500: the guard must carry the number the suite ran", f.Schedules)
	}
}

// TestTimingIsActuallyRecorded is the guard on the instrument itself.
//
// The two metrics are collected by decorators -- one around the operator, one
// around the keyed state -- and the way a decorator fails is silently. A
// factory handed no tally, an operator wrapper that lost a method, a state
// wrapper the runtime never installed: any of them leaves the numbers at zero,
// and a zero flush fraction reads as a healthy engine rather than as a broken
// instrument.
//
// So the assertions are against a number known independently of the
// instrument. A clean run fires every (key, window) exactly once, and the
// oracle says how many there are, so the firings must sum to exactly that. And
// both buckets must be non-empty: a run where every window fired on the flush
// would make the fraction 1 and say nothing, and one where none did would make
// it 0 and say nothing either.
func TestTimingIsActuallyRecorded(t *testing.T) {
	res := RunSchedule(t, emptySeeds(t, 1)[0])

	rows := int64(len(oracleCounts()))
	if got := res.CleanTiming.firings(); got != rows {
		t.Errorf("the clean run recorded %d window firings and the oracle holds %d (key, window) rows: "+
			"the operator decorator is not counting what the sink received", got, rows)
	}
	if res.CleanTiming.FlushFirings == 0 {
		t.Error("no window fired on the end-of-input flush, so the flush fraction is pinned at zero " +
			"and cannot move under a watermark that stalls")
	}
	if res.CleanTiming.WatermarkFirings == 0 {
		t.Error("every window fired on the end-of-input flush, so the flush fraction is pinned at one " +
			"and cannot move under a watermark that runs ahead")
	}
	if res.CleanTiming.OtherEmissions != 0 {
		t.Errorf("the operator emitted %d records outside ProcessWatermark; the flush fraction is a "+
			"fraction of the wrong denominator", res.CleanTiming.OtherEmissions)
	}
	if res.CleanTiming.PeakStateEntries == 0 {
		t.Error("the run peaked at zero keyed-state entries, so the state decorator was never installed")
	}
	// The peak must be at least one entry per (key, window) the run holds open
	// at once, which is more than a handful and fewer than every row plus its
	// timer plus the watermark scalar.
	if got, ceiling := res.CleanTiming.PeakStateEntries, 2*rows+2; got > ceiling {
		t.Errorf("the run peaked at %d keyed-state entries against %d rows: an aggregate and a timer "+
			"per open window plus one watermark per subtask cannot exceed %d", got, rows, ceiling)
	}
	t.Logf("clean run: %d firings (%d on the flush, %.1f%%), peak %d state entries",
		res.CleanTiming.firings(), res.CleanTiming.FlushFirings,
		100*res.CleanTiming.FlushFraction(), res.CleanTiming.PeakStateEntries)
}

// TestTimingAddSumsFiringsAndTakesTheGreaterPeak.
//
// A peak is not a sum: two attempts each holding a thousand entries never held
// two thousand, because the first had unwound before the second began. Adding
// them would inflate the fault-side peak in proportion to how many faults a
// seed drew, which is exactly the correlation that would make the baseline
// meaningless.
func TestTimingAddSumsFiringsAndTakesTheGreaterPeak(t *testing.T) {
	var got Timing
	got.add(Timing{FlushFirings: 3, WatermarkFirings: 7, OtherEmissions: 1, PeakStateEntries: 1000})
	got.add(Timing{FlushFirings: 5, WatermarkFirings: 5, OtherEmissions: 0, PeakStateEntries: 400})
	want := Timing{FlushFirings: 8, WatermarkFirings: 12, OtherEmissions: 1, PeakStateEntries: 1000}
	if got != want {
		t.Errorf("add gave %+v, want %+v", got, want)
	}
	if f := got.FlushFraction(); f != 0.4 {
		t.Errorf("FlushFraction = %v, want 0.4", f)
	}
	if f := (Timing{}).FlushFraction(); f != 0 {
		t.Errorf("the flush fraction of a run that fired nothing is %v, want 0", f)
	}
}

// TestTimingBaselineBandsFailOnEachSide.
//
// One row per figure per direction. Both directions matter and for different
// reasons: too high is a watermark that stalls and leaves its windows to the
// end-of-input flush, too low is one that runs ahead and fires them early. A
// band checked on one side only would be half an assertion.
func TestTimingBaselineBandsFailOnEachSide(t *testing.T) {
	base := SuiteTimingBaseline(500)
	healthy := func() TimingAggregate {
		a := TimingAggregate{Schedules: 500}
		// 32% of 100000 on the flush, and 30% of 100000 on the fault side.
		a.Clean = Timing{FlushFirings: 32000, WatermarkFirings: 68000}
		a.Fault = Timing{FlushFirings: 30000, WatermarkFirings: 70000}
		a.CleanPeakStateSum = int64(base.CleanPeakState.Baseline) * 500
		a.FaultPeakStateSum = int64(base.FaultPeakState.Baseline) * 500
		return a
	}
	if err := healthy().Check(base); err != nil {
		t.Fatalf("the baseline aggregate does not clear its own bands, so no row below spoils anything: %v", err)
	}

	for _, tc := range []struct {
		name    string
		spoil   func(*TimingAggregate)
		wantSub string
	}{
		{"clean flush stalls high", func(a *TimingAggregate) {
			a.Clean = Timing{FlushFirings: 90000, WatermarkFirings: 10000}
		}, "clean flush fraction"},
		{"clean flush collapses low", func(a *TimingAggregate) {
			a.Clean = Timing{FlushFirings: 1000, WatermarkFirings: 99000}
		}, "clean flush fraction"},
		{"fault flush stalls high", func(a *TimingAggregate) {
			a.Fault = Timing{FlushFirings: 90000, WatermarkFirings: 10000}
		}, "fault flush fraction"},
		{"fault flush collapses low", func(a *TimingAggregate) {
			a.Fault = Timing{FlushFirings: 1000, WatermarkFirings: 99000}
		}, "fault flush fraction"},
		{"clean peak inflates", func(a *TimingAggregate) { a.CleanPeakStateSum *= 2 }, "clean mean peak state"},
		{"clean peak collapses", func(a *TimingAggregate) { a.CleanPeakStateSum /= 2 }, "clean mean peak state"},
		{"fault peak inflates", func(a *TimingAggregate) { a.FaultPeakStateSum *= 2 }, "fault mean peak state"},
		{"fault peak collapses", func(a *TimingAggregate) { a.FaultPeakStateSum /= 2 }, "fault mean peak state"},
		{"the operator emits outside a watermark", func(a *TimingAggregate) {
			a.Clean.OtherEmissions = 1
		}, "outside ProcessWatermark"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := healthy()
			tc.spoil(&a)
			err := a.Check(base)
			if err == nil {
				t.Fatalf("Check passed an aggregate where %s", tc.name)
			}
			if !strings.Contains(err.Error(), tc.wantSub) {
				t.Errorf("Check = %q, want it to name %q", err, tc.wantSub)
			}
		})
	}
}

// TestTimingCheckRefusesAnUninstrumentedAggregate is the timing half of the
// census guard: a comparison against a baseline is vacuous when the thing
// compared was never populated.
func TestTimingCheckRefusesAnUninstrumentedAggregate(t *testing.T) {
	for _, tc := range []struct {
		name    string
		a       TimingAggregate
		wantSub string
	}{
		{"no schedules", TimingAggregate{}, "covers no schedules at all"},
		{"clean side never counted", TimingAggregate{
			Schedules: 500,
			Fault:     Timing{FlushFirings: 32000, WatermarkFirings: 68000},
		}, "nothing was instrumented"},
		{"fault side never counted", TimingAggregate{
			Schedules: 500,
			Clean:     Timing{FlushFirings: 32000, WatermarkFirings: 68000},
		}, "nothing was instrumented"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.a.Check(SuiteTimingBaseline(500))
			if err == nil {
				t.Fatal("Check passed an aggregate nothing populated")
			}
			if !strings.Contains(err.Error(), tc.wantSub) {
				t.Errorf("Check = %q, want it to mention %q", err, tc.wantSub)
			}
		})
	}
}

// TestSuiteTimingBaselineMatchesWhatWasObserved pins the committed constants
// against the runs they were taken from, the way TestSuiteFloorIsBelowWhatThe
// SuiteObserves pins the census floor. It is the assertion that stops somebody
// widening a band until the check stops meaning anything.
//
// Both suite sizes, because the flush band is a function of the size and a test
// of one regime would leave the other free to drift.
func TestSuiteTimingBaselineMatchesWhatWasObserved(t *testing.T) {
	for _, size := range []struct {
		name      string
		schedules int
		// lowest and highest are the extremes observed over repeated runs at
		// this size, per figure, in the order the rows below take them.
		clean, fault         [2]float64
		cleanPeak, faultPeak [2]float64
		maxFlushWidth        float64
	}{
		{
			name: "full suite", schedules: 500,
			clean: [2]float64{0.3444, 0.3510}, fault: [2]float64{0.3180, 0.3288},
			cleanPeak: [2]float64{3765.2, 3779.0}, faultPeak: [2]float64{3898.5, 3908.2},
			maxFlushWidth: 0.30,
		},
		{
			name: "race subset", schedules: 25,
			clean: [2]float64{0.2229, 0.3400}, fault: [2]float64{0.2236, 0.3610},
			cleanPeak: [2]float64{3703.2, 3810.0}, faultPeak: [2]float64{3779.9, 3925.0},
			maxFlushWidth: 0.75,
		},
	} {
		t.Run(size.name, func(t *testing.T) {
			b := SuiteTimingBaseline(size.schedules)
			for _, tc := range []struct {
				name     string
				band     TimingBand
				seen     [2]float64
				maxWidth float64
			}{
				{"clean flush fraction", b.CleanFlushFraction, size.clean, size.maxFlushWidth},
				{"fault flush fraction", b.FaultFlushFraction, size.fault, size.maxFlushWidth},
				{"clean mean peak state", b.CleanPeakState, size.cleanPeak, 0.20},
				{"fault mean peak state", b.FaultPeakState, size.faultPeak, 0.20},
			} {
				t.Run(tc.name, func(t *testing.T) {
					if tc.band.lo() > tc.seen[0] {
						t.Errorf("the band opens at %.4f and the lowest run observed %.4f: "+
							"it will fail on a healthy suite", tc.band.lo(), tc.seen[0])
					}
					if tc.band.hi() < tc.seen[1] {
						t.Errorf("the band closes at %.4f and the highest run observed %.4f: "+
							"it will fail on a healthy suite", tc.band.hi(), tc.seen[1])
					}
					if tc.band.Below > tc.maxWidth || tc.band.Above > tc.maxWidth {
						t.Errorf("the band runs -%.0f%% +%.0f%%, which is past the %.0f%% this "+
							"figure's spread justifies",
							100*tc.band.Below, 100*tc.band.Above, 100*tc.maxWidth)
					}
				})
			}
		})
	}
}

// TestThePeakStateBandCanStillCatchAStalledWatermark.
//
// The peak metric has a ceiling, and it is close. Every (key, window) this
// workload produces holds an aggregate and a timer while it is open, plus one
// watermark scalar per operator subtask, so nothing can exceed 2*rows+2 entries
// -- and the clean peak already sits at about seven-eighths of that. A
// watermark that stalled completely would raise it to the ceiling and no
// further, so the band ABOVE the baseline has to be inside that gap or the
// metric is decoration.
//
// This is the assertion that says the upper band is worth having. It is also
// the honest record of how little room there is: if the baseline ever rises
// past the ceiling minus its own upper band, this fails and says so rather than
// leaving a check that cannot trip.
func TestThePeakStateBandCanStillCatchAStalledWatermark(t *testing.T) {
	ceiling := float64(2*len(oracleCounts()) + 2)
	b := SuiteTimingBaseline(500)
	for _, tc := range []struct {
		name string
		band TimingBand
	}{
		{"clean", b.CleanPeakState},
		{"fault", b.FaultPeakState},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.band.hi() >= ceiling {
				t.Errorf("the band closes at %.0f entries and the workload cannot hold more than %.0f: "+
					"a watermark that stalled every window would still be inside the band",
					tc.band.hi(), ceiling)
			}
			t.Logf("baseline %.0f, band closes at %.0f, ceiling %.0f: the metric has %.0f entries of "+
				"headroom to detect a stall in", tc.band.Baseline, tc.band.hi(), ceiling, ceiling-tc.band.hi())
		})
	}
}

// TestTheSmallSuiteBandIsStillAnAssertion.
//
// A band of plus or minus sixty per cent is wide, and the thing to check about
// a wide band is that the failures it exists for still fall outside it. Both
// watermark bugs this metric is aimed at take the fraction to an extreme: a
// gate taking the maximum fires every window early and leaves nothing for the
// flush, and one that freezes an exhausted input at its last watermark leaves
// almost everything to it.
func TestTheSmallSuiteBandIsStillAnAssertion(t *testing.T) {
	b := SuiteTimingBaseline(25)
	for _, tc := range []struct {
		name     string
		fraction float64
	}{
		{"a gate taking the maximum fires everything early", 0.01},
		{"an exhausted input frozen leaves everything to the flush", 0.99},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.fraction >= b.CleanFlushFraction.lo() && tc.fraction <= b.CleanFlushFraction.hi() {
				t.Errorf("a flush fraction of %.2f sits inside the band [%.4f, %.4f], so the widened "+
					"band no longer catches what it is for",
					tc.fraction, b.CleanFlushFraction.lo(), b.CleanFlushFraction.hi())
			}
		})
	}
}
