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
		{"faults that fired", f.FiredFraction, 0.854},
		{"schedules that aborted", f.AbortedFraction, 0.714},
		{"resumes from a real checkpoint", f.CheckpointResumeFraction, 0.343},
		{"schedules with a pending window", f.PendingWindowFraction, 0.466},
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
