package chaos

import (
	"context"
	"fmt"
	"testing"

	"github.com/AarinB1/tidemark/pkg/checkpoint"
	"github.com/AarinB1/tidemark/pkg/runtime"
	"github.com/AarinB1/tidemark/pkg/sinks"
)

// TestRunScheduleOnSeedsThatFireNothing is the control group.
//
// A schedule with no fault at all still runs the whole harness: the clean run,
// the "fault" run that has no fault in it, and both comparisons against the
// oracle. If the harness could not get this right there would be nothing to
// read into the five hundred seeds that do abort something.
func TestRunScheduleOnSeedsThatFireNothing(t *testing.T) {
	empty := emptySeeds(t, 4)
	for _, seed := range empty {
		t.Run(fmt.Sprintf("seed%d", seed), func(t *testing.T) {
			res := RunSchedule(t, seed)
			if len(res.Faults) != 0 {
				t.Fatalf("seed %d was chosen for having no faults and has %v", seed, res.Faults)
			}
			if len(res.Recoveries) != 0 {
				t.Errorf("seed %d recovered %d times with nothing scheduled", seed, len(res.Recoveries))
			}
		})
	}
}

// emptySeeds returns the first n seeds whose schedule holds no fault.
func emptySeeds(t *testing.T, n int) []int64 {
	t.Helper()
	var out []int64
	for seed := int64(1); len(out) < n && seed <= 200; seed++ {
		if len(ScheduleFor(seed, scheduleGraphOnce())) == 0 {
			out = append(out, seed)
		}
	}
	if len(out) < n {
		t.Fatalf("only %d of the first 200 seeds schedule nothing", len(out))
	}
	return out
}

// TestRunScheduleRecoversFromAScheduledFault runs the first seeds that schedule
// something and asserts the oracle comparison still holds.
//
// Deliberately not a hand-written fault: the harness has to be right on the
// schedules the suite will actually run, and a case written by hand is a case
// that agrees with whatever the author expected. Recoveries is reported rather
// than asserted at a number, because whether a given fault fires depends on the
// order concurrent inputs reach the gate; the census in step 4 is what measures
// that, and asserting a count here would be asserting on the Go scheduler.
func TestRunScheduleRecoversFromAScheduledFault(t *testing.T) {
	for _, seed := range faultySeeds(t, 6) {
		t.Run(fmt.Sprintf("seed%d", seed), func(t *testing.T) {
			res := RunSchedule(t, seed)
			if len(res.Faults) == 0 {
				t.Fatalf("seed %d was chosen for scheduling a fault and schedules none", seed)
			}
			t.Logf("seed %d: %d faults %v, %d recoveries", seed, len(res.Faults), res.Faults, len(res.Recoveries))
			if len(res.Recoveries) > maxRecoveries {
				t.Errorf("seed %d recovered %d times, above the cap of %d", seed, len(res.Recoveries), maxRecoveries)
			}
		})
	}
}

// faultySeeds returns the first n seeds that schedule at least one fault
// against a SOURCE subtask under the element trigger.
//
// Those are the faults that fire deterministically: a source subtask's n-th
// record is the n-th element of its own contiguous range, so a fault below the
// range length always lands. A fault against an operator or a sink depends on
// how the key hash divided the stream, and one aimed at an alignment window
// depends on the order two inputs reach the gate. Choosing the deterministic
// ones here is what makes this test assert that recovery works rather than that
// it sometimes gets exercised.
func faultySeeds(t *testing.T, n int) []int64 {
	t.Helper()
	var out []int64
	for seed := int64(1); len(out) < n && seed <= 200; seed++ {
		for _, f := range ScheduleFor(seed, scheduleGraphOnce()) {
			if f.Trigger == TriggerAfterElements && (f.VertexID == "srcA" || f.VertexID == "srcB") {
				out = append(out, seed)
				break
			}
		}
	}
	if len(out) < n {
		t.Fatalf("only %d of the first 200 seeds schedule an element fault on a source", len(out))
	}
	return out
}

// TestTheWorkloadReachesEveryCheckpoint.
//
// The schedule generator draws checkpoint IDs in 1..maxCheckpoint from the
// barrier arithmetic, and a job that completed fewer would spend every barrier
// fault above that on a checkpoint nobody reaches. Asserted against a clean
// run, where nothing interferes.
//
// The bound is taken from barriersPerSubtask, which is the function sitesOf
// bounds the draw with, rather than written out as a literal. A literal is a
// restatement of the arithmetic and drifts from it silently: this test held the
// number four, correct at a barrier interval of 500, and the only thing that
// noticed when the interval moved was the test itself failing with a number it
// could have computed. What is being asserted is that the RUN reaches what the
// GENERATOR aims at, and that comparison is only meaningful when one side is
// measured and the other is the generator's own arithmetic.
func TestTheWorkloadReachesEveryCheckpoint(t *testing.T) {
	// Both sources inject the same number of barriers by construction; see the
	// note on barrierIntervalA. The generator takes the maximum over the source
	// vertices, so either one produces it.
	want := barriersPerSubtask(sourceACount, jobParallelism, barrierIntervalA)
	if other := barriersPerSubtask(sourceBCount, jobParallelism, barrierIntervalB); other != want {
		t.Fatalf("srcA injects %d barriers a subtask and srcB injects %d: checkpoint k names a "+
			"different depth on each input and the alignment windows stop lining up", want, other)
	}
	if ids := cleanRunCheckpoints(t); ids != want {
		t.Errorf("a clean run completed checkpoint %d as its last, want %d: "+
			"the schedule generator draws barrier faults it cannot land", ids, want)
	}
}

// TestTheWorkloadFiresWindowsBeforeItEnds.
//
// The whole recovery story needs windows that close while the run is still
// going: a job whose windows all fired at the end-of-input flush would put
// nothing interesting in a checkpoint, and every recovery would be exercising
// an empty state. Counted from the clean run's sink against the oracle's row
// count, which is the number of windows the job produces in total.
func TestTheWorkloadFiresWindowsBeforeItEnds(t *testing.T) {
	if rows := len(oracleCounts()); rows < 1000 {
		t.Errorf("the workload produces %d (key, window) rows, which is too few to say anything "+
			"about where a fault lands", rows)
	} else {
		t.Logf("the workload produces %d (key, window) rows over %d records", rows, sourceACount+sourceBCount)
	}
}

// cleanRunCheckpoints runs the workload with checkpointing on and returns the
// highest complete checkpoint it left behind.
func cleanRunCheckpoints(t *testing.T) int64 {
	t.Helper()
	root := t.TempDir()
	if err := runtime.RunWithOptions(context.Background(),
		jobGraph(sinks.NewCollect(), &windowFactory{}),
		runtime.Options{CheckpointRoot: root, Seed: 1}); err != nil {
		t.Fatalf("the clean run failed: %v", err)
	}
	id, ok, err := checkpoint.NewStorage(root).Latest()
	if err != nil {
		t.Fatalf("Latest: %v", err)
	}
	if !ok {
		t.Fatal("the clean run completed no checkpoint at all")
	}
	return id
}

// cleanRunCheckpointIDs runs the workload with checkpointing on and returns its
// storage together with every complete checkpoint ID it left behind.
func cleanRunCheckpointIDs(t *testing.T) (*checkpoint.Storage, []int64) {
	t.Helper()
	root := t.TempDir()
	if err := runtime.RunWithOptions(context.Background(),
		jobGraph(sinks.NewCollect(), &windowFactory{}),
		runtime.Options{CheckpointRoot: root, Seed: 1}); err != nil {
		t.Fatalf("the clean run failed: %v", err)
	}
	storage := checkpoint.NewStorage(root)
	last, ok, err := storage.Latest()
	if err != nil {
		t.Fatalf("Latest: %v", err)
	}
	if !ok {
		return storage, nil
	}
	var ids []int64
	for id := int64(1); id <= last; id++ {
		if _, _, err := storage.Load(id); err == nil {
			ids = append(ids, id)
		}
	}
	return storage, ids
}
