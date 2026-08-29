package chaos

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"

	"github.com/AarinB1/tidemark/pkg/checkpoint"
	"github.com/AarinB1/tidemark/pkg/sources"
	"github.com/AarinB1/tidemark/pkg/state"
	"github.com/AarinB1/tidemark/test/oracle"
)

// The census is the real result of this phase.
//
// Zero divergence across five hundred schedules means nothing on its own. It
// means something in proportion to how many of those schedules put a fault
// somewhere a bug could have surfaced: if forty of five hundred had a complete
// but unfired window at the cut they recovered from, the strength of the suite
// is forty, and the other four hundred and sixty are a statement about a job
// that recovered from nothing interesting.
//
// This is the generalisation of the pending-window guard Phase 3c put on its
// two recovery cases. That guard asserted that ONE hand-written case reached a
// state where a lost timer would show; this counts how often five hundred
// seeded ones do, and refuses to pass if the answer is too low.

// triggerKindCount is how many trigger kinds there are, for the fixed-size
// tallies below. An array rather than a map so that the census has a usable
// zero value and prints in a fixed order; a map here would put Go's iteration
// randomisation into a table that is meant to be diffed between runs.
const triggerKindCount = int(TriggerDuringAlignment) + 1

// allTriggerKinds is the order the table reports in.
var allTriggerKinds = [triggerKindCount]TriggerKind{
	TriggerAfterElements, TriggerAfterBarrier, TriggerDuringAlignment,
}

// AlignmentOutcome says what became of a fault aimed at an alignment window.
//
// The distinction the middle two draw is the one step 4 exists for: a fault
// that did not fire is not automatically a fault that had nowhere to land, and
// counting the two together would let an alignment trigger that never worked
// look like an alignment trigger that was merely unlucky.
type AlignmentOutcome uint8

const (
	// AlignmentNotApplicable is every fault that is not an alignment fault.
	AlignmentNotApplicable AlignmentOutcome = iota
	// AlignmentInsideWindow is a fault that fired. The gate offers the decision
	// only while a live input has still to deliver the barrier, so a fire is by
	// construction a fault that landed inside a genuine alignment window --
	// this is not inferred after the fact.
	AlignmentInsideWindow
	// AlignmentCompletedFirst is a fault whose subtask DID open an alignment
	// for that checkpoint, but never with as many inputs delivered as the
	// schedule named. Alignment completed at fewer, so the window had closed
	// before the position existed. This is "alignment had already completed".
	AlignmentCompletedFirst
	// AlignmentNeverOpened is a fault whose subtask never had an open
	// alignment for that checkpoint at all: the run stopped before the
	// checkpoint reached it, or every barrier for it completed alignment as it
	// arrived.
	AlignmentNeverOpened
)

func (o AlignmentOutcome) String() string {
	switch o {
	case AlignmentNotApplicable:
		return "n/a"
	case AlignmentInsideWindow:
		return "inside-window"
	case AlignmentCompletedFirst:
		return "alignment-completed-first"
	case AlignmentNeverOpened:
		return "alignment-never-opened"
	default:
		return "unknown"
	}
}

// FaultOutcome is what became of one scheduled fault.
type FaultOutcome struct {
	Fault Fault
	// Fired is whether the runtime aborted a subtask for this fault. A fault at
	// an element count no subtask reaches never does, and neither does one
	// behind another fault that stopped the run first.
	Fired bool
	// Alignment is AlignmentNotApplicable except on an alignment fault.
	Alignment AlignmentOutcome
}

// Recovery is one resume of a schedule after an abort.
type Recovery struct {
	// FromCheckpoint is false when no checkpoint had completed at the moment
	// the fault fired, so the job restarted from zero. That is a legitimate and
	// interesting outcome rather than a failure -- but a suite where it was the
	// only outcome would never have exercised restore at all.
	FromCheckpoint bool
	CheckpointID   int64
	// PendingWindows is how many (key, window) pairs had all their records
	// below the cut this recovery resumes from and their firing watermark above
	// it.
	//
	// It is the number that says whether a recovery proved anything. A pair
	// whose records straddle the cut is re-armed by the replay and fires again
	// whether or not its timer survived; one that fired before the cut is
	// already in the sink. Only a pair that receives nothing more depends on
	// the timer being in the checkpoint: with timers in RAM it comes back as an
	// aggregate nothing will ever fire, and it goes missing from the sink with
	// no error anywhere.
	//
	// It is measured at the CUT rather than at the fault, because the cut is
	// what the checkpoint records and what the replay starts from. The fault
	// lands somewhere after it.
	PendingWindows int
}

// Census aggregates the outcomes of a suite of schedules.
//
// The zero value is an empty census, and Schedules is what says so. Every floor
// below is checked against it, which is why Check refuses a census of zero
// schedules outright: a comparison against a floor is vacuous when the thing
// being compared was never populated, and that is exactly how the Phase 3c
// WalkDir scan passed while reading no files.
type Census struct {
	// Schedules is how many schedules were examined. Check requires it to equal
	// the number requested.
	Schedules int
	// SchedulesWithFaults is how many held at least one fault; the rest are the
	// control group.
	SchedulesWithFaults int
	// SchedulesThatAborted is how many had at least one fault actually fire.
	SchedulesThatAborted int

	FaultsScheduled [triggerKindCount]int
	FaultsFired     [triggerKindCount]int

	Resumes               int
	ResumesFromCheckpoint int
	RestartsFromZero      int

	// SchedulesWithPendingWindow is how many schedules recovered from a cut
	// holding at least one complete but unfired (key, window).
	SchedulesWithPendingWindow int
	PendingWindowsTotal        int
	PendingWindowsMax          int

	AlignmentInsideWindow   int
	AlignmentCompletedFirst int
	AlignmentNeverOpened    int
}

// Add folds one schedule's result into the census.
func (c *Census) Add(r Result) {
	c.Schedules++
	if len(r.Faults) > 0 {
		c.SchedulesWithFaults++
	}

	aborted := false
	for _, o := range r.Outcomes {
		c.FaultsScheduled[o.Fault.Trigger]++
		if o.Fired {
			c.FaultsFired[o.Fault.Trigger]++
			aborted = true
		}
		switch o.Alignment {
		case AlignmentInsideWindow:
			c.AlignmentInsideWindow++
		case AlignmentCompletedFirst:
			c.AlignmentCompletedFirst++
		case AlignmentNeverOpened:
			c.AlignmentNeverOpened++
		}
	}
	if aborted {
		c.SchedulesThatAborted++
	}

	pending := 0
	for _, rec := range r.Recoveries {
		c.Resumes++
		if rec.FromCheckpoint {
			c.ResumesFromCheckpoint++
		} else {
			c.RestartsFromZero++
		}
		c.PendingWindowsTotal += rec.PendingWindows
		if rec.PendingWindows > c.PendingWindowsMax {
			c.PendingWindowsMax = rec.PendingWindows
		}
		pending += rec.PendingWindows
	}
	if pending > 0 {
		c.SchedulesWithPendingWindow++
	}
}

// faultsScheduled and faultsFired are the totals across every trigger kind.
func (c Census) faultsScheduled() int { return sum(c.FaultsScheduled) }
func (c Census) faultsFired() int     { return sum(c.FaultsFired) }

func sum(a [triggerKindCount]int) int {
	total := 0
	for _, n := range a {
		total += n
	}
	return total
}

// alignmentScheduled is how many alignment faults were drawn. It is the
// denominator the alignment floor is a fraction of.
func (c Census) alignmentScheduled() int { return c.FaultsScheduled[TriggerDuringAlignment] }

// Table renders the census for a person to read.
//
// Absolute counts AND the fraction each is of its denominator, because the
// floor is stated as a fraction and a table that printed only one of the two
// would leave the reader doing the arithmetic that decides whether the suite
// passed.
func (c Census) Table() string {
	var b strings.Builder
	fmt.Fprintf(&b, "\nchaos census over %d schedules\n", c.Schedules)
	fmt.Fprintf(&b, "  %-42s %6d\n", "schedules examined", c.Schedules)
	fmt.Fprintf(&b, "  %-42s %6d  %s\n", "with at least one fault", c.SchedulesWithFaults, pct(c.SchedulesWithFaults, c.Schedules))
	fmt.Fprintf(&b, "  %-42s %6d  %s\n", "control group (no fault scheduled)", c.Schedules-c.SchedulesWithFaults, pct(c.Schedules-c.SchedulesWithFaults, c.Schedules))
	fmt.Fprintf(&b, "  %-42s %6d  %s\n", "that aborted at least once", c.SchedulesThatAborted, pct(c.SchedulesThatAborted, c.Schedules))
	fmt.Fprintf(&b, "\n  faults, by trigger            scheduled   fired\n")
	for _, k := range allTriggerKinds {
		fmt.Fprintf(&b, "  %-28s %7d %7d  %s\n", k, c.FaultsScheduled[k], c.FaultsFired[k], pct(c.FaultsFired[k], c.FaultsScheduled[k]))
	}
	fmt.Fprintf(&b, "  %-28s %7d %7d  %s\n", "total", c.faultsScheduled(), c.faultsFired(), pct(c.faultsFired(), c.faultsScheduled()))
	fmt.Fprintf(&b, "\n  %-42s %6d\n", "resumes", c.Resumes)
	fmt.Fprintf(&b, "  %-42s %6d  %s\n", "from a complete checkpoint", c.ResumesFromCheckpoint, pct(c.ResumesFromCheckpoint, c.Resumes))
	fmt.Fprintf(&b, "  %-42s %6d  %s\n", "restarted from zero (none complete yet)", c.RestartsFromZero, pct(c.RestartsFromZero, c.Resumes))
	fmt.Fprintf(&b, "\n  %-42s %6d  %s\n", "schedules with a pending window at a cut", c.SchedulesWithPendingWindow, pct(c.SchedulesWithPendingWindow, c.Schedules))
	fmt.Fprintf(&b, "  %-42s %6d\n", "pending (key, window) pairs, total", c.PendingWindowsTotal)
	fmt.Fprintf(&b, "  %-42s %6d\n", "pending (key, window) pairs, max at a cut", c.PendingWindowsMax)
	fmt.Fprintf(&b, "\n  alignment faults (%d scheduled)\n", c.alignmentScheduled())
	fmt.Fprintf(&b, "  %-42s %6d  %s\n", "landed inside an alignment window", c.AlignmentInsideWindow, pct(c.AlignmentInsideWindow, c.alignmentScheduled()))
	fmt.Fprintf(&b, "  %-42s %6d  %s\n", "alignment completed before that point", c.AlignmentCompletedFirst, pct(c.AlignmentCompletedFirst, c.alignmentScheduled()))
	fmt.Fprintf(&b, "  %-42s %6d  %s\n", "no alignment opened for that checkpoint", c.AlignmentNeverOpened, pct(c.AlignmentNeverOpened, c.alignmentScheduled()))
	return b.String()
}

// pct renders n out of total. A zero denominator prints as such rather than as
// a percentage of nothing.
func pct(n, total int) string {
	if total == 0 {
		return "(no denominator)"
	}
	return fmt.Sprintf("(%.1f%%)", 100*float64(n)/float64(total))
}

// Floor is the least a suite's census may report and still pass.
//
// Stated as FRACTIONS rather than as counts so that the same floor applies to
// the five hundred-schedule run and to the twenty-five-schedule one CI runs
// under the race detector. The seeds are contiguous and the schedules are a
// pure function of them, so the smaller run is a fixed subset rather than a
// sample: neither number is subject to variance, and a fraction that holds on
// both is a fraction that holds on both for a reason.
//
// Schedules is not a fraction and is not a floor. It is the guard: the census
// must have examined exactly the number of schedules the suite asked for. Every
// comparison below is vacuous without it, which is the lesson of the Phase 3c
// WalkDir scan that walked no files and passed.
type Floor struct {
	Schedules int
	// FiredFraction is the least share of scheduled faults that must fire.
	FiredFraction float64
	// AbortedFraction is the least share of SCHEDULES in which something fired.
	AbortedFraction float64
	// CheckpointResumeFraction is the least share of resumes that must come
	// from a complete checkpoint rather than a restart from zero. A suite that
	// only ever restarted from zero would never have run the restore path.
	CheckpointResumeFraction float64
	// PendingWindowFraction is the least share of schedules that must have
	// recovered from a cut holding a complete but unfired (key, window). This
	// is the number that says the suite reached somewhere a lost timer would
	// show.
	PendingWindowFraction float64
	// AlignmentInsideWindowFraction is the least share of alignment faults that
	// must have landed inside a genuine alignment window. Below this, the third
	// trigger kind is not buying what it was added for.
	AlignmentInsideWindowFraction float64
}

// SuiteFloor is the floor the chaos suite is held to, over Schedules
// schedules.
//
// # Where these numbers come from
//
// Three independent five-hundred-seed runs and three twenty-five-seed ones,
// with zero divergence from the batch oracle in all six. What they observed:
//
//	                                      500 seeds        25 seeds     floor
//	faults that fired                 91.0 - 91.7%    85.4 - 87.8%      70%
//	schedules that aborted                   71.4%           76.0%      60%
//	resumes from a real checkpoint    44.2 - 45.2%    34.3 - 38.9%      25%
//	schedules with a pending window   46.6 - 47.8%    48.0 - 56.0%      30%
//	alignment faults inside a window        100.0%          100.0%      80%
//
// Each floor sits twenty to thirty-five per cent below the WORST of the six,
// which is the margin the numbers themselves ask for: a schedule is a pure
// function of its seed, but whether a fault fires is not. Whether the fault
// aimed between two inputs' barriers lands depends on the order those inputs
// reach the gate, and that is the Go scheduler's to decide. The spread above
// is what that costs, and the margin is set against it rather than against a
// guess.
//
// Fractions and not counts, so that the same floor holds for the five hundred
// schedules CI runs without the race detector and the twenty-five it runs with
// it. The seeds are contiguous and the schedules derive from them, so the
// smaller run is a fixed SUBSET rather than a sample: both columns above are
// reproducible numbers, not draws, and a fraction clearing both clears both for
// a reason.
//
// # What each one is protecting
//
// They are not five readings of one thing. A suite can pass the first two and
// still be worthless: every fault could fire early, every recovery could
// restart from zero, and five hundred schedules would then be five hundred
// runs of a job that recovered from nothing. The pending-window floor is the
// one that says otherwise, and it is the number to read first -- at 47 per
// cent, the strength of this suite is about two hundred and thirty-seven
// schedules and not five hundred. The alignment floor is what says the third
// trigger kind is still buying what it was added for.
func SuiteFloor(schedules int) Floor {
	return Floor{
		Schedules:                     schedules,
		FiredFraction:                 0.70,
		AbortedFraction:               0.60,
		CheckpointResumeFraction:      0.25,
		PendingWindowFraction:         0.30,
		AlignmentInsideWindowFraction: 0.80,
	}
}

// Check reports every way the census falls short of f, or nil.
//
// Every shortfall rather than the first: a suite that has drifted usually
// drifts in more than one place at once, and reporting one at a time turns
// diagnosis into a sequence of runs.
func (c Census) Check(f Floor) error {
	var problems []string
	// The guard first, and separately, because a census of zero schedules
	// satisfies no fraction and would otherwise be reported as five failures
	// with one cause.
	if c.Schedules != f.Schedules {
		return fmt.Errorf("the census examined %d schedules and the suite ran %d: "+
			"every floor below is a comparison against a census that was not populated", c.Schedules, f.Schedules)
	}
	if c.Schedules == 0 {
		return fmt.Errorf("the census examined no schedules at all")
	}

	check := func(name string, n, total int, floor float64) {
		if total == 0 {
			problems = append(problems, fmt.Sprintf("%s: nothing to measure against (denominator is zero)", name))
			return
		}
		got := float64(n) / float64(total)
		if got < floor {
			problems = append(problems, fmt.Sprintf("%s: %d of %d (%.1f%%), floor is %.1f%%",
				name, n, total, 100*got, 100*floor))
		}
	}
	check("faults that fired", c.faultsFired(), c.faultsScheduled(), f.FiredFraction)
	check("schedules that aborted", c.SchedulesThatAborted, c.Schedules, f.AbortedFraction)
	check("resumes from a complete checkpoint", c.ResumesFromCheckpoint, c.Resumes, f.CheckpointResumeFraction)
	check("schedules with a pending window at a cut", c.SchedulesWithPendingWindow, c.Schedules, f.PendingWindowFraction)
	check("alignment faults inside a window", c.AlignmentInsideWindow, c.alignmentScheduled(), f.AlignmentInsideWindowFraction)

	if len(problems) == 0 {
		return nil
	}
	return fmt.Errorf("the census fell below its floor:\n    %s", strings.Join(problems, "\n    "))
}

// pendingWindowsAt counts the (key, window) pairs that are COMPLETE but UNFIRED
// at the cut checkpoint id records.
//
// Computed from the checkpoint itself rather than from a model of one. The
// entries under state.PrefixTimer are the windows still open at the cut, and
// the source offsets say where the replay begins, so the pairs that will
// receive nothing more are the difference between the two. That is only
// possible because Phase 3c put the timers in the checkpoint, which is a
// pleasant circularity and not a vacuous one: the count is over the
// checkpoint's contents and every assertion it gates is over the sink's.
//
// The timer layout is read from OUTSIDE pkg/operators on purpose, against the
// layout that package documents: the prefix and fire time are the first nine
// bytes, the window start is the last eight, and the record key is what lies
// between. A helper inside the operator would make this a reading of the
// operator's own opinion of its state.
func pendingWindowsAt(storage *checkpoint.Storage, id int64) (int, error) {
	_, payloads, err := storage.Load(id)
	if err != nil {
		return 0, fmt.Errorf("loading checkpoint %d: %w", id, err)
	}

	open := make(map[oracle.Key]bool)
	for index := range jobParallelism {
		payload, ok := payloads[checkpoint.SubtaskKey{VertexID: "window", Index: index}]
		if !ok {
			return 0, fmt.Errorf("checkpoint %d holds no state for window subtask %d", id, index)
		}
		st := state.NewMemory()
		if err := state.ReadFrom(st, bytes.NewReader(payload)); err != nil {
			return 0, fmt.Errorf("decoding window subtask %d of checkpoint %d: %w", index, id, err)
		}
		watermark := storedWatermark(st)
		var iterErr error
		st.Iterate(func(k, v []byte) bool {
			if len(k) == 0 || k[0] != state.PrefixTimer {
				return true
			}
			if len(k) < 1+state.OrderedInt64Bytes+windowStartBytes {
				iterErr = fmt.Errorf("checkpoint %d holds a %d-byte timer key", id, len(k))
				return false
			}
			// A timer in a checkpoint is unfired by construction: firing deletes
			// it, and firing runs to completion inside one ProcessWatermark.
			// Asserted rather than assumed, because it is also the check that
			// the watermark in the checkpoint is the one that was current at
			// the cut -- a stale one would make every count here wrong in the
			// direction that flatters the suite.
			if fireTime := state.DecodeOrderedInt64(k[1:]); fireTime <= watermark {
				iterErr = fmt.Errorf("checkpoint %d holds a timer due at %d against a stored watermark of %d: "+
					"it should already have fired", id, fireTime, watermark)
				return false
			}
			recordKey := string(k[1+state.OrderedInt64Bytes : len(k)-windowStartBytes])
			windowStart := int64(binary.BigEndian.Uint64(k[len(k)-windowStartBytes:]))
			open[oracle.Key{Key: recordKey, WindowStart: windowStart}] = true
			return true
		})
		if iterErr != nil {
			return 0, iterErr
		}
	}

	// The (key, window) pairs the replay will deliver more records into.
	fed := make(map[oracle.Key]bool)
	for _, src := range []struct {
		id  string
		cfg sources.GeneratorConfig
	}{{"srcA", sourceAConfig()}, {"srcB", sourceBConfig()}} {
		for index := range jobParallelism {
			payload, ok := payloads[checkpoint.SubtaskKey{VertexID: src.id, Index: index}]
			if !ok {
				return 0, fmt.Errorf("checkpoint %d holds no state for %s subtask %d", id, src.id, index)
			}
			offset, err := decodePosition(payload)
			if err != nil {
				return 0, fmt.Errorf("checkpoint %d, %s subtask %d: %w", id, src.id, index, err)
			}
			_, end := sourceRange(src.cfg.Count, jobParallelism, index)
			if err := markWindowsFrom(src.cfg, offset, end, fed); err != nil {
				return 0, err
			}
		}
	}

	pending := 0
	for k := range open {
		if !fed[k] {
			pending++
		}
	}
	return pending, nil
}

// windowStartBytes is the width of the window start at the tail of a timer key.
const windowStartBytes = 8

// storedWatermark reads the operator watermark out of a restored state, or the
// minimum if the subtask had processed no watermark when it snapshotted.
//
// The absence is legitimate. The gate's output watermark is the MINIMUM across
// its inputs, so it forwards nothing until every input has produced one, and a
// subtask can therefore process its first barrier having seen no watermark at
// all. The minimum is then the correct reading rather than a missing one: it is
// what the operator would have reported at that moment, and a checkpoint of it
// is a checkpoint of "nothing has been purged", which is true.
func storedWatermark(st state.KeyedState) int64 {
	v, ok := st.Get(append([]byte{state.PrefixOperatorState}, "watermark"...))
	if !ok {
		return -1 << 63
	}
	return state.DecodeOrderedInt64(v)
}

// decodePosition reads a source subtask's resume offset.
//
// The whole of a source subtask's checkpoint is one big-endian integer, which
// is what contiguous ranges bought in Phase 1. Written out here rather than
// shared with the runtime because it is unexported there, and exporting an
// eight-byte decode to let a test read a checkpoint would be widening the
// runtime's API for the reader rather than for the job.
func decodePosition(payload []byte) (int64, error) {
	if len(payload) != 8 {
		return 0, fmt.Errorf("source position payload is %d bytes, want 8", len(payload))
	}
	return int64(binary.BigEndian.Uint64(payload)), nil
}

// sourceRange is the half-open offset range subtask index of a source vertex at
// the given parallelism reads. It is runtime.sourceRange written out again, for
// the same reason decodePosition is.
func sourceRange(count int64, parallelism, index int) (start, end int64) {
	return int64(index) * count / int64(parallelism), int64(index+1) * count / int64(parallelism)
}

// markWindowsFrom marks every (key, window) the elements in [offset, end) of
// cfg belong to. It is the replay, read straight from the generator.
func markWindowsFrom(cfg sources.GeneratorConfig, offset, end int64, into map[oracle.Key]bool) error {
	src := sources.NewGenerator(cfg)
	if err := src.Open(nil); err != nil {
		return fmt.Errorf("opening the generator to replay [%d, %d): %w", offset, end, err)
	}
	defer func() { _ = src.Close() }()
	if err := src.SeekTo(offset); err != nil {
		return fmt.Errorf("seeking to %d: %w", offset, err)
	}
	for pos := offset; pos < end; pos++ {
		rec, ok, err := src.Next()
		if err != nil {
			return fmt.Errorf("reading element %d: %w", pos, err)
		}
		if !ok {
			break
		}
		start := rec.EventTime - floorMod(rec.EventTime, windowSize)
		into[oracle.Key{Key: string(rec.Key), WindowStart: start}] = true
	}
	return nil
}

// floorMod returns a mod b for b > 0, always in [0, b). Go's % takes the sign
// of the dividend, so a negative event time would land in the window above the
// one containing it.
func floorMod(a, b int64) int64 {
	m := a % b
	if m < 0 {
		m += b
	}
	return m
}
