package chaos

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/AarinB1/tidemark/pkg/checkpoint"
	"github.com/AarinB1/tidemark/pkg/core"
	"github.com/AarinB1/tidemark/pkg/operators"
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

// The oracle is structurally blind to timing, and five hundred comparisons
// against it inherit that blindness exactly.
//
// Phase 2 established the shape of it: freezing an exhausted input at its last
// watermark instead of dropping it out of the minimum cannot change the final
// contents of the sink, because the gate's MaxInt64 at end of input flushes
// whatever is still open and fixes everything up. Every window comes out with
// the right count. What changes is WHEN the windows fired and how much state
// the run was holding while they did not, and neither is a thing the oracle
// has an opinion about.
//
// So two numbers are recorded that the oracle cannot see, and both inflate
// under exactly that class of bug: a watermark that advances too slowly leaves
// windows open to the end, which raises the share of firings that come from the
// flush and raises the peak state along with it.
//
// They are asserted on the AGGREGATE across the suite and never per run.
// Recovery legitimately raises the flush fraction -- a resumed run re-fires
// windows against a watermark that starts again from the minimum -- so a
// per-run bound would be a bound on how many faults a seed happened to draw.

// Timing is what one run, or one schedule's runs together, did in time rather
// than in contents.
type Timing struct {
	// FlushFirings is how many windows fired on the end-of-input MaxInt64
	// watermark, and WatermarkFirings how many fired on an advancing one.
	FlushFirings     int64
	WatermarkFirings int64
	// OtherEmissions is anything this operator emitted outside
	// ProcessWatermark. It is zero today -- WindowCount emits only from fire,
	// which only ProcessWatermark calls -- and it is counted rather than
	// assumed so that an operator that started emitting elsewhere would show up
	// here instead of silently unbalancing the fraction above.
	OtherEmissions int64
	// PeakStateEntries is the greatest number of KeyedState entries the
	// operator subtasks held between them at one moment.
	PeakStateEntries int64
}

// firings is the denominator of the flush fraction.
func (t Timing) firings() int64 { return t.FlushFirings + t.WatermarkFirings }

// add folds another run's timing in: the firings sum, the peak is the greater.
//
// Summing the firings across the attempts of one schedule is what makes the
// aggregate a fraction of every window this schedule fired, replay included. A
// peak is not a sum: two runs each holding a thousand entries never held two
// thousand, because the first had unwound before the second started.
func (t *Timing) add(o Timing) {
	t.FlushFirings += o.FlushFirings
	t.WatermarkFirings += o.WatermarkFirings
	t.OtherEmissions += o.OtherEmissions
	if o.PeakStateEntries > t.PeakStateEntries {
		t.PeakStateEntries = o.PeakStateEntries
	}
}

// TimingAggregate is the suite's timing, clean runs and fault runs apart.
//
// Apart, because the difference between them is more informative than either
// alone. A clean run and a recovered one hold the same final contents by
// construction; if they also held the same peak state and fired the same share
// of their windows on the flush, recovery would be costing nothing, and it does
// cost something. The two columns are what makes that visible instead of
// averaged away.
type TimingAggregate struct {
	Schedules int
	Clean     Timing
	Fault     Timing
	// PeakStateSum is the total of the per-schedule peaks, from which the mean
	// is taken. The mean rather than the maximum: one schedule that recovered
	// three times has a peak the other four hundred and ninety-nine say nothing
	// about, and a baseline pinned to it would be a baseline pinned to the
	// unluckiest seed.
	CleanPeakStateSum int64
	FaultPeakStateSum int64
}

// AddTiming folds one schedule's timing into the aggregate.
func (a *TimingAggregate) AddTiming(r Result) {
	a.Schedules++
	a.Clean.add(r.CleanTiming)
	a.Fault.add(r.FaultTiming)
	a.CleanPeakStateSum += r.CleanTiming.PeakStateEntries
	a.FaultPeakStateSum += r.FaultTiming.PeakStateEntries
}

// FlushFraction is the share of windows that fired on the end-of-input flush.
func (t Timing) FlushFraction() float64 {
	if t.firings() == 0 {
		return 0
	}
	return float64(t.FlushFirings) / float64(t.firings())
}

// MeanPeakState is the mean per-schedule peak entry count.
func (a TimingAggregate) MeanPeakState(sum int64) float64 {
	if a.Schedules == 0 {
		return 0
	}
	return float64(sum) / float64(a.Schedules)
}

// Table renders the timing aggregate for a person to read.
func (a TimingAggregate) Table() string {
	var b strings.Builder
	fmt.Fprintf(&b, "\ntiming over %d schedules            clean        fault\n", a.Schedules)
	fmt.Fprintf(&b, "  %-28s %10d %12d\n", "windows fired on a watermark", a.Clean.WatermarkFirings, a.Fault.WatermarkFirings)
	fmt.Fprintf(&b, "  %-28s %10d %12d\n", "windows fired on the flush", a.Clean.FlushFirings, a.Fault.FlushFirings)
	fmt.Fprintf(&b, "  %-28s %9.1f%% %11.1f%%\n", "flush fraction", 100*a.Clean.FlushFraction(), 100*a.Fault.FlushFraction())
	fmt.Fprintf(&b, "  %-28s %10.1f %12.1f\n", "mean peak state entries",
		a.MeanPeakState(a.CleanPeakStateSum), a.MeanPeakState(a.FaultPeakStateSum))
	fmt.Fprintf(&b, "  %-28s %10d %12d\n", "emissions outside a watermark", a.Clean.OtherEmissions, a.Fault.OtherEmissions)
	return b.String()
}

// TimingBand is one baseline figure and the relative band the suite is allowed
// around it.
//
// Per figure rather than one tolerance for all four, because the four do not
// vary alike and a single number would have to be the widest of them.
//
// ASYMMETRIC, and the peaks are why. The failure this phase's timing metrics
// are aimed at is inflation -- a watermark that stalls holds its windows to the
// end of the run -- so the side of the band that has to be tight is the one
// above the baseline. The side below is only there for the opposite bug, a
// watermark that runs ahead, which the flush fraction catches far more sharply
// than the peak does. So the peaks are given room below, where the noise is
// (the race detector alone moves them four per cent), and held close above,
// where the signal is.
//
// The peak metric has little room to move up in the first place, and that is
// worth knowing rather than hiding. This workload's peak sits at about
// seven-eighths of every window it will ever open being open at once, which is
// the staircase again; a watermark that stalled completely could raise it by
// about a seventh and no more. The flush fraction is the load-bearing half of
// this pair.
type TimingBand struct {
	Baseline float64
	// Below and Above are the relative distances the figure may fall or rise
	// from the baseline.
	Below float64
	Above float64
}

func (b TimingBand) lo() float64 { return b.Baseline * (1 - b.Below) }
func (b TimingBand) hi() float64 { return b.Baseline * (1 + b.Above) }

// TimingBaseline is what the suite's timing aggregate is held to.
//
// A committed constant with a band, the way test/bench/baseline.json works. The
// values are observations and not targets: they are what this workload does,
// and the assertion is that it keeps doing it.
//
// # Why both sides are bounded
//
// The upper bound is the watermark that advances too slowly: windows stay open
// to the end, the flush fires them, and the peak state carries the whole run.
// That is the failure freezing an exhausted input at its last watermark
// produces, and it is invisible to the oracle -- the MaxInt64 flush fixes the
// contents up at the end, so every count comes out right.
//
// The lower bound is the opposite failure and is worth as much. A watermark
// that runs AHEAD -- a gate taking the maximum over its inputs rather than the
// minimum -- fires windows early, so almost nothing is left for the flush and
// the fraction collapses. The oracle usually catches that one through the
// counts, and a band that only bounded one side would be leaving the cheaper
// detection on the floor.
type TimingBaseline struct {
	CleanFlushFraction TimingBand
	FaultFlushFraction TimingBand
	CleanPeakState     TimingBand
	FaultPeakState     TimingBand
}

// flushToleranceSmallSuite and flushToleranceFullSuite are the bands the two
// flush fractions get, and smallSuite is where one becomes the other.
//
// Two regimes because the figure has two variances, not because a tight number
// was inconvenient. Over five hundred schedules the clean flush fraction lands
// in a range of about 0.015; over twenty-five it ranges by 0.14, which is very
// nearly ten times as wide. That is what a mean over twenty-five samples of a
// concurrent job does, and no amount of care in the instrument changes it:
// whether a window fires before the input ends depends on how far the sources
// ran ahead of the operator, and that is the Go scheduler's to decide.
//
// This is the shape the throughput check already uses -- fifteen per cent
// locally against forty in CI, because "a tight threshold there produces false
// alarms that train you to ignore the check". The wide band is still an
// assertion. What it is aimed at moves this fraction to nearly zero or nearly
// one: a gate taking the maximum fires every window early, and one that freezes
// an exhausted input at its last watermark fires almost none of them until the
// end-of-input flush. Neither lands inside a band of 0.128 to 0.512.
const (
	flushToleranceFullSuite  = 0.20
	flushToleranceSmallSuite = 0.60
	smallSuite               = 200
)

// SuiteTimingBaseline is the committed baseline for this workload over the
// given number of schedules.
//
// Observed over runs of both sizes, with zero divergence from the oracle
// throughout:
//
//	                         500 schedules      25 schedules   baseline        band
//	clean flush fraction   0.3117 - 0.3510   0.2176 - 0.4040      0.320       below
//	fault flush fraction   0.3122 - 0.3382   0.1648 - 0.4077      0.320       below
//	clean mean peak state  3807.6 - 3840.0   3628.3 - 3877.8       3810   -15% +8%
//	fault mean peak state  3925.6 - 3941.7   3727.5 - 3969.3       3930   -15% +8%
//
// The twenty-five-schedule column spans runs with AND without the race
// detector, which is what CI runs at that size. The detector costs the peaks
// about four per cent -- everything is slower, so the sources run less far
// ahead of the operator and less is open at once -- and that is the whole
// reason those bands are given room below.
//
// The flush band is +-20% at or above two hundred schedules and +-60% below it;
// see the constants above for why.
//
// Roughly a third of this workload's windows fire on the end-of-input flush.
// That is the staircase runtime.watermarkGenerator documents, showing up as a
// number: source subtasks cover contiguous event-time ranges, so the gate's
// minimum sits at the earliest subtask's watermark until that subtask exhausts
// its range, and the windows above it are still open when the input ends. It is
// expected, it is recorded here so that it stops being a surprise, and it is
// why the metric is a baseline rather than a target of zero.
func SuiteTimingBaseline(schedules int) TimingBaseline {
	flush := flushToleranceFullSuite
	if schedules < smallSuite {
		flush = flushToleranceSmallSuite
	}
	return TimingBaseline{
		CleanFlushFraction: TimingBand{Baseline: 0.320, Below: flush, Above: flush},
		FaultFlushFraction: TimingBand{Baseline: 0.320, Below: flush, Above: flush},
		CleanPeakState:     TimingBand{Baseline: 3810, Below: 0.15, Above: 0.08},
		FaultPeakState:     TimingBand{Baseline: 3930, Below: 0.15, Above: 0.08},
	}
}

// Check reports every way a's timing leaves the baseline's bands, or nil.
func (a TimingAggregate) Check(b TimingBaseline) error {
	if a.Schedules == 0 {
		return fmt.Errorf("the timing aggregate covers no schedules at all")
	}
	// The guard the floors have, in the shape timing needs it. A decorator that
	// silently stopped counting -- an operator wrapper that lost a method, a
	// factory handed no tally -- leaves every band below comparing zero against
	// a baseline, and a zero flush fraction would read as a failure of the
	// engine rather than of the instrument. Named here instead.
	if a.Clean.firings() == 0 || a.Fault.firings() == 0 {
		return fmt.Errorf("the timing aggregate recorded %d clean firings and %d fault firings: "+
			"nothing was instrumented, so every band below is a comparison against zero",
			a.Clean.firings(), a.Fault.firings())
	}
	var problems []string
	if a.Clean.OtherEmissions != 0 || a.Fault.OtherEmissions != 0 {
		problems = append(problems, fmt.Sprintf("the operator emitted %d records outside ProcessWatermark: "+
			"the flush fraction is a fraction of the wrong denominator",
			a.Clean.OtherEmissions+a.Fault.OtherEmissions))
	}
	band := func(name string, got float64, want TimingBand) {
		if got < want.lo() || got > want.hi() {
			problems = append(problems, fmt.Sprintf("%s: %.4f, baseline %.4f, band [%.4f, %.4f]",
				name, got, want.Baseline, want.lo(), want.hi()))
		}
	}
	band("clean flush fraction", a.Clean.FlushFraction(), b.CleanFlushFraction)
	band("fault flush fraction", a.Fault.FlushFraction(), b.FaultFlushFraction)
	band("clean mean peak state", a.MeanPeakState(a.CleanPeakStateSum), b.CleanPeakState)
	band("fault mean peak state", a.MeanPeakState(a.FaultPeakStateSum), b.FaultPeakState)
	if len(problems) == 0 {
		return nil
	}
	return fmt.Errorf("the timing aggregate left its baseline:\n    %s", strings.Join(problems, "\n    "))
}

// firingTally accumulates one run's window firings, attributed to the
// watermark that released them.
//
// The runtime calls an operator from one goroutine, but a job has two operator
// subtasks and they share one tally, so it locks. The lock is taken once per
// ProcessWatermark and never on the record path.
type firingTally struct {
	mu sync.Mutex
	t  Timing
}

// atWatermark records that n windows were emitted while processing wm.
//
// math.MaxInt64 is the end-of-input flush and nothing else reaches it. No
// source emits it -- see runtime.watermarkGenerator -- and the gate produces it
// exactly when every one of its inputs has finished, because the minimum over
// an empty set of live inputs IS the maximum int64. So the test is an equality
// rather than a threshold: "no live input can contradict me" and "event time is
// over" are the same statement, and a threshold would quietly also catch a
// watermark that merely got very large.
func (f *firingTally) atWatermark(wm int64, n int64) {
	if n == 0 {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if wm == math.MaxInt64 {
		f.t.FlushFirings += n
	} else {
		f.t.WatermarkFirings += n
	}
}

// elsewhere records emissions from anywhere that is not ProcessWatermark.
func (f *firingTally) elsewhere(n int64) {
	if n == 0 {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.t.OtherEmissions += n
}

func (f *firingTally) timing() Timing {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.t
}

// countingContext is a core.Context that counts what an operator emits through
// it and forwards everything else.
//
// Every method is forwarded EXPLICITLY rather than promoted from an embedded
// core.Context. Embedding would compile and would leave a decorator that
// silently stopped counting the moment core.Context grew a method -- which is
// the trap CLAUDE.md records against decorators of core.Source, in a package
// where the consequence is a fault suite that quietly measures nothing.
type countingContext struct {
	inner   core.Context
	emitted int64
}

var _ core.Context = (*countingContext)(nil)

func (c *countingContext) Emit(rec *core.Record) {
	c.emitted++
	c.inner.Emit(rec)
}

func (c *countingContext) CurrentWatermark() int64 { return c.inner.CurrentWatermark() }
func (c *countingContext) State() state.KeyedState { return c.inner.State() }

// firingRecorder wraps a WindowCount and attributes each window it emits to the
// watermark that fired it.
//
// The operator emits only from fire, and only ProcessWatermark calls fire, so
// counting the emissions of one ProcessWatermark call is counting the windows
// that watermark released. That is a property of WindowCount today rather than
// of core.Operator, which is why the other two element paths are wrapped as
// well and their emissions counted separately: if it stopped being true, the
// number would move to OtherEmissions and the suite would say so, instead of
// the fraction quietly becoming a fraction of the wrong total.
//
// Every method is forwarded explicitly, for the reason on countingContext.
type firingRecorder struct {
	inner *operators.WindowCount
	tally *firingTally
}

var _ core.Operator = (*firingRecorder)(nil)

func (f *firingRecorder) Open(ctx core.Context) error {
	c := &countingContext{inner: ctx}
	err := f.inner.Open(c)
	f.tally.elsewhere(c.emitted)
	return err
}

func (f *firingRecorder) ProcessElement(rec *core.Record, ctx core.Context) error {
	c := &countingContext{inner: ctx}
	err := f.inner.ProcessElement(rec, c)
	f.tally.elsewhere(c.emitted)
	return err
}

func (f *firingRecorder) ProcessWatermark(wm int64, ctx core.Context) error {
	c := &countingContext{inner: ctx}
	err := f.inner.ProcessWatermark(wm, c)
	f.tally.atWatermark(wm, c.emitted)
	return err
}

func (f *firingRecorder) OnEndOfStream(ctx core.Context) error {
	c := &countingContext{inner: ctx}
	err := f.inner.OnEndOfStream(c)
	f.tally.elsewhere(c.emitted)
	return err
}

func (f *firingRecorder) Snapshot(w io.Writer) error { return f.inner.Snapshot(w) }
func (f *firingRecorder) Restore(r io.Reader) error  { return f.inner.Restore(r) }
func (f *firingRecorder) Close() error               { return f.inner.Close() }

// stateMeter tracks the entries the operator subtasks of one run hold between
// them, and the greatest number they held at once.
//
// Shared across the subtasks and read from the test goroutine afterwards, so it
// is atomic rather than locked: the increment is on the record path, once per
// new (key, window) and once per timer, and a mutex there would be paying for
// contention on every window a record opens.
type stateMeter struct {
	current atomic.Int64
	peak    atomic.Int64
}

func (m *stateMeter) inc() {
	n := m.current.Add(1)
	for {
		p := m.peak.Load()
		if n <= p || m.peak.CompareAndSwap(p, n) {
			return
		}
	}
}

func (m *stateMeter) dec() { m.current.Add(-1) }

// newState is what runtime.Options.NewState is set to.
func (m *stateMeter) newState() (state.KeyedState, error) {
	return &countingState{inner: state.NewMemory(), meter: m}, nil
}

// countingState is a KeyedState that keeps its entry count.
//
// Put and Delete each read the key first to tell an insert from an update and a
// removal from a no-op. That doubles the reads on the record path, which is a
// real cost and is the reason this decorator is confined to the chaos suite:
// throughput is secondary here and the number is not obtainable any other way,
// since KeyedState deliberately has no Len.
//
// The purge path deletes entries from inside an Iterate callback, and those
// deletes come back through this Delete because the operator holds this type as
// its state. That is what makes the count fall when windows are purged rather
// than only ever rising.
type countingState struct {
	inner state.KeyedState
	meter *stateMeter
}

var _ state.KeyedState = (*countingState)(nil)

func (c *countingState) Get(key []byte) ([]byte, bool) { return c.inner.Get(key) }

func (c *countingState) Put(key, value []byte) {
	_, existed := c.inner.Get(key)
	c.inner.Put(key, value)
	if !existed {
		c.meter.inc()
	}
}

func (c *countingState) Delete(key []byte) {
	_, existed := c.inner.Get(key)
	c.inner.Delete(key)
	if existed {
		c.meter.dec()
	}
}

func (c *countingState) Iterate(fn func(key, value []byte) bool) { c.inner.Iterate(fn) }
func (c *countingState) Err() error                              { return c.inner.Err() }
