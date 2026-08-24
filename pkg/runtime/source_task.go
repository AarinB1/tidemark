package runtime

import (
	"context"
	"fmt"
	"math"

	"github.com/AarinB1/tidemark/pkg/core"
)

// splittableSource is a core.Source whose offset space is finite and known in
// advance, so it can be divided among the subtasks of a parallel source vertex.
//
// This is not an abstraction layer held open for a later phase: a source vertex
// at parallelism P is being built now, and dividing an offset space requires
// knowing how large it is. core.Source deliberately does not carry Count,
// because an unbounded source has no answer to give; such a source is simply
// not splittable and is rejected at parallelism > 1 rather than guessed at.
//
// A decorator wrapping a core.Source MUST forward Count explicitly. Embedding
// the core.Source interface is not sufficient and is the trap:
//
//	type wrapper struct {         // WRONG: promotes only core.Source's
//		core.Source           // methods, so Count is not there and the
//	}                             // type assertion below fails.
//
//	type wrapper struct {         // Right: either forward Count by hand,
//		core.Source           // or embed the concrete source type so
//		count int64           // Count stays promoted.
//	}
//	func (w *wrapper) Count() int64 { return w.count }
//
// The failure is loud — the job is refused at parallelism > 1 rather than
// silently having every subtask read the whole input — but the trigger is easy
// to hit by accident, and the refusal names the missing Count rather than the
// wrapper that dropped it.
//
// Phase 4's chaos harness is the case this note exists for. It wraps sources to
// inject faults at logical positions, and a wrapper that loses splittability
// would force the entire fault suite to parallelism 1, which is precisely where
// the concurrency bugs it is meant to find do not appear. A fault suite that
// quietly stopped testing the interesting configuration would still pass.
type splittableSource interface {
	core.Source
	// Count returns the number of elements the source will produce from
	// offset 0.
	Count() int64
}

// sourceRange returns the half-open offset range [start, end) that subtask
// index of a source vertex at the given parallelism reads.
//
// The ranges are contiguous rather than strided (offset%P == index) because
// Phase 3 checkpoints one resume offset per subtask. A stride would have to
// store a stride and a phase alongside it; a contiguous range makes the whole
// of a source subtask's recovery state a single integer.
//
// Integer division absorbs the remainder without a special case: the ranges
// stay adjacent because each subtask's end is computed by the same expression
// that gives the next subtask's start, so the last of them lands exactly on
// count no matter how the remainder falls. Ranges are empty rather than
// negative when parallelism exceeds count, which is what makes a source vertex
// wider than its input harmless instead of wrong.
func sourceRange(count int64, parallelism, index int) (start, end int64) {
	start = int64(index) * count / int64(parallelism)
	end = int64(index+1) * count / int64(parallelism)
	return start, end
}

// sourceLoop reads the records assigned to one subtask of a source vertex,
// handing each to emitRecord, each due watermark to emitWatermark, and each due
// barrier to emitBarrier. It returns when the subtask's range is exhausted,
// when the source reports it has no more elements, or on the first error from
// the source, from any emitter, or from ctx.
//
// emitBarrier is handed the source's POSITION at the moment of injection,
// alongside the barrier. That position is the offset the next Next would
// return, so it is the resume offset for this checkpoint: every element below
// it belongs to the checkpoint being closed and every element from it onwards
// belongs to the next one. It is read here, between the injection decision and
// the emit, and not by the caller afterwards. A caller reading it later reads
// it after whatever has happened in between, and "whatever has happened in
// between" is nothing today and is a replayed or a skipped record the first
// time it is not.
//
// The record goes out before the watermark derived from it. Either order is
// safe, since a watermark of maxSeen-lag-1 does not bound the record that
// produced it, but records-then-watermark is the one that reads as what it is:
// the watermark summarises everything already sent. The barrier goes out last
// of the three, so it closes a checkpoint over exactly the elements already
// emitted.
//
// Both control elements go out on the SAME stream as the records, in the order
// shown, which is invariant 5. A barrier on a side channel could overtake the
// records it is meant to separate, and every element on the wrong side of it
// would be checkpointed into the wrong epoch.
//
// wm is taken by value. Each subtask drives its own generator over its own
// contiguous slice of the offset space, which is what produces the staircase
// documented on watermarkGenerator; a generator shared between subtasks would
// be a data race and would also collapse the per-subtask watermarks into one.
//
// The source must already be open: Open validates configuration, and a source
// that failed validation must not be seeked.
func sourceLoop(ctx context.Context, src core.Source, parallelism, index int, wm watermarkGenerator, barrierIntervalElements int64, resume *sourceResume, emitRecord func(*core.Record) error, emitWatermark func(int64) error, emitBarrier func(b *core.Barrier, position int64) error) error {
	start, end, bounded, count, err := seekToRange(src, parallelism, index)
	if err != nil {
		return err
	}
	br := newBarrierGenerator(barrierIntervalElements, maxBarriers(count, parallelism, barrierIntervalElements, bounded))
	if resume != nil {
		if err := resume.apply(src, &br, start, end, bounded); err != nil {
			return err
		}
	}

	for {
		// Checked every element so that a subtask whose consumer is keeping up,
		// and which therefore never blocks in emit, still notices a cancelled
		// job.
		if err := ctx.Err(); err != nil {
			return err
		}
		if bounded && src.Position() >= end {
			return nil
		}
		rec, ok, err := src.Next()
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		if err := emitRecord(rec); err != nil {
			return err
		}
		if t, ok := wm.onRecord(rec.EventTime); ok {
			if err := emitWatermark(t); err != nil {
				return err
			}
		}
		// After the watermark, so that a barrier injected on the same element
		// carries the watermark that element produced rather than the previous
		// one. Nothing depends on the value, but "the last watermark this
		// subtask emitted" has to mean one thing rather than two.
		if id, ok := br.onRecord(); ok {
			// Captured at the injection point. See the note on this function.
			position := src.Position()
			if err := emitBarrier(&core.Barrier{CheckpointID: id, Timestamp: wm.lastEmitted}, position); err != nil {
				return err
			}
		}
	}
}

// seekToRange positions src at the start of subtask index's range and returns
// the offset it must stop at.
//
// A source that does not report a Count cannot be divided. At parallelism 1
// there is nothing to divide, so it is read to exhaustion and bounded is false;
// above parallelism 1 there is no safe reading of the request and it is an
// error rather than a silent read of the whole input by every subtask, which
// would duplicate every record P times.
func seekToRange(src core.Source, parallelism, index int) (start, end int64, bounded bool, count int64, err error) {
	s, ok := src.(splittableSource)
	if !ok {
		if parallelism > 1 {
			return 0, 0, false, 0, fmt.Errorf("source does not report a Count and cannot be split across %d subtasks", parallelism)
		}
		return 0, 0, false, 0, nil
	}
	count = s.Count()
	start, end = sourceRange(count, parallelism, index)
	if err := src.SeekTo(start); err != nil {
		return 0, 0, false, 0, err
	}
	return start, end, true, count, nil
}

// sourceResume is where a restored source subtask picks up.
//
// The whole of a source subtask's recovery state is one offset, which is what
// contiguous ranges bought in Phase 1; barriersInjected is not stored anywhere
// on disk but derived from the checkpoint ID, because a subtask injects
// barrier k as its k-th barrier and IDs are contiguous from 1 within a subtask.
// That identity is invariant 3 read from the other end: injection at a fixed
// element interval is what makes the k-th barrier and checkpoint k the same
// thing.
type sourceResume struct {
	position         int64
	barriersInjected int64
}

// apply seeks src to the resume offset and restores the barrier count.
//
// The offset is checked against the subtask's own range. It must be inside it:
// an offset from another subtask's range would have this subtask read records
// that belong to a sibling, so the two would duplicate each other's work and
// leave a hole where this one should have been. The metadata check on restore
// is what normally prevents that, and this is the assertion that the two
// mechanisms agree rather than an alternative to it. The end of the range is
// allowed, and means the subtask had finished when the checkpoint was taken.
func (r *sourceResume) apply(src core.Source, br *barrierGenerator, start, end int64, bounded bool) error {
	if bounded && (r.position < start || r.position > end) {
		return fmt.Errorf("restored position %d is outside this subtask's range [%d, %d)", r.position, start, end)
	}
	if r.position < 0 {
		return fmt.Errorf("restored position %d is negative", r.position)
	}
	if err := src.SeekTo(r.position); err != nil {
		return err
	}
	// Continue the numbering rather than restarting it. A resumed run emitting
	// a second barrier 1 at a different logical position would hand the
	// coordinator two different cuts under one name.
	br.injected = r.barriersInjected
	br.sinceLast = 0
	return nil
}

// unboundedBarriers is the barrier budget of a source that reports no Count.
//
// Such a source is refused above parallelism 1 by seekToRange, so a job that
// has one has exactly one source subtask on that vertex and there is no second
// subtask for it to agree with. It injects a barrier every interval elements
// for as long as it produces them.
const unboundedBarriers = -1

// maxBarriers is how many barriers EVERY subtask of a source vertex injects.
//
// Contiguous ranges are equal in length only up to integer division: count 10
// at parallelism 4 gives ranges of 2, 3, 2 and 3. A subtask injecting a barrier
// every interval elements of its OWN range would therefore inject more barriers
// from a longer range than from a shorter one, and a downstream operator
// aligning on the last one would wait for a barrier that no subtask is ever
// going to send. Alignment has no timeout and nothing reports an error: the job
// simply stops producing output, which is the failure mode invariant 3 is
// written against.
//
// So the budget comes from the FLOOR of count/parallelism, which every range is
// at least as long as, and every subtask gets that same number. Elements past
// it in a longer range still flow; they carry no barrier.
func maxBarriers(count int64, parallelism int, intervalElements int64, bounded bool) int64 {
	if !bounded {
		return unboundedBarriers
	}
	if intervalElements <= 0 {
		return 0
	}
	return (count / int64(parallelism)) / intervalElements
}

// barrierGenerator decides when a source subtask injects a checkpoint barrier.
//
// The interval is counted in ELEMENTS and never on a wall clock. This is
// invariant 3, and invariant 6 is the general rule behind it: a barrier
// injected on a timer lands at a different logical position on every run, so a
// recovered run cannot be compared against a clean one and a fault schedule
// keyed to "after the second barrier" means a different thing each time. It is
// also why injection does not depend on data volume: a quiet source that
// stopped injecting would stall alignment everywhere downstream of it.
//
// Checkpoint IDs start at 1 and are contiguous within a subtask. Every subtask
// of a vertex counts within its own range, so barrier k is injected after
// k*interval of that subtask's elements, and all of them stop at the same
// budget; see maxBarriers.
//
// The generator holds a count and a budget and nothing else, which is what
// makes an injection point a pure function of the subtask's element index.
type barrierGenerator struct {
	intervalElements int64
	// maxBarriers is the number this subtask will inject, or unboundedBarriers.
	maxBarriers int64

	sinceLast int64
	injected  int64
}

func newBarrierGenerator(intervalElements, maxBarriers int64) barrierGenerator {
	return barrierGenerator{intervalElements: intervalElements, maxBarriers: maxBarriers}
}

// onRecord counts one element and reports the checkpoint ID to inject, if any.
// The caller emits the record first, so a barrier never precedes an element
// belonging to the checkpoint it closes.
func (g *barrierGenerator) onRecord() (checkpointID int64, ok bool) {
	if g.intervalElements <= 0 {
		return 0, false
	}
	if g.maxBarriers != unboundedBarriers && g.injected >= g.maxBarriers {
		return 0, false
	}
	g.sinceLast++
	if g.sinceLast < g.intervalElements {
		return 0, false
	}
	g.sinceLast = 0
	g.injected++
	return g.injected, true
}

// watermarkGenerator decides when a source subtask emits a watermark and what
// value it carries.
//
// Generation lives in the source runner rather than on core.Source. Every
// source needs a watermark and the policy is uniform across all of them, so a
// method on the interface would be polymorphism with exactly one behaviour
// behind it. core.Source is already at the size CLAUDE.md justifies, and an
// interface method added to support one implementation is the abstraction the
// scope rules forbid.
//
// The interval is counted in ELEMENTS. Not on a ticker, not on a wall clock,
// and not "at least every so often". A time-based interval makes the position
// of every watermark within the element stream a function of the Go scheduler,
// so the same seed replays a different stream and a recovered run cannot be
// compared against a clean one. Invariant 6 states the general rule; this is
// its first instance.
//
// There is deliberately no final MaxInt64 watermark at end of input. The gate
// emits that once every one of its inputs has finished. Two mechanisms for one
// job means neither gets exercised properly by any test, and the one that
// silently stopped working would take the tail windows with it.
//
// # Known consequence: the staircase
//
// Source subtasks split the offset space into contiguous ranges, and the
// generator derives event time as Base + n*Step - lag(n). Subtask 0 therefore
// covers the earliest event times and subtask P-1 the latest. At parallelism P
// the downstream gate's minimum is pinned near subtask 0's watermark until
// subtask 0 exhausts its range, so event time advances in a staircase rather
// than smoothly, and window state peaks near the size of the whole dataset
// instead of near one window's worth.
//
// This is correct. Final sink contents are unaffected, because the gate's
// MaxInt64 at end of input flushes whatever is still open. What it means is
// that checkpoint size in Phase 3 and the state-size target in Phase 6 have to
// be measured against this topology deliberately rather than reasoned about
// from window size, and that a strided source split would change the number.
// Do not "fix" it here by reordering the split: contiguous ranges are what make
// a source subtask's recovery state a single integer.
type watermarkGenerator struct {
	// intervalElements is the number of records between two emissions. Zero or
	// negative disables generation entirely, which is what a source in a job
	// that does no event-time work wants.
	intervalElements int64
	// maxOutOfOrderness is how far behind the maximum observed event time a
	// watermark is held, in millis.
	maxOutOfOrderness int64

	sinceLast    int64
	maxEventTime int64
	lastEmitted  int64
}

// newWatermarkGenerator returns a generator that has observed nothing.
func newWatermarkGenerator(intervalElements, maxOutOfOrderness int64) watermarkGenerator {
	return watermarkGenerator{
		intervalElements:  intervalElements,
		maxOutOfOrderness: maxOutOfOrderness,
		// Nothing has been observed and nothing has been emitted. Starting
		// either at zero would claim that every event before 1970 had arrived.
		maxEventTime: math.MinInt64,
		lastEmitted:  math.MinInt64,
	}
}

// onRecord observes one element's event time and reports the watermark to emit,
// if any. The caller emits the record first and the watermark after it, so a
// watermark never precedes a record it is meant to bound.
//
// ok is false far more often than it is true: a watermark is due only every
// intervalElements records, and even then only when the value it would carry is
// strictly greater than the last one emitted. Re-emitting an unchanged
// watermark costs a broadcast to every downstream channel and tells nobody
// anything.
func (g *watermarkGenerator) onRecord(eventTime int64) (wm int64, ok bool) {
	if eventTime > g.maxEventTime {
		g.maxEventTime = eventTime
	}
	if g.intervalElements <= 0 {
		return 0, false
	}

	// Counted, and reset, on every interval boundary whether or not a watermark
	// comes out of it. The interval is a count of records, not a count of
	// emissions: making it the latter would bunch emissions together after a
	// stretch of out-of-order data.
	g.sinceLast++
	if g.sinceLast < g.intervalElements {
		return 0, false
	}
	g.sinceLast = 0

	// Minus one because a watermark asserts that no element with event time
	// <= w will arrive, and an element at exactly maxObserved-maxOutOfOrderness
	// still may.
	candidate := subFloor(subFloor(g.maxEventTime, g.maxOutOfOrderness), 1)
	if candidate <= g.lastEmitted {
		return 0, false
	}
	g.lastEmitted = candidate
	return candidate, true
}

// subFloor returns a - b, clamped to the int64 range rather than wrapping.
//
// A wrap here is the worst class of bug this engine has. Subtracting a lag from
// a very negative event time wraps to a large POSITIVE watermark, which fires
// every open window at once and produces a plausible-looking wrong answer with
// no error anywhere. Clamping holds the value at the minimum instead, which
// emits nothing at all, because MinInt64 is never strictly greater than the
// initial last-emitted value.
func subFloor(a, b int64) int64 {
	d := a - b
	switch {
	case b < 0 && d < a:
		return math.MaxInt64
	case b > 0 && d > a:
		return math.MinInt64
	default:
		return d
	}
}
