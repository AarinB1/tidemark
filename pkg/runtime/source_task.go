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
// handing each to emitRecord and each due watermark to emitWatermark. It
// returns when the subtask's range is exhausted, when the source reports it has
// no more elements, or on the first error from the source, from either emitter,
// or from ctx.
//
// The record goes out before the watermark derived from it. Either order is
// safe, since a watermark of maxSeen-lag-1 does not bound the record that
// produced it, but records-then-watermark is the one that reads as what it is:
// the watermark summarises everything already sent.
//
// wm is taken by value. Each subtask drives its own generator over its own
// contiguous slice of the offset space, which is what produces the staircase
// documented on watermarkGenerator; a generator shared between subtasks would
// be a data race and would also collapse the per-subtask watermarks into one.
//
// The source must already be open: Open validates configuration, and a source
// that failed validation must not be seeked.
func sourceLoop(ctx context.Context, src core.Source, parallelism, index int, wm watermarkGenerator, emitRecord func(*core.Record) error, emitWatermark func(int64) error) error {
	end, bounded, err := seekToRange(src, parallelism, index)
	if err != nil {
		return err
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
func seekToRange(src core.Source, parallelism, index int) (end int64, bounded bool, err error) {
	s, ok := src.(splittableSource)
	if !ok {
		if parallelism > 1 {
			return 0, false, fmt.Errorf("source does not report a Count and cannot be split across %d subtasks", parallelism)
		}
		return 0, false, nil
	}
	start, end := sourceRange(s.Count(), parallelism, index)
	if err := src.SeekTo(start); err != nil {
		return 0, false, err
	}
	return end, true, nil
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
