package runtime

import (
	"context"
	"fmt"

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
// handing each to emit. It returns when the subtask's range is exhausted, when
// the source reports it has no more elements, or on the first error from the
// source, from emit, or from ctx.
//
// The source must already be open: Open validates configuration, and a source
// that failed validation must not be seeked.
func sourceLoop(ctx context.Context, src core.Source, parallelism, index int, emit func(*core.Record) error) error {
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
		if err := emit(rec); err != nil {
			return err
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
