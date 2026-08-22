// Package oracle computes windowed aggregates by brute force, as the thing the
// engine's answers are checked against.
//
// It imports pkg/core and pkg/sources and nothing else from this repository. No
// gate, no timer, no watermark, no operator, no transport: if it needed any of
// those it would be a second implementation of the engine, and two
// implementations of the same design agree about the same mistakes. What is
// under test is the windowing and the aggregation, so those are what this
// package writes out again from scratch.
//
// The input generator is shared rather than re-derived. The engine and the
// oracle have to read the SAME records for a comparison between them to mean
// anything, so re-implementing splitmix64 here would not add independence: it
// would test the generator against a copy of itself and leave the two sides
// free to disagree about their input, which shows up as a diff in the result
// that has nothing to do with windowing.
package oracle

import (
	"fmt"
	"slices"
	"strings"

	"github.com/AarinB1/tidemark/pkg/core"
	"github.com/AarinB1/tidemark/pkg/sources"
)

// Spec is the windowing to compute. Slide equal to Size is a tumbling window.
type Spec struct {
	Size  int64
	Slide int64
}

// Key identifies one result row.
type Key struct {
	Key         string
	WindowStart int64
}

// Triple is one result row flattened for comparison.
type Triple struct {
	Key         string
	WindowStart int64
	Count       int64
}

// check rejects a specification this package will not compute, matching the
// window operator's constructor. A size that is not a whole multiple of the
// slide would give windows whose membership count varies with the event time.
func (s Spec) check() error {
	switch {
	case s.Size <= 0:
		return fmt.Errorf("oracle: size is %d, must be > 0", s.Size)
	case s.Slide <= 0:
		return fmt.Errorf("oracle: slide is %d, must be > 0", s.Slide)
	case s.Size%s.Slide != 0:
		return fmt.Errorf("oracle: size %d is not a multiple of slide %d", s.Size, s.Slide)
	}
	return nil
}

// Counts returns the number of records in every non-empty (key, window) the
// generator described by cfg produces.
//
// It takes the configuration rather than a slice of records, so the input is
// regenerated here instead of handed over. A materialised slice would be a
// second thing that could be wrong, and at the counts Phase 4 runs it would be
// the memory high-water mark of the whole test.
//
// There is no sort anywhere. Each record is assigned to its windows and
// accumulated into a map as it is read: O(n) for tumbling and O(n * size/slide)
// for sliding, in one pass, with no ordering assumption about the input at all.
// Sorting by event time and sweeping would be the obvious way to write this and
// would cost an n log n that Phase 4 pays 500 times inside a CI budget. It
// would also make the oracle care about order, which the thing it is checking
// explicitly does not.
//
// There is no lateness model either, deliberately. The window operator drops a
// record whose window has been purged; nothing here can, because nothing here
// has a watermark. The equivalence tests are run at a lateness where no record
// is ever late, so the two agree; the drop path is checked against a
// hand-written input in pkg/operators instead. An oracle complicated enough to
// model lateness is not an oracle.
func Counts(cfg sources.GeneratorConfig, spec Spec) (map[Key]int64, error) {
	if err := spec.check(); err != nil {
		return nil, err
	}

	src := sources.NewGenerator(cfg)
	if err := src.Open(nil); err != nil {
		return nil, fmt.Errorf("oracle: open: %w", err)
	}
	defer func() { _ = src.Close() }()

	counts := make(map[Key]int64)
	for {
		rec, ok, err := src.Next()
		if err != nil {
			return nil, fmt.Errorf("oracle: next: %w", err)
		}
		if !ok {
			return counts, nil
		}
		accumulate(counts, rec, spec)
	}
}

// countRecords is Counts over records already in hand. It exists so the oracle
// itself can be checked against a fixture written out by a person: the oracle
// is what everything else is compared against, so it is the one place where the
// expected values must not be computed by any code in this repository, and
// nobody can compute a generator's output in their head.
func countRecords(recs []*core.Record, spec Spec) (map[Key]int64, error) {
	if err := spec.check(); err != nil {
		return nil, err
	}
	counts := make(map[Key]int64)
	for _, rec := range recs {
		accumulate(counts, rec, spec)
	}
	return counts, nil
}

// accumulate adds one record to every window it belongs to.
//
// This is a deliberate second writing of the assignment in
// pkg/operators/window.go, not a call into it. It is the logic under test, so
// sharing it would make the comparison vacuous.
func accumulate(counts map[Key]int64, rec *core.Record, spec Spec) {
	key := string(rec.Key)
	start := rec.EventTime - floorMod(rec.EventTime, spec.Slide)
	for n := spec.Size / spec.Slide; n > 0; n-- {
		counts[Key{Key: key, WindowStart: start}]++
		start -= spec.Slide
	}
}

// floorMod returns a mod b for b > 0, always in [0, b). Go's % takes the sign
// of the dividend, so a negative event time needs the correction.
func floorMod(a, b int64) int64 {
	m := a % b
	if m < 0 {
		m += b
	}
	return m
}

// Sorted flattens counts into triples ordered by key bytes and then window
// start.
//
// The sort is for comparison and is not part of the computation: the engine
// emits its windows in whatever order its subtasks finished them, and the sink
// contents are a set. Comparing emission order would be a broken test.
func Sorted(counts map[Key]int64) []Triple {
	out := make([]Triple, 0, len(counts))
	for k, n := range counts {
		out = append(out, Triple{Key: k.Key, WindowStart: k.WindowStart, Count: n})
	}
	slices.SortFunc(out, CompareTriples)
	return out
}

// CompareTriples orders triples by key bytes, then window start, then count.
// Count is last so that two result sets differing only in a count still sort
// alongside each other and the first difference reported is the interesting
// one.
func CompareTriples(a, b Triple) int {
	if c := strings.Compare(a.Key, b.Key); c != 0 {
		return c
	}
	if a.WindowStart != b.WindowStart {
		if a.WindowStart < b.WindowStart {
			return -1
		}
		return 1
	}
	switch {
	case a.Count < b.Count:
		return -1
	case a.Count > b.Count:
		return 1
	}
	return 0
}
