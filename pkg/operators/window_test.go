package operators

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"slices"
	"strings"
	"testing"

	"github.com/AarinB1/tidemark/pkg/state"
)

// triple is what a fired window looks like once decoded: the unit the batch
// oracle is compared against.
type triple struct {
	key         string
	windowStart int64
	count       int64
}

// windowHarness drives one window operator and decodes what it emits.
type windowHarness struct {
	t    *testing.T
	op   *WindowCount
	ctx  *emitContext
	seen int
}

func newWindowHarness(t *testing.T, op *WindowCount) *windowHarness {
	t.Helper()
	h := &windowHarness{t: t, op: op, ctx: &emitContext{}}
	if err := op.Open(h.ctx); err != nil {
		t.Fatalf("Open: %v", err)
	}
	return h
}

func (h *windowHarness) record(key string, eventTime int64) {
	h.t.Helper()
	if err := h.op.ProcessElement(rec(key, eventTime), h.ctx); err != nil {
		h.t.Fatalf("ProcessElement(%s, %d): %v", key, eventTime, err)
	}
}

// watermark delivers wm and returns the windows that fired in response.
func (h *windowHarness) watermark(wm int64) []triple {
	h.t.Helper()
	h.ctx.watermark = wm
	if err := h.op.ProcessWatermark(wm, h.ctx); err != nil {
		h.t.Fatalf("ProcessWatermark(%d): %v", wm, err)
	}
	return h.take()
}

func (h *windowHarness) endOfStream() []triple {
	h.t.Helper()
	if err := h.op.OnEndOfStream(h.ctx); err != nil {
		h.t.Fatalf("OnEndOfStream: %v", err)
	}
	return h.take()
}

// take decodes everything emitted since the last call.
//
// The window start is DERIVED, not read: the operator stamps a fired window
// with its end-1, so the start is EventTime-(size-1) and the harness has to
// undo the same saturating arithmetic the operator applied. Reading EventTime
// as a window start would silently shift every expectation in this file by
// size-1 and every row would still look plausible.
func (h *windowHarness) take() []triple {
	h.t.Helper()
	var out []triple
	for _, r := range h.ctx.emitted[h.seen:] {
		count, err := DecodeCount(r.Value)
		if err != nil {
			h.t.Fatalf("DecodeCount: %v", err)
		}
		out = append(out, triple{key: string(r.Key), windowStart: subFloor(r.EventTime, h.op.size-1), count: count})
	}
	h.seen = len(h.ctx.emitted)
	return out
}

// TestFloorModIsFlooredNotTruncated is the one-line bug that no epoch-based
// test can reach.
//
// Go's % takes the sign of the dividend, so -1 % 100 is -1 and a start computed
// as eventTime - eventTime%size puts a negative event time in the window ABOVE
// the one that contains it. Every timestamp in a normal test is a positive
// millisecond count since 1970, where truncated and floored agree exactly.
func TestFloorModIsFlooredNotTruncated(t *testing.T) {
	tests := []struct {
		a, b, want int64
	}{
		{a: 0, b: 100, want: 0},
		{a: 1, b: 100, want: 1},
		{a: 99, b: 100, want: 99},
		{a: 100, b: 100, want: 0},
		{a: 101, b: 100, want: 1},
		// Go's % gives -1, -99, 0 and -1 for these four.
		{a: -1, b: 100, want: 99},
		{a: -99, b: 100, want: 1},
		{a: -100, b: 100, want: 0},
		{a: -101, b: 100, want: 99},
		{a: math.MinInt64 + 1, b: 100, want: 93},
	}
	for _, tt := range tests {
		got := floorMod(tt.a, tt.b)
		if got != tt.want {
			t.Errorf("floorMod(%d, %d) = %d, want %d", tt.a, tt.b, got, tt.want)
		}
		if got < 0 || got >= tt.b {
			t.Errorf("floorMod(%d, %d) = %d, which is outside [0, %d)", tt.a, tt.b, got, tt.b)
		}
	}
}

// TestSubFloorAndAddCeilClamps pins the arithmetic the window operator uses
// at the ends of the int64 range.
//
// Unsaturated, eventTime - floorMod wraps to a large positive start, and
// start+size-1 / start+size+lateness wrap to a negative fire or purge time.
// Either way the result is a plausible-looking wrong window with no error.
func TestSubFloorAndAddCeilClamps(t *testing.T) {
	subTests := []struct {
		a, b, want int64
	}{
		{a: 0, b: 0, want: 0},
		{a: 100, b: 1, want: 99},
		{a: -100, b: 1, want: -101},
		{a: math.MinInt64, b: 1, want: math.MinInt64},
		{a: math.MinInt64 + 5, b: 10, want: math.MinInt64},
		{a: math.MinInt64, b: 0, want: math.MinInt64},
		{a: math.MaxInt64, b: 1, want: math.MaxInt64 - 1},
	}
	for _, tt := range subTests {
		if got := subFloor(tt.a, tt.b); got != tt.want {
			t.Errorf("subFloor(%d, %d) = %d, want %d", tt.a, tt.b, got, tt.want)
		}
	}

	addTests := []struct {
		a, b, want int64
	}{
		{a: 0, b: 0, want: 0},
		{a: 100, b: 1, want: 101},
		{a: -100, b: 1, want: -99},
		{a: math.MaxInt64, b: 1, want: math.MaxInt64},
		{a: math.MaxInt64 - 5, b: 10, want: math.MaxInt64},
		{a: math.MaxInt64, b: 0, want: math.MaxInt64},
		{a: math.MinInt64, b: 1, want: math.MinInt64 + 1},
	}
	for _, tt := range addTests {
		if got := addCeil(tt.a, tt.b); got != tt.want {
			t.Errorf("addCeil(%d, %d) = %d, want %d", tt.a, tt.b, got, tt.want)
		}
	}
}

// TestWindowAssignment pins membership at the exact boundaries and below zero.
//
// Half-open [start, end): the element at end belongs to the next window and the
// element at end-1 to this one. Off by one either way moves a boundary record
// into the neighbouring window, which changes two counts by one each and never
// changes the total, so nothing but an exact comparison finds it.
func TestWindowAssignment(t *testing.T) {
	tests := []struct {
		name        string
		size, slide int64
		eventTime   int64
		want        []int64
	}{
		{name: "tumbling at zero", size: 100, slide: 100, eventTime: 0, want: []int64{0}},
		{name: "tumbling inside", size: 100, slide: 100, eventTime: 42, want: []int64{0}},
		{name: "tumbling at end-1", size: 100, slide: 100, eventTime: 99, want: []int64{0}},
		{name: "tumbling at end", size: 100, slide: 100, eventTime: 100, want: []int64{100}},
		{name: "tumbling just below zero", size: 100, slide: 100, eventTime: -1, want: []int64{-100}},
		{name: "tumbling on a negative boundary", size: 100, slide: 100, eventTime: -100, want: []int64{-100}},
		{name: "tumbling below a negative boundary", size: 100, slide: 100, eventTime: -101, want: []int64{-200}},
		{
			name: "sliding at zero", size: 100, slide: 25, eventTime: 0,
			want: []int64{0, -25, -50, -75},
		},
		{
			name: "sliding at end-1 of the first aligned window", size: 100, slide: 25, eventTime: 99,
			want: []int64{75, 50, 25, 0},
		},
		{
			// One past, so the window starting at 0 drops out and 100 comes in.
			name: "sliding at end", size: 100, slide: 25, eventTime: 100,
			want: []int64{100, 75, 50, 25},
		},
		{
			name: "sliding just below zero", size: 100, slide: 25, eventTime: -1,
			want: []int64{-25, -50, -75, -100},
		},
		{
			name: "sliding at slide == size is tumbling", size: 60, slide: 60, eventTime: 61,
			want: []int64{60},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := NewSlidingCount(tt.size, tt.slide, 0)
			got := w.windowsFor(nil, tt.eventTime)
			if !slices.Equal(got, tt.want) {
				t.Fatalf("windowsFor(%d) = %v, want %v", tt.eventTime, got, tt.want)
			}
			// Every returned window must actually contain the event time, and
			// be aligned to the slide. Asserting the list alone would pass for
			// a list that was consistently wrong.
			for _, s := range got {
				if tt.eventTime < s || tt.eventTime >= s+tt.size {
					t.Errorf("window [%d, %d) does not contain event time %d", s, s+tt.size, tt.eventTime)
				}
				if floorMod(s, tt.slide) != 0 {
					t.Errorf("window start %d is not aligned to slide %d", s, tt.slide)
				}
			}
		})
	}
}

// TestWindowsForClampsInsteadOfWrapping covers assignment at the bottom of
// the int64 range.
//
// For event times in [MinInt64, MinInt64+slide), floorMod is positive, so
// eventTime - floorMod wraps to a start near MaxInt64. The record is then
// counted in windows that do not contain it. Clamped, the start stays at
// MinInt64, which does contain the event. MinInt64+8 is the first tumbling
// timestamp of size 100 whose aligned start still fits, and must not be
// pulled down to the floor by an over-eager clamp.
func TestWindowsForClampsInsteadOfWrapping(t *testing.T) {
	tests := []struct {
		name        string
		size, slide int64
		eventTime   int64
		want        []int64
	}{
		{name: "tumbling at MinInt64", size: 100, slide: 100, eventTime: math.MinInt64, want: []int64{math.MinInt64}},
		{name: "tumbling at MinInt64+1", size: 100, slide: 100, eventTime: math.MinInt64 + 1, want: []int64{math.MinInt64}},
		{name: "tumbling last wrapping timestamp", size: 100, slide: 100, eventTime: math.MinInt64 + 7, want: []int64{math.MinInt64}},
		{name: "tumbling first representable aligned start", size: 100, slide: 100, eventTime: math.MinInt64 + 8, want: []int64{math.MinInt64 + 8}},
		{name: "sliding at MinInt64", size: 100, slide: 25, eventTime: math.MinInt64, want: []int64{math.MinInt64}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := NewSlidingCount(tt.size, tt.slide, 0)
			got := w.windowsFor(nil, tt.eventTime)
			if !slices.Equal(got, tt.want) {
				t.Fatalf("windowsFor(%d) = %v, want %v", tt.eventTime, got, tt.want)
			}
			for _, s := range got {
				if s > tt.eventTime {
					t.Errorf("window start %d is above event time %d; subtraction wrapped", s, tt.eventTime)
				}
			}
		})
	}

	// The wrapped start is near MaxInt64, so the MaxInt64 watermark would
	// emit that window: a correct-looking count for an interval the event
	// is not in. Clamped, the record fires in a window that contains it.
	h := newWindowHarness(t, NewTumblingCount(100, 0))
	h.record("a", math.MinInt64)
	got := h.watermark(math.MaxInt64)
	if !slices.Equal(got, []triple{{"a", math.MinInt64, 1}}) {
		t.Fatalf("MinInt64 record fired %v, want window start MinInt64 with count 1", got)
	}
}

// TestWindowFireTimeClampsInsteadOfWrapping covers start+size-1 at the top
// of the range.
//
// A tumbling window whose start is itself near MaxInt64 has a fire time
// that overflows to a large negative value, so the first ordinary watermark
// completes it. Clamped, only the end-of-stream watermark can fire it,
// which is when no further event time can still land in the window.
func TestWindowFireTimeClampsInsteadOfWrapping(t *testing.T) {
	size := int64(math.MaxInt64 - 10)
	h := newWindowHarness(t, NewTumblingCount(size, 0))
	h.record("a", size)
	if got := len(h.ctx.emitted); got != 0 {
		t.Fatalf("the record emitted %d rows on arrival", got)
	}
	if h.watermark(0); len(h.ctx.emitted) != 0 {
		t.Fatalf("watermark 0 fired a window; fire time wrapped below zero")
	}

	// Asserted on the record rather than through the harness's triple. This
	// window's end is not representable, so end-1 saturates at MaxInt64 and the
	// start cannot be recovered from it. That is the one case where the emitted
	// event time is lossy, and it is harmless: no event time above MaxInt64 is
	// left for a downstream stage to be late against.
	h.watermark(math.MaxInt64)
	if len(h.ctx.emitted) != 1 {
		t.Fatalf("the MaxInt64 flush fired %d rows, want 1", len(h.ctx.emitted))
	}
	got := h.ctx.emitted[0]
	if string(got.Key) != "a" {
		t.Errorf("fired key %q, want \"a\"", got.Key)
	}
	if got.EventTime != math.MaxInt64 {
		t.Errorf("fired event time %d, want MaxInt64: start+size-1 must saturate rather than wrap", got.EventTime)
	}
	if count, err := DecodeCount(got.Value); err != nil || count != 1 {
		t.Errorf("fired count %d (err %v), want 1", count, err)
	}
}

// TestWindowPurgeClampsInsteadOfWrapping covers start+size+lateness at the
// top of the range.
//
// Lateness of MaxInt64 makes the purge threshold wrap to a negative value,
// so isPurged is true for any ordinary watermark and a late record is
// dropped instead of held. Clamped, the threshold is MaxInt64 and the
// window stays open for the rest of time.
func TestWindowPurgeClampsInsteadOfWrapping(t *testing.T) {
	h := newWindowHarness(t, NewTumblingCount(100, math.MaxInt64))
	h.record("a", 10)
	if got := h.watermark(99); !slices.Equal(got, []triple{{"a", 0, 1}}) {
		t.Fatalf("watermark 99 fired %v, want a count of 1", got)
	}
	h.record("a", 20)
	if got := h.watermark(200); !slices.Equal(got, []triple{{"a", 0, 2}}) {
		t.Fatalf("watermark 200 fired %v, want the corrected count of 2; purge threshold wrapped", got)
	}
	if got := h.op.Dropped(); got != 0 {
		t.Errorf("Dropped = %d, want 0: lateness of MaxInt64 must not purge", got)
	}
}

// TestSlidingMembershipIsSizeOverSlide checks the count of windows a record
// belongs to, over a long sweep of event times including negative ones.
//
// size/slide, always, with no gaps and no doubling at a boundary. A sweep is
// what catches an assignment that is right in the middle of a window and wrong
// at its edge, which a handful of hand-picked event times can miss.
func TestSlidingMembershipIsSizeOverSlide(t *testing.T) {
	tests := []struct{ size, slide int64 }{
		{size: 100, slide: 100},
		{size: 100, slide: 50},
		{size: 100, slide: 25},
		{size: 100, slide: 1},
		{size: 12, slide: 4},
	}

	for _, tt := range tests {
		w := NewSlidingCount(tt.size, tt.slide, 0)
		want := int(tt.size / tt.slide)
		for eventTime := -3 * tt.size; eventTime <= 3*tt.size; eventTime++ {
			got := w.windowsFor(nil, eventTime)
			if len(got) != want {
				t.Fatalf("size %d slide %d: event time %d is in %d windows, want %d",
					tt.size, tt.slide, eventTime, len(got), want)
			}
			seen := make(map[int64]bool, len(got))
			for _, s := range got {
				if seen[s] {
					t.Fatalf("size %d slide %d: event time %d assigned to window %d twice",
						tt.size, tt.slide, eventTime, s)
				}
				seen[s] = true
				if eventTime < s || eventTime >= s+tt.size {
					t.Fatalf("size %d slide %d: window [%d, %d) does not contain %d",
						tt.size, tt.slide, s, s+tt.size, eventTime)
				}
			}
		}
	}
}

// TestWindowFiresAtEndMinusOneAndNotBefore pins the firing boundary.
//
// A watermark of w means no element with event time <= w will arrive, so
// [start, end) is complete exactly when w reaches end-1. Firing at end instead
// costs nothing visible in the output and delays every window by a millisecond;
// firing at end-2 reports a window that can still receive an element, which
// changes the count and produces no error.
func TestWindowFiresAtEndMinusOneAndNotBefore(t *testing.T) {
	h := newWindowHarness(t, NewTumblingCount(100, 0))
	h.record("a", 10)
	h.record("a", 20)
	h.record("b", 150)

	for _, wm := range []int64{-1, 0, 50, 97, 98} {
		if got := h.watermark(wm); len(got) != 0 {
			t.Fatalf("watermark %d fired %v, but window [0, 100) is not complete until 99", wm, got)
		}
	}
	if got := h.watermark(99); !slices.Equal(got, []triple{{"a", 0, 2}}) {
		t.Fatalf("watermark 99 fired %v, want the window [0, 100) for a with count 2", got)
	}
	// The second window is untouched: firing one window must not flush the
	// rest of the map.
	if got := h.watermark(198); len(got) != 0 {
		t.Fatalf("watermark 198 fired %v, but window [100, 200) is not complete until 199", got)
	}
	if got := h.watermark(199); !slices.Equal(got, []triple{{"b", 100, 1}}) {
		t.Fatalf("watermark 199 fired %v, want the window [100, 200) for b with count 1", got)
	}
}

// TestWindowFiresOncePerWindowNotOncePerRecord is what the idempotent timer
// key buys at the operator level.
//
// A thousand records in one window must produce one emission carrying a
// thousand, not a thousand emissions each carrying a correct running count. The
// counts on the latter all look right, so only the number of rows in the sink
// gives it away.
func TestWindowFiresOncePerWindowNotOncePerRecord(t *testing.T) {
	h := newWindowHarness(t, NewTumblingCount(1000, 0))
	for i := range int64(1000) {
		h.record("k", i)
	}
	if got := h.watermark(999); !slices.Equal(got, []triple{{"k", 0, 1000}}) {
		t.Errorf("1000 records in one window fired %d times: %v", len(got), got)
	}
}

// TestWindowFiresEveryKeyInDeterministicOrder checks the tie-break the timer key
// layout produces, at the operator level.
//
// Several keys complete at the same watermark, so the order is a pure tie-break
// on key bytes. It must not depend on the order the records arrived in, which
// upstream is whatever the shuffle produced.
func TestWindowFiresEveryKeyInDeterministicOrder(t *testing.T) {
	keysIn := []string{"delta", "alpha", "charlie", "bravo"}

	h := newWindowHarness(t, NewTumblingCount(100, 0))
	for _, k := range keysIn {
		h.record(k, 10)
	}
	want := []triple{{"alpha", 0, 1}, {"bravo", 0, 1}, {"charlie", 0, 1}, {"delta", 0, 1}}
	if got := h.watermark(99); !slices.Equal(got, want) {
		t.Fatalf("fired %v, want %v", got, want)
	}

	// Same records, arriving in the opposite order.
	h = newWindowHarness(t, NewTumblingCount(100, 0))
	for i := len(keysIn) - 1; i >= 0; i-- {
		h.record(keysIn[i], 10)
	}
	if got := h.watermark(99); !slices.Equal(got, want) {
		t.Errorf("with the records reversed, fired %v, want %v", got, want)
	}
}

// TestWindowRefiresForARecordBeforePurge and the drop test below are the two
// halves of allowed lateness, and they share one script because the boundary
// between them is the whole point.
//
// Between end-1, where the window fires, and end+L, where it is purged, the
// state is still there. A record landing in that gap updates the aggregate and
// the window fires again with the new value. Delivery is at-least-once, so a
// second row for the same window is not a duplicate to be suppressed: it is the
// corrected answer, and the sink sees both.
func TestWindowRefiresForARecordBeforePurge(t *testing.T) {
	h := newWindowHarness(t, NewTumblingCount(100, 0))
	h.record("a", 10)
	h.record("a", 20)

	if got := h.watermark(99); !slices.Equal(got, []triple{{"a", 0, 2}}) {
		t.Fatalf("watermark 99 fired %v, want a count of 2", got)
	}

	// Late, but with L = 0 the window is purged only once the watermark passes
	// 100, so at 99 it is still held.
	h.record("a", 30)
	if got := h.take(); len(got) != 0 {
		t.Fatalf("a late record emitted %v on arrival; the re-fire belongs to the next watermark", got)
	}
	if got := h.watermark(100); !slices.Equal(got, []triple{{"a", 0, 3}}) {
		t.Fatalf("watermark 100 fired %v, want the corrected count of 3", got)
	}
	if got := h.op.Dropped(); got != 0 {
		t.Errorf("Dropped = %d, want 0: nothing here arrived after the purge", got)
	}
}

// TestWindowDropsAndCountsARecordAfterPurge is the other side of that boundary.
//
// Once the watermark passes end+L the state is gone, and a record for that
// window cannot be accounted for without resurrecting a window that has already
// been reported. It is dropped, and the drop is counted, because a stream
// quietly losing records is the failure this whole engine is written to make
// visible.
func TestWindowDropsAndCountsARecordAfterPurge(t *testing.T) {
	tests := []struct {
		name            string
		allowedLateness int64
		// purgedAt is the first watermark past which the window is gone.
		purgedAt int64
	}{
		{name: "no lateness allowed", allowedLateness: 0, purgedAt: 101},
		{name: "50ms of lateness", allowedLateness: 50, purgedAt: 151},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newWindowHarness(t, NewTumblingCount(100, tt.allowedLateness))
			h.record("a", 10)
			if got := h.watermark(99); !slices.Equal(got, []triple{{"a", 0, 1}}) {
				t.Fatalf("watermark 99 fired %v, want a count of 1", got)
			}

			// One before the purge: still accepted, still re-fires.
			h.watermark(tt.purgedAt - 1)
			h.record("a", 20)
			if got := h.watermark(tt.purgedAt - 1); !slices.Equal(got, []triple{{"a", 0, 2}}) {
				t.Fatalf("at watermark %d the window fired %v, want the corrected count of 2",
					tt.purgedAt-1, got)
			}
			if got := h.op.Dropped(); got != 0 {
				t.Fatalf("Dropped = %d before the purge, want 0", got)
			}

			// Past the purge: dropped, counted, and nothing emitted for it
			// however far the watermark goes afterwards.
			h.watermark(tt.purgedAt)
			h.record("a", 30)
			if got := h.op.Dropped(); got != 1 {
				t.Errorf("Dropped = %d after the purge, want 1", got)
			}
			if got := h.watermark(math.MaxInt64); len(got) != 0 {
				t.Errorf("a record after the purge produced %v; it must be dropped, not resurrect the window", got)
			}
		})
	}
}

// TestWindowCountsDropsPerAssignmentUnderSliding checks what the drop counter
// counts.
//
// Under a sliding specification one record belongs to size/slide windows, and
// an event time can be too late for the earliest of them while still being in
// time for the later ones. The counter is per (record, window) assignment, so
// the partially late record below drops some and lands in the rest.
func TestWindowCountsDropsPerAssignmentUnderSliding(t *testing.T) {
	// Windows of 100 every 25, so event time 99 belongs to those starting at
	// 75, 50, 25 and 0.
	h := newWindowHarness(t, NewSlidingCount(100, 25, 0))

	// Purge everything ending at or before 125, which is the windows starting
	// at 0 and 25.
	h.watermark(126)
	h.record("a", 99)

	if got := h.op.Dropped(); got != 2 {
		t.Errorf("Dropped = %d, want 2: the windows at 0 and 25 are purged, those at 50 and 75 are not", got)
	}
	want := []triple{{"a", 50, 1}, {"a", 75, 1}}
	if got := h.watermark(math.MaxInt64); !slices.Equal(got, want) {
		t.Errorf("fired %v, want %v", got, want)
	}
}

// TestWindowFlushesOnMaxInt64AndNotOnEndOfStream pins which of the two flushes.
//
// The gate emits a MaxInt64 watermark immediately before end-of-stream and that
// is the single mechanism for firing what is still open. An operator that also
// flushed in OnEndOfStream would double every tail window in the sink, and if
// one of the two mechanisms later broke the other would hide it.
func TestWindowFlushesOnMaxInt64AndNotOnEndOfStream(t *testing.T) {
	h := newWindowHarness(t, NewTumblingCount(100, 0))
	h.record("a", 10)
	h.record("b", 150)

	if got := h.endOfStream(); len(got) != 0 {
		t.Fatalf("OnEndOfStream flushed %v; the gate's MaxInt64 watermark is what does that", got)
	}

	want := []triple{{"a", 0, 1}, {"b", 100, 1}}
	if got := h.watermark(math.MaxInt64); !slices.Equal(got, want) {
		t.Fatalf("the MaxInt64 flush fired %v, want %v", got, want)
	}
	// And still nothing afterwards: the flush emptied the timers rather than
	// leaving them for a second pass.
	if got := h.endOfStream(); len(got) != 0 {
		t.Errorf("OnEndOfStream after the flush produced %v", got)
	}
}

// TestNewSlidingCountRejectsABadSpecification checks the constructor.
//
// size % slide != 0 is rejected because the general case doubles the assignment
// logic for windows nothing here needs. The refusal is a panic: graph.Vertex
// holds a func() core.Operator that cannot return an error, and a check
// deferred to Open would fail in every subtask at once with the cause several
// frames away.
func TestNewSlidingCountRejectsABadSpecification(t *testing.T) {
	tests := []struct {
		name                         string
		size, slide, allowedLateness int64
		wantPanic                    bool
		wantMessage                  string
	}{
		{name: "tumbling", size: 100, slide: 100},
		{name: "size a multiple of slide", size: 100, slide: 25},
		{name: "slide of one", size: 100, slide: 1},
		{name: "lateness allowed", size: 100, slide: 100, allowedLateness: 50},
		{
			name: "size not a multiple of slide", size: 100, slide: 30,
			wantPanic: true, wantMessage: "not a multiple",
		},
		{
			name: "slide larger than size", size: 100, slide: 150,
			wantPanic: true, wantMessage: "not a multiple",
		},
		{name: "zero size", size: 0, slide: 1, wantPanic: true, wantMessage: "size"},
		{name: "negative size", size: -100, slide: 100, wantPanic: true, wantMessage: "size"},
		{name: "zero slide", size: 100, slide: 0, wantPanic: true, wantMessage: "slide"},
		{name: "negative slide", size: 100, slide: -25, wantPanic: true, wantMessage: "slide"},
		{
			name: "negative lateness", size: 100, slide: 100, allowedLateness: -1,
			wantPanic: true, wantMessage: "allowedLateness",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var message string
			func() {
				defer func() {
					if r := recover(); r != nil {
						message, _ = r.(string)
						if message == "" {
							t.Fatalf("panicked with %v, want a string message", r)
						}
					}
				}()
				NewSlidingCount(tt.size, tt.slide, tt.allowedLateness)
			}()

			switch {
			case tt.wantPanic && message == "":
				t.Fatalf("NewSlidingCount(%d, %d, %d) was accepted", tt.size, tt.slide, tt.allowedLateness)
			case !tt.wantPanic && message != "":
				t.Fatalf("NewSlidingCount(%d, %d, %d) panicked: %s", tt.size, tt.slide, tt.allowedLateness, message)
			case tt.wantPanic && !strings.Contains(message, tt.wantMessage):
				t.Errorf("panic message %q does not name %q", message, tt.wantMessage)
			}
		})
	}
}

// TestNewTumblingCountIsSlidingAtSlideEqualsSize checks the two constructors
// agree, so the single assignment path really is shared.
func TestNewTumblingCountIsSlidingAtSlideEqualsSize(t *testing.T) {
	tumbling := NewTumblingCount(100, 25)
	if tumbling.size != 100 || tumbling.slide != 100 || tumbling.allowedLateness != 25 {
		t.Fatalf("NewTumblingCount(100, 25) = size %d slide %d lateness %d",
			tumbling.size, tumbling.slide, tumbling.allowedLateness)
	}
	for eventTime := int64(-250); eventTime <= 250; eventTime++ {
		a := tumbling.windowsFor(nil, eventTime)
		b := NewSlidingCount(100, 100, 25).windowsFor(nil, eventTime)
		if !slices.Equal(a, b) {
			t.Fatalf("at event time %d tumbling gives %v and sliding gives %v", eventTime, a, b)
		}
	}
}

// TestCountRoundTrips checks the encoding the sink carries.
func TestCountRoundTrips(t *testing.T) {
	for _, n := range []int64{0, 1, 2, 1000, math.MaxInt64, -1, math.MinInt64} {
		got, err := DecodeCount(encodeCount(n))
		if err != nil {
			t.Fatalf("DecodeCount(encodeCount(%d)): %v", n, err)
		}
		if got != n {
			t.Errorf("DecodeCount(encodeCount(%d)) = %d", n, got)
		}
	}

	// A short value is an error rather than a zero count. Silently reading a
	// truncated value as zero would put a plausible wrong answer in a
	// comparison against the oracle.
	if _, err := DecodeCount([]byte{1, 2, 3}); !errors.Is(err, errCountTooShort) {
		t.Errorf("DecodeCount of three bytes = %v, want %v", err, errCountTooShort)
	}
	if _, err := DecodeCount(nil); !errors.Is(err, errCountTooShort) {
		t.Errorf("DecodeCount(nil) = %v, want %v", err, errCountTooShort)
	}
}

// TestWindowSnapshotRefuses pins the Phase 3 boundary.
//
// Map and Filter write zero bytes because they hold nothing. This operator
// holds windows and timers, so a zero-byte snapshot would not be a snapshot of
// nothing but a claim that there is nothing to keep, and a recovery from it
// would lose every open window with no error to point at.
func TestWindowSnapshotRefuses(t *testing.T) {
	w := NewTumblingCount(100, 0)
	if err := w.Snapshot(io.Discard); err == nil {
		t.Error("Snapshot wrote a window operator's state away silently")
	}
	if err := w.Restore(strings.NewReader("")); err == nil {
		t.Error("Restore accepted a snapshot that cannot exist yet")
	}
}

// TestWindowEmitsTheKeyUnchanged checks the shape of the emitted record, which
// the sink and the oracle both read.
//
// The key must survive intact: it is what the record partitions on downstream,
// and an empty one is refused by the writer. Event time carries the window's
// end-1; see TestWindowEmitsEndMinusOneSoTheOutputIsNotLateDownstream for why
// that and not the start.
func TestWindowEmitsTheKeyUnchanged(t *testing.T) {
	h := newWindowHarness(t, NewTumblingCount(100, 0))
	h.record("\x00\x01\xff", 10)
	h.watermark(99)

	if len(h.ctx.emitted) != 1 {
		t.Fatalf("emitted %d records, want 1", len(h.ctx.emitted))
	}
	got := h.ctx.emitted[0]
	if string(got.Key) != "\x00\x01\xff" {
		t.Errorf("emitted key %x, want the input key unchanged", got.Key)
	}
	if got.EventTime != 99 {
		t.Errorf("emitted event time %d, want 99, the end-1 of window [0, 100)", got.EventTime)
	}
	if len(got.Key) == 0 {
		t.Error("emitted an unkeyed record, which the writer refuses to partition")
	}
}

// TestWindowEmitsEndMinusOneSoTheOutputIsNotLateDownstream is the property the
// emitted event time exists for.
//
// A watermark w asserts that no element with event time <= w will arrive, and
// this operator fires [start, end) at the first w >= end-1. That same watermark
// passed windowStart long before, so an output stamped with windowStart is
// already behind the watermark that released it and a second event-time stage
// would see the whole stream as late and drop it. The assertion is therefore
// not "the event time equals end-1" on its own but that it is NOT BELOW the
// firing watermark, which is what windowStart violates and what a chained
// operator actually depends on. Nexmark q5 is two event-time stages and lands
// in Phase 6.
//
// A sliding row is included because size and slide differ there: under
// windowStart the gap to the firing watermark is size-1 regardless of the
// slide, so a fix that used slide-1 would pass a tumbling-only test.
func TestWindowEmitsEndMinusOneSoTheOutputIsNotLateDownstream(t *testing.T) {
	tests := []struct {
		name        string
		size, slide int64
		eventTime   int64
		// fireAt is the watermark that completes the earliest window the record
		// lands in, and the whole set fires by the last one below.
		watermarks []int64
	}{
		{name: "tumbling", size: 100, slide: 100, eventTime: 10, watermarks: []int64{99}},
		{name: "tumbling negative", size: 100, slide: 100, eventTime: -1, watermarks: []int64{-1}},
		{name: "sliding", size: 100, slide: 25, eventTime: 99, watermarks: []int64{99, 124, 149, 174}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newWindowHarness(t, NewSlidingCount(tt.size, tt.slide, 0))
			h.record("k", tt.eventTime)

			fired := 0
			for _, wm := range tt.watermarks {
				before := len(h.ctx.emitted)
				h.watermark(wm)
				for _, r := range h.ctx.emitted[before:] {
					fired++
					if r.EventTime < wm {
						t.Errorf("a window fired at watermark %d carrying event time %d, which that watermark has already passed: every downstream event-time operator sees this output as late",
							wm, r.EventTime)
					}
					// And it is exactly end-1, not merely at or above the
					// watermark: anything larger would claim the window can
					// hold an element it cannot.
					start := subFloor(r.EventTime, tt.size-1)
					if got, want := r.EventTime, addCeil(start, tt.size-1); got != want {
						t.Errorf("window [%d, %d) emitted event time %d, want its end-1 %d",
							start, start+tt.size, got, want)
					}
				}
			}
			if want := int(tt.size / tt.slide); fired != want {
				t.Fatalf("%d windows fired, want %d; the assertion above saw the wrong number of rows", fired, want)
			}
		})
	}
}

// nilStateContext is a Context that provides no keyed state.
type nilStateContext struct{ emitContext }

func (nilStateContext) State() state.KeyedState { return nil }

// TestWindowOpenRefusesWithoutState pins that the operator takes its state from
// the runtime rather than making its own.
//
// An operator that fell back to a private map would run correctly and
// checkpoint as empty, so a recovery would come back with every open window
// gone and nothing to point at. Refusing at Open turns that into a job that
// does not start.
func TestWindowOpenRefusesWithoutState(t *testing.T) {
	if err := NewTumblingCount(100, 0).Open(&nilStateContext{}); err == nil {
		t.Error("Open accepted a Context with no keyed state")
	}
}

// partitionKeys returns every key in one partition of the subtask key space,
// in iteration order.
//
// Every count in this file is per PARTITION and never over the whole key space,
// and that is a change this phase forces rather than a style choice: the
// operator now writes a timer beside every open window, so a count of
// everything counts both and moves whenever either does. A test that said
// "state holds three entries" would be asserting the timer count as well,
// without saying so, and would break for the right reason and the wrong
// message the next time either side changes.
func partitionKeys(st state.KeyedState, prefix byte) [][]byte {
	var out [][]byte
	st.Iterate(func(k, v []byte) bool {
		if len(k) > 0 && k[0] == prefix {
			out = append(out, slices.Clone(k))
		}
		return true
	})
	return out
}

// TestWindowStateLayout pins the two encodings Phase 3b has to serialise and
// read back.
//
// The composite key is the record key bytes then the window start as a
// big-endian int64, and the value is the aggregate as a big-endian int64. Both
// are asserted against bytes written out by hand here rather than against
// appendStateKey, which would be the operator agreeing with itself.
//
// The negative window start matters: a start below zero has its top bit set, so
// an encoding that wrote it as anything other than the raw two's-complement
// bytes would still round-trip through this operator and only disagree with a
// snapshot written by a different one.
func TestWindowStateLayout(t *testing.T) {
	h := newWindowHarness(t, NewTumblingCount(100, 0))
	h.record("ab", 150)
	h.record("ab", 160)
	h.record("ab", -50)
	h.record("z", 150)

	want := []struct {
		key   string
		start int64
		count int64
	}{
		{key: "ab", start: -100, count: 1},
		{key: "ab", start: 100, count: 2},
		{key: "z", start: 100, count: 1},
	}

	st := h.ctx.State()
	for _, w := range want {
		// Built here from the layout rather than by calling appendStateKey, so
		// that the test states what the bytes are instead of agreeing with the
		// code that writes them. The discriminator leads.
		stateKey := []byte{state.PrefixUserState}
		stateKey = append(stateKey, w.key...)
		var startBytes [8]byte
		binary.BigEndian.PutUint64(startBytes[:], uint64(w.start))
		stateKey = append(stateKey, startBytes[:]...)

		value, ok := st.Get(stateKey)
		if !ok {
			t.Fatalf("no state under key %q + window start %d", w.key, w.start)
		}
		if len(value) != 8 {
			t.Fatalf("value for (%q, %d) is %d bytes, want 8", w.key, w.start, len(value))
		}
		if got := int64(binary.BigEndian.Uint64(value)); got != w.count {
			t.Errorf("(%q, %d) holds %d, want %d", w.key, w.start, got, w.count)
		}
	}

	// Exactly those three aggregates, so a stray write under some other key
	// would be caught rather than ignored. Counted within the user-state
	// partition: the timer beside each of them is the next test's subject.
	if got := len(partitionKeys(st, state.PrefixUserState)); got != len(want) {
		t.Errorf("the user-state partition holds %d entries, want %d", got, len(want))
	}
	// One timer per open window, and not one per record: four records went in.
	if got := len(partitionKeys(st, state.PrefixTimer)); got != len(want) {
		t.Errorf("the timer partition holds %d entries, want %d, one per open window", got, len(want))
	}
}

// TestWindowStateGroupsAKeysWindowsTogether is what putting the window start
// AFTER the record key buys.
//
// Sorted iteration must visit all of one key's windows before any of the next
// key's, so a scan for a key is a scan of a contiguous run. Phase 3b's Pebble
// backend depends on it, and so does anything that wants a key's state without
// reading the whole of it.
func TestWindowStateGroupsAKeysWindowsTogether(t *testing.T) {
	h := newWindowHarness(t, NewTumblingCount(100, 0))
	// Interleaved on purpose: the grouping must come from the layout, not from
	// the order the records arrived in.
	for _, eventTime := range []int64{0, 100, 200, 300} {
		for _, key := range []string{"kc", "ka", "kb"} {
			h.record(key, eventTime)
		}
	}

	var order []string
	for _, k := range partitionKeys(h.ctx.State(), state.PrefixUserState) {
		order = append(order, string(k[prefixBytes:len(k)-windowStartBytes]))
	}
	if len(order) != 12 {
		t.Fatalf("the user-state partition holds %d entries, want 12", len(order))
	}

	// One contiguous run per key, in key order, four windows each.
	want := []string{"ka", "ka", "ka", "ka", "kb", "kb", "kb", "kb", "kc", "kc", "kc", "kc"}
	if !slices.Equal(order, want) {
		t.Errorf("sorted iteration visited keys in order %q, want %q: a key's windows are not contiguous", order, want)
	}
}

// TestWindowPurgeRemovesStateAndNotJustTimers checks that the scan the
// watermark drives actually frees the entry.
//
// Firing a window deregisters its timer either way, so a purge that never
// deleted anything would produce identical output and leave state growing for
// the length of the run. That is the Phase 6 state-size target failing silently.
func TestWindowPurgeRemovesStateAndNotJustTimers(t *testing.T) {
	h := newWindowHarness(t, NewTumblingCount(100, 0))
	h.record("a", 10)
	h.record("a", 150)

	entries := func() int {
		return len(partitionKeys(h.ctx.State(), state.PrefixUserState))
	}
	if got := entries(); got != 2 {
		t.Fatalf("the user-state partition holds %d entries after two records in two windows, want 2", got)
	}

	// 99 fires [0, 100) but does not purge it: with lateness 0 the window is
	// held until the watermark passes end.
	h.watermark(99)
	if got := entries(); got != 2 {
		t.Errorf("state holds %d entries at the firing watermark, want 2: the window is still open for late records", got)
	}

	h.watermark(101)
	if got := entries(); got != 1 {
		t.Fatalf("state holds %d entries past the purge threshold, want 1", got)
	}
	// And it is the right one that survived.
	if _, ok := h.ctx.State().Get(appendStateKey(nil, []byte("a"), 100)); !ok {
		t.Error("the purge removed the window that is still open")
	}
	if _, ok := h.ctx.State().Get(appendStateKey(nil, []byte("a"), 0)); ok {
		t.Error("the purged window still holds state")
	}
}

// TestWindowStartOfRoundTripsThroughTheCompositeKey covers the split back, at
// the ends of the range and for keys of different lengths.
func TestWindowStartOfRoundTripsThroughTheCompositeKey(t *testing.T) {
	keys := []string{"", "a", "ab", "\x00\xff\x00", "aaaaaaaaaaaaaaaa"}
	starts := []int64{0, 1, -1, 100, -100, math.MaxInt64, math.MinInt64}
	for _, key := range keys {
		for _, start := range starts {
			composite := appendStateKey(nil, []byte(key), start)
			if got, want := len(composite), 1+len(key)+8; got != want {
				t.Fatalf("composite key for (%q, %d) is %d bytes, want %d", key, start, got, want)
			}
			if composite[0] != state.PrefixUserState {
				t.Fatalf("composite key for (%q, %d) leads with %#x, want the user-state prefix %#x", key, start, composite[0], state.PrefixUserState)
			}
			got, err := windowStartOf(composite)
			if err != nil {
				t.Fatalf("windowStartOf(%q, %d): %v", key, start, err)
			}
			if got != start {
				t.Errorf("windowStartOf for (%q, %d) = %d", key, start, got)
			}
			if k := string(composite[1 : len(composite)-8]); k != key {
				t.Errorf("the key half of the composite for (%q, %d) is %q", key, start, k)
			}
		}
	}

	// Anything too short to carry both the discriminator and the window start
	// is reported rather than read past the front of the slice. Eight bytes is
	// in the list because it was long enough before the prefix existed: a
	// length check left at the old width would read the discriminator as part
	// of the start and return a number that looks like a window.
	for _, short := range [][]byte{nil, {1}, make([]byte, 7), make([]byte, 8)} {
		if _, err := windowStartOf(short); !errors.Is(err, errStateKeyTooShort) {
			t.Errorf("windowStartOf(%d bytes) = %v, want %v", len(short), err, errStateKeyTooShort)
		}
	}
}

// TestWindowStateKeysAreConfinedToTheTwoPartitionsItOwns pins the property the
// discriminator exists for, now that both partitions are written.
//
// Every key this operator writes carries one of the two discriminators, and
// each partition is one CONTIGUOUS run under sorted iteration with every
// aggregate sorting before every timer. That second half is what makes the byte
// worth its cost: the firing scan is confined to the timer partition and the
// purge scan to the user-state partition, and neither confinement is possible
// unless a partition's keys sort together.
//
// A trailing discriminator would pass the first half of this test and fail the
// second, which is exactly the mistake it is written against. So would a
// timer key that led with the record key instead of the prefix.
func TestWindowStateKeysAreConfinedToTheTwoPartitionsItOwns(t *testing.T) {
	h := newWindowHarness(t, NewTumblingCount(100, 0))
	for _, eventTime := range []int64{0, 100, 200, -50} {
		for _, key := range []string{"kb", "ka"} {
			h.record(key, eventTime)
		}
	}

	var prefixes []byte
	counts := map[byte]int{}
	h.ctx.State().Iterate(func(k, v []byte) bool {
		if len(k) == 0 {
			t.Fatal("state holds a zero-length key, which carries no discriminator at all")
		}
		prefixes = append(prefixes, k[0])
		counts[k[0]]++
		return true
	})

	// Two keys over four windows: eight aggregates, and one timer for each.
	if got := counts[state.PrefixUserState]; got != 8 {
		t.Errorf("the operator wrote %d entries into the user-state partition, want 8", got)
	}
	if got := counts[state.PrefixTimer]; got != 8 {
		t.Errorf("the operator wrote %d entries into the timer partition, want 8", got)
	}
	if len(prefixes) != 16 {
		t.Fatalf("state holds %d entries, want 16", len(prefixes))
	}

	// One run of 0x00 then one run of 0x01, with no interleaving. Written as a
	// scan for a descent rather than as a sort comparison, because a sorted
	// check would pass on a state that alternated between two prefixes that
	// happened to be in order within each pair.
	for i := 1; i < len(prefixes); i++ {
		if prefixes[i] < prefixes[i-1] {
			t.Fatalf("sorted iteration visited prefix %#x after %#x at entry %d: the partitions interleave, so a scan cannot be confined to one",
				prefixes[i], prefixes[i-1], i)
		}
	}
	if prefixes[0] != state.PrefixUserState {
		t.Errorf("the first entry visited carries prefix %#x, want the user-state prefix %#x", prefixes[0], state.PrefixUserState)
	}
	if prefixes[len(prefixes)-1] != state.PrefixTimer {
		t.Errorf("the last entry visited carries prefix %#x, want the timer prefix %#x: the aggregates do not all sort before the timers",
			prefixes[len(prefixes)-1], state.PrefixTimer)
	}
}

// The tests below carry over the properties pkg/operators/timers_test.go pinned
// on the heap this phase deleted. They are stated at the operator level and at
// the key-layout level rather than against a timer type, because there is no
// longer a timer type: the ordering that used to be a three-field comparator is
// now an emergent property of the bytes in the key.

// TestWindowFiresInFireTimeOrderIndependentOfArrival is the determinism claim
// the heap's Less used to carry.
//
// Firing order must be a function of the pending set alone and never of the
// order the registrations arrived in. Upstream, records reach an operator in
// whatever order the shuffle produced, so if the firing order followed arrival
// every downstream comparison would be flaky for a reason that reads like a
// concurrency bug. Each row therefore feeds the SAME records twice, the second
// time back to front, and asserts both fire the same list in the same order.
//
// The old test could register a fire time directly. This one can only arm a
// timer by feeding a record, so the fire time is always start+size-1: rows that
// need two windows to share a fire time are not expressible here and are
// covered structurally by TestTimerKeySortsLikeTheTripleItEncodes below.
func TestWindowFiresInFireTimeOrderIndependentOfArrival(t *testing.T) {
	type input struct {
		key       string
		eventTime int64
	}
	tests := []struct {
		name  string
		size  int64
		input []input
		w     int64
		want  []triple
	}{
		{
			name:  "by fire time",
			size:  100,
			input: []input{{"a", 300}, {"a", 100}, {"a", 200}},
			w:     1000,
			want:  []triple{{"a", 100, 1}, {"a", 200, 1}, {"a", 300, 1}},
		},
		{
			// One window start, so the fire times are equal and the bytes that
			// follow decide. Those bytes are the record key.
			name:  "a tie goes to the lower key",
			size:  100,
			input: []input{{"c", 50}, {"a", 50}, {"b", 50}},
			w:     99,
			want:  []triple{{"a", 0, 1}, {"b", 0, 1}, {"c", 0, 1}},
		},
		{
			// Keys are byte slices, so the order is bytewise and not any text
			// collation: NUL first, uppercase before lowercase, 0xff last.
			name:  "keys order by bytes",
			size:  100,
			input: []input{{"b", 10}, {"B", 10}, {"\x00", 10}, {"\xff", 10}},
			w:     99,
			want: []triple{
				{"\x00", 0, 1}, {"B", 0, 1}, {"b", 0, 1}, {"\xff", 0, 1},
			},
		},
		{
			name:  "fire time beats key",
			size:  100,
			input: []input{{"a", 250}, {"z", 50}},
			w:     1000,
			want:  []triple{{"z", 0, 1}, {"a", 200, 1}},
		},
		{
			// The row plain big-endian gets wrong. A negative fire time has its
			// top bit set, so big-endian sorts it ABOVE every positive one and
			// the scan -- which stops at the first fire time past the watermark
			// -- would reach the positive window first, stop, and fire nothing.
			name:  "negative fire times sort below positive ones",
			size:  100,
			input: []input{{"b", 50}, {"a", -50}, {"c", 150}},
			w:     1000,
			want:  []triple{{"a", -100, 1}, {"b", 0, 1}, {"c", 100, 1}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			feed := func(in []input) []triple {
				h := newWindowHarness(t, NewTumblingCount(tt.size, 0))
				for _, r := range in {
					h.record(r.key, r.eventTime)
				}
				return h.watermark(tt.w)
			}
			if got := feed(tt.input); !slices.Equal(got, tt.want) {
				t.Errorf("fired %v, want %v", got, tt.want)
			}
			reversed := slices.Clone(tt.input)
			slices.Reverse(reversed)
			if got := feed(reversed); !slices.Equal(got, tt.want) {
				t.Errorf("with the records reversed, fired %v, want %v", got, tt.want)
			}
		})
	}
}

// TestWindowFiringBoundaries covers the two ends of the watermark range and the
// inclusive comparison between them, which the heap's `<= w` used to carry.
//
// The MaxInt64 row is the end-of-input flush the gate emits. It is the only
// thing that fires windows still open when the input runs out, so a scan that
// treated it as just another large number would leave the tail of every run in
// the operator instead of in the sink.
func TestWindowFiringBoundaries(t *testing.T) {
	// One window per key, spread across the range: at the floor, negative,
	// at zero, positive, and at the ceiling.
	inputs := []struct {
		key       string
		eventTime int64
		start     int64
	}{
		{key: "floor", eventTime: math.MinInt64, start: math.MinInt64},
		{key: "neg", eventTime: -150, start: -200},
		{key: "zero", eventTime: 50, start: 0},
		{key: "pos", eventTime: 150, start: 100},
		{key: "ceil", eventTime: math.MaxInt64 - 1, start: math.MaxInt64 - 99},
	}

	tests := []struct {
		name string
		w    int64
		want []triple
	}{
		{
			// The floor window's fire time is MinInt64+99, so a watermark of
			// MinInt64 is below every timer.
			name: "below every timer",
			w:    math.MinInt64,
			want: nil,
		},
		{
			// Inclusive at the boundary: a window fires when the watermark
			// reaches exactly end-1, not one past it.
			name: "exactly on a fire time",
			w:    -101,
			want: []triple{{"floor", math.MinInt64, 1}, {"neg", -200, 1}},
		},
		{
			name: "one below a fire time",
			w:    -102,
			want: []triple{{"floor", math.MinInt64, 1}},
		},
		{
			name: "negative event times",
			w:    -1,
			want: []triple{{"floor", math.MinInt64, 1}, {"neg", -200, 1}},
		},
		{
			name: "MaxInt64 fires everything",
			w:    math.MaxInt64,
			want: []triple{
				{"floor", math.MinInt64, 1}, {"neg", -200, 1}, {"zero", 0, 1},
				{"pos", 100, 1}, {"ceil", math.MaxInt64 - 99, 1},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newWindowHarness(t, NewTumblingCount(100, 0))
			for _, in := range inputs {
				h.record(in.key, in.eventTime)
			}
			if got := len(partitionKeys(h.ctx.State(), state.PrefixTimer)); got != len(inputs) {
				t.Fatalf("%d timers pending, want one per window (%d)", got, len(inputs))
			}
			got := h.watermark(tt.w)
			if !slices.Equal(got, tt.want) {
				t.Fatalf("at watermark %d fired %v, want %v", tt.w, got, tt.want)
			}
			// Whatever fired is gone from the timer partition, and whatever
			// did not is still there. A scan that fired without deleting would
			// pass the assertion above and re-fire on the next watermark.
			if want := len(inputs) - len(tt.want); len(partitionKeys(h.ctx.State(), state.PrefixTimer)) != want {
				t.Errorf("%d timers left pending, want %d",
					len(partitionKeys(h.ctx.State(), state.PrefixTimer)), want)
			}
		})
	}
}

// TestWindowArmsOneTimerPerWindowHoweverManyRecords is the structural half of
// what the dedupe map used to do.
//
// The map is gone: writing the same composite key again is idempotent, because
// the key is a function of (fireTime, recordKey, windowStart) and the fire time
// is itself a function of the window start and the size. If that were not
// one-to-one with (recordKey, windowStart), a window holding a thousand records
// would hold a thousand timers and emit a thousand times.
func TestWindowArmsOneTimerPerWindowHoweverManyRecords(t *testing.T) {
	h := newWindowHarness(t, NewTumblingCount(1000, 0))
	for i := range int64(1000) {
		h.record("k", i)
	}
	if got := len(partitionKeys(h.ctx.State(), state.PrefixTimer)); got != 1 {
		t.Errorf("1000 records in one window armed %d timers, want 1", got)
	}

	// Two windows and two keys are four distinct timers, so the key identifies
	// a (record key, window) pair and not just one half of it.
	h = newWindowHarness(t, NewTumblingCount(100, 0))
	for _, key := range []string{"j", "k"} {
		for _, eventTime := range []int64{10, 110} {
			h.record(key, eventTime)
			h.record(key, eventTime)
		}
	}
	if got := len(partitionKeys(h.ctx.State(), state.PrefixTimer)); got != 4 {
		t.Errorf("two keys over two windows armed %d timers, want 4", got)
	}
}

// TestWindowFiringDeletesTheTimerAndLeavesTheWindowOpen separates the two
// deletions this operator does, which used to happen in two different places
// and now happen in one key space.
//
// Firing removes the TIMER. Purging removes the AGGREGATE, later, once allowed
// lateness has passed. A firing that also dropped the aggregate would lose the
// late-record path; a firing that left the timer behind would re-fire the
// window on every subsequent watermark.
func TestWindowFiringDeletesTheTimerAndLeavesTheWindowOpen(t *testing.T) {
	h := newWindowHarness(t, NewTumblingCount(100, 0))
	h.record("a", 10)

	st := h.ctx.State()
	if got := len(partitionKeys(st, state.PrefixTimer)); got != 1 {
		t.Fatalf("%d timers armed, want 1", got)
	}

	if got := h.watermark(99); !slices.Equal(got, []triple{{"a", 0, 1}}) {
		t.Fatalf("fired %v, want the one window", got)
	}
	if got := len(partitionKeys(st, state.PrefixTimer)); got != 0 {
		t.Errorf("%d timers left after firing, want none: the window will fire again", got)
	}
	if got := len(partitionKeys(st, state.PrefixUserState)); got != 1 {
		t.Errorf("%d aggregates left after firing, want 1: the window is still open for late records", got)
	}

	// And it does not fire a second time on the next watermark.
	if got := h.watermark(100); len(got) != 0 {
		t.Errorf("the window fired again at watermark 100: %v", got)
	}
}

// TestWindowStopsFiringOnAnError is what the heap's stop-on-error carried.
//
// A failing firing stops the drain rather than grinding through every remaining
// timer. The old test made the emit fail; here the failure is planted in state,
// which is the only way this operator's fire can fail. Either way the timers
// behind it are left pending, which is what a checkpoint taken after the
// failure would keep.
func TestWindowStopsFiringOnAnError(t *testing.T) {
	h := newWindowHarness(t, NewTumblingCount(100, 0))
	h.record("a", 10)
	h.record("b", 10)
	h.record("c", 10)

	// b's aggregate is truncated to something DecodeCount refuses. Only this
	// operator writes here, so a short value means the layout and the code have
	// come apart, and firing it as a zero would put a plausible count in the
	// sink.
	h.ctx.State().Put(appendStateKey(nil, []byte("b"), 0), []byte{0, 0})

	err := h.op.ProcessWatermark(99, h.ctx)
	if !errors.Is(err, errCountTooShort) {
		t.Fatalf("ProcessWatermark = %v, want %v", err, errCountTooShort)
	}
	// a fired before b failed; c never did.
	if got := h.take(); !slices.Equal(got, []triple{{"a", 0, 1}}) {
		t.Errorf("emitted %v, want the drain to stop after a", got)
	}
	if got := len(partitionKeys(h.ctx.State(), state.PrefixTimer)); got != 1 {
		t.Errorf("%d timers left pending, want the one that never fired", got)
	}
}

// TestTimerKeyLayout pins the bytes, against a key built by hand here rather
// than by calling appendTimerKey, which would be the operator agreeing with
// itself.
//
//	state.PrefixTimer || fireTime, sign-flipped big-endian || key ||
//	windowStart, big-endian int64
func TestTimerKeyLayout(t *testing.T) {
	h := newWindowHarness(t, NewTumblingCount(100, 0))
	h.record("ab", 150)
	h.record("ab", -50)
	h.record("z", 150)

	want := []struct {
		key      string
		start    int64
		fireTime int64
	}{
		{key: "ab", start: -100, fireTime: -1},
		{key: "ab", start: 100, fireTime: 199},
		{key: "z", start: 100, fireTime: 199},
	}

	st := h.ctx.State()
	for _, w := range want {
		timerKey := []byte{state.PrefixTimer}
		var fire [8]byte
		binary.BigEndian.PutUint64(fire[:], uint64(w.fireTime)^(1<<63))
		timerKey = append(timerKey, fire[:]...)
		timerKey = append(timerKey, w.key...)
		var startBytes [8]byte
		binary.BigEndian.PutUint64(startBytes[:], uint64(w.start))
		timerKey = append(timerKey, startBytes[:]...)

		value, ok := st.Get(timerKey)
		if !ok {
			t.Fatalf("no timer under (fire %d, key %q, start %d)", w.fireTime, w.key, w.start)
		}
		// The key carries everything, so the value carries nothing. A value
		// here would be a second place for the same fact to live.
		if len(value) != 0 {
			t.Errorf("the timer for (%q, %d) holds a %d-byte value, want none", w.key, w.start, len(value))
		}
	}
	if got := len(partitionKeys(st, state.PrefixTimer)); got != len(want) {
		t.Errorf("the timer partition holds %d entries, want %d", got, len(want))
	}
}

// TestTimerKeyRoundTrips covers the split back, at the ends of the range and
// for record keys of different lengths.
//
// The empty key is in the list because it is the shortest a timer key can be
// and is what timerKeyMinBytes is measured against: the two fixed fields are
// read off the two ends, so the middle being nothing at all has to work rather
// than underflow.
func TestTimerKeyRoundTrips(t *testing.T) {
	keys := []string{"", "a", "ab", "\x00\xff\x00", "aaaaaaaaaaaaaaaa"}
	times := []int64{0, 1, -1, 100, -100, math.MaxInt64, math.MinInt64}
	for _, key := range keys {
		for _, fireTime := range times {
			for _, start := range times {
				composite := appendTimerKey(nil, fireTime, []byte(key), start)
				if got, want := len(composite), prefixBytes+state.OrderedInt64Bytes+len(key)+windowStartBytes; got != want {
					t.Fatalf("timer key for (%d, %q, %d) is %d bytes, want %d", fireTime, key, start, got, want)
				}
				if composite[0] != state.PrefixTimer {
					t.Fatalf("timer key for (%d, %q, %d) leads with %#x, want %#x", fireTime, key, start, composite[0], state.PrefixTimer)
				}
				gotFire, gotKey, gotStart, err := parseTimerKey(composite)
				if err != nil {
					t.Fatalf("parseTimerKey(%d, %q, %d): %v", fireTime, key, start, err)
				}
				if gotFire != fireTime || string(gotKey) != key || gotStart != start {
					t.Errorf("parseTimerKey round-tripped (%d, %q, %d) as (%d, %q, %d)",
						fireTime, key, start, gotFire, gotKey, gotStart)
				}
			}
		}
	}

	// Anything too short to carry both fixed fields is reported rather than
	// read past the ends of the slice. Sixteen bytes is in the list because it
	// is one short of the minimum: a length check off by the discriminator
	// would split it into a fire time and a window start that overlap.
	for _, short := range [][]byte{nil, {state.PrefixTimer}, make([]byte, 8), make([]byte, 16)} {
		if _, _, _, err := parseTimerKey(short); !errors.Is(err, errTimerKeyTooShort) {
			t.Errorf("parseTimerKey(%d bytes) = %v, want %v", len(short), err, errTimerKeyTooShort)
		}
	}
}

// TestTimerKeySortsLikeTheTripleItEncodes is the ordering claim, stated on the
// bytes rather than on a firing.
//
// It used to be timerQueue.Less: fire time, then key, then window start, a
// total order so that container/heap's instability could not leak the insertion
// sequence into the firing order. It is now the byte order of the key, and this
// is where that is pinned -- including the rows a single window specification
// cannot produce, since a real fire time is a function of the window start and
// two windows can never share one.
func TestTimerKeySortsLikeTheTripleItEncodes(t *testing.T) {
	type triplet struct {
		fireTime int64
		key      string
		start    int64
	}
	// In the order they must come out in.
	want := []triplet{
		{fireTime: math.MinInt64, key: "a", start: 0},
		{fireTime: -100, key: "a", start: -200},
		{fireTime: -1, key: "z", start: -100},
		{fireTime: 0, key: "a", start: 0},
		// Equal fire times: the record key decides, bytewise.
		{fireTime: 50, key: "\x00", start: 0},
		{fireTime: 50, key: "a", start: 0},
		{fireTime: 50, key: "b", start: 0},
		{fireTime: 50, key: "\xff", start: 0},
		// Equal fire time and equal key: the window start decides, BYTEWISE.
		// The start is written plain big-endian, so a negative one has its top
		// bit set and sorts above every non-negative one -- the opposite of the
		// numeric order the heap's comparator used here.
		//
		// That difference is deliberate and unobservable. The old comparator
		// needed a TOTAL order, not a numeric one, because container/heap is
		// unstable and any two timers left equal would come out in whatever
		// order the sift happened to leave them. Bytewise is total. And the
		// case is unreachable from one window specification anyway: the fire
		// time is start+size-1, so two windows sharing a fire time share a
		// start. It is pinned so that a second specification in one operator
		// could not introduce nondeterminism unnoticed.
		{fireTime: 60, key: "k", start: 0},
		{fireTime: 60, key: "k", start: 10},
		{fireTime: 60, key: "k", start: -10},
		{fireTime: math.MaxInt64, key: "a", start: 0},
	}

	encoded := make([][]byte, len(want))
	for i, w := range want {
		encoded[i] = appendTimerKey(nil, w.fireTime, []byte(w.key), w.start)
	}
	for i := 1; i < len(encoded); i++ {
		if bytes.Compare(encoded[i-1], encoded[i]) >= 0 {
			t.Errorf("timer key for %+v does not sort below %+v", want[i-1], want[i])
		}
	}

	// And the order is a function of the set rather than of insertion: shuffled
	// deterministically, a byte sort puts them back.
	shuffled := slices.Clone(encoded)
	for i := range shuffled {
		j := (i*7 + 3) % len(shuffled)
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	}
	slices.SortFunc(shuffled, bytes.Compare)
	if !slices.EqualFunc(shuffled, encoded, func(a, b []byte) bool { return bytes.Equal(a, b) }) {
		t.Error("sorting the shuffled timer keys did not reproduce the order above")
	}
}

// restoreWindow puts the state behind h through the checkpoint serialisation
// and opens op on the result, which is what the runtime does to an operator
// subtask on recovery.
//
// It goes through state.WriteTo and state.ReadFrom rather than handing the same
// KeyedState to a second operator, because that is the path a real restore
// takes and it is the path that would silently drop an entry the format did not
// carry. Handing over the live state would prove the operator reads its state
// and nothing about whether the state survives a checkpoint.
func restoreWindow(t *testing.T, h *windowHarness, op *WindowCount) *windowHarness {
	t.Helper()
	var buf bytes.Buffer
	if err := state.WriteTo(h.ctx.State(), &buf); err != nil {
		t.Fatalf("WriteTo: %v", err)
	}
	restored := state.NewMemory()
	if err := state.ReadFrom(restored, &buf); err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	ctx := &emitContext{state: restored}
	if err := op.Open(ctx); err != nil {
		t.Fatalf("Open on restored state: %v", err)
	}
	return &windowHarness{t: t, op: op, ctx: ctx}
}

// TestWindowStoresTheWatermarkUnderItsOwnPrefix pins the third partition:
// one entry per SUBTASK, keyed by name, holding the last watermark processed.
//
// The bytes are built here by hand rather than by calling setWatermark, which
// would be the operator agreeing with itself.
func TestWindowStoresTheWatermarkUnderItsOwnPrefix(t *testing.T) {
	h := newWindowHarness(t, NewTumblingCount(100, 0))
	st := h.ctx.State()

	// Nothing is stored before the first watermark, and the operator reads that
	// absence as minWatermark rather than as zero: at zero it would treat every
	// window before 1970 as already purged.
	if got := len(partitionKeys(st, state.PrefixOperatorState)); got != 0 {
		t.Errorf("%d scalars stored before any watermark, want none", got)
	}
	if got, err := h.op.currentWatermark(); err != nil || got != minWatermark {
		t.Errorf("currentWatermark with nothing stored = (%d, %v), want (%d, nil)", got, err, int64(minWatermark))
	}

	wantKey := append([]byte{state.PrefixOperatorState}, "watermark"...)
	for _, wm := range []int64{-500, -1, 0, 99, math.MaxInt64} {
		h.watermark(wm)

		value, ok := st.Get(wantKey)
		if !ok {
			t.Fatalf("nothing stored under %#x || \"watermark\" after watermark %d", state.PrefixOperatorState, wm)
		}
		var encoded [8]byte
		binary.BigEndian.PutUint64(encoded[:], uint64(wm)^(1<<63))
		if !bytes.Equal(value, encoded[:]) {
			t.Errorf("watermark %d stored as %x, want %x", wm, value, encoded)
		}
		if got, err := h.op.currentWatermark(); err != nil || got != wm {
			t.Errorf("currentWatermark after %d = (%d, %v)", wm, got, err)
		}
		// One entry, replaced rather than appended to. A watermark arrives
		// hundreds of times a run and an entry per arrival would grow the
		// checkpoint without bound.
		if got := len(partitionKeys(st, state.PrefixOperatorState)); got != 1 {
			t.Errorf("the operator-scalar partition holds %d entries after watermark %d, want 1", got, wm)
		}
	}
}

// TestWindowRejectsAWatermarkStoredWrong. Only this operator writes to its own
// state, so a short value means the layout and the code have come apart.
// Reading it as some other number would move what counts as late by an
// arbitrary amount and produce plausible counts.
func TestWindowRejectsAWatermarkStoredWrong(t *testing.T) {
	h := newWindowHarness(t, NewTumblingCount(100, 0))
	h.ctx.State().Put(append([]byte{state.PrefixOperatorState}, "watermark"...), []byte{0, 0})

	if _, err := h.op.currentWatermark(); err == nil {
		t.Error("currentWatermark accepted a two-byte watermark")
	}
	if err := h.op.ProcessElement(rec("a", 10), h.ctx); err == nil {
		t.Error("ProcessElement ran against a watermark it could not read")
	}
}

// TestRestoredWindowRecoversItsWatermark is the divergence this step closes,
// asserted against the run that does not recover it.
//
// The script purges a window, then restores. A record for that window arriving
// after the restore is LATE and must be dropped -- the window has already been
// fired and reported, and counting it again resurrects a (key, window) the sink
// already holds. An operator whose watermark came back as minWatermark thinks
// nothing has been purged, so it accepts the record, opens the window a second
// time and emits it again.
//
// The counterfactual is in the test rather than in a comment: the same records
// are fed to an operator opened on EMPTY state, and the assertion is that the
// two disagree. Without it this test would pass against an operator that
// dropped the record for some unrelated reason.
//
// # Why this has to be pinned HERE and not in the recovery suite
//
// The job-level recovery suite in pkg/runtime is blind to this. Deleting the
// watermark from the restore path leaves every one of its cases passing, and
// that is not a weakness in those cases -- it is a property of the input.
//
// The generator's out-of-orderness is BOUNDED: element n has event time
// base + n*step - lag with lag in [0, MaxLag]. The source's watermark generator
// emits maxSeen - MaxOutOfOrderness - 1, and the jobs there set
// MaxOutOfOrderness to the generator's own MaxLag. So the watermark is always
// strictly below the smallest event time any later element can carry, and NO
// RECORD IS EVER LATE. The lateness path therefore has nothing to reject during
// the gap between a restore and the first watermark from the resumed sources,
// which is the only window in which a lost watermark is observable at all. A
// watermark restored as MinInt64 is more permissive over that gap, and nothing
// arrives that a correct one would have dropped.
//
// That guarantee belongs to the GENERATOR, not to the engine, and it stops
// holding in Phase 6. Nexmark's input makes no bounded-lag promise, so a
// record below the watermark is an ordinary event there rather than an
// impossible one, and the gap this test covers becomes reachable from a whole
// job. At that point the job level has to watch it too and this test stops
// being the only thing standing between the bug and the sink.
func TestRestoredWindowRecoversItsWatermark(t *testing.T) {
	h := newWindowHarness(t, NewTumblingCount(100, 0))
	h.record("a", 10)
	// Fires [0, 100) and, at lateness 0, purges it too.
	if got := h.watermark(250); !slices.Equal(got, []triple{{"a", 0, 1}}) {
		t.Fatalf("the run before the checkpoint fired %v, want the one window", got)
	}
	if got := len(partitionKeys(h.ctx.State(), state.PrefixUserState)); got != 0 {
		t.Fatalf("%d aggregates left after the purge, want none", got)
	}

	restored := restoreWindow(t, h, NewTumblingCount(100, 0))
	if got, err := restored.op.currentWatermark(); err != nil || got != 250 {
		t.Fatalf("the restored operator's watermark is (%d, %v), want (250, nil)", got, err)
	}

	// The late record. Dropped, counted, and it leaves nothing behind.
	restored.record("a", 10)
	if got := restored.op.Dropped(); got != 1 {
		t.Errorf("the restored operator dropped %d assignments, want 1", got)
	}
	if got := len(partitionKeys(restored.ctx.State(), state.PrefixUserState)); got != 0 {
		t.Errorf("the late record opened %d windows on the restored operator, want none", got)
	}
	if got := restored.watermark(math.MaxInt64); len(got) != 0 {
		t.Errorf("the restored operator emitted %v for a window the sink already holds", got)
	}

	// The same record against an operator that did not recover a watermark.
	// This is what the Go field produced: the window comes back, and the sink
	// ends up with two rows for one (key, window).
	fresh := newWindowHarness(t, NewTumblingCount(100, 0))
	fresh.record("a", 10)
	if got := fresh.op.Dropped(); got != 0 {
		t.Fatalf("an operator on empty state dropped %d, want 0: the counterfactual does not hold", got)
	}
	if got := fresh.watermark(math.MaxInt64); !slices.Equal(got, []triple{{"a", 0, 1}}) {
		t.Fatalf("an operator on empty state emitted %v, want the duplicate window: the counterfactual does not hold", got)
	}
}
