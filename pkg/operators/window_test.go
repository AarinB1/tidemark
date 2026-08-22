package operators

import (
	"errors"
	"io"
	"math"
	"slices"
	"strings"
	"testing"
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
func (h *windowHarness) take() []triple {
	h.t.Helper()
	var out []triple
	for _, r := range h.ctx.emitted[h.seen:] {
		count, err := DecodeCount(r.Value)
		if err != nil {
			h.t.Fatalf("DecodeCount: %v", err)
		}
		out = append(out, triple{key: string(r.Key), windowStart: r.EventTime, count: count})
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

// TestWindowFiresOncePerWindowNotOncePerRecord is what the timer service's
// deduplication buys at the operator level.
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

// TestWindowFiresEveryKeyInDeterministicOrder checks the firing order the timer
// service documents, at the operator level.
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
// and an empty one is refused by the writer. Event time carries the window
// start, which is what makes the emitted stream a (key, windowStart, count)
// triple with nothing else to decode.
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
	if got.EventTime != 0 {
		t.Errorf("emitted event time %d, want the window start 0", got.EventTime)
	}
	if len(got.Key) == 0 {
		t.Error("emitted an unkeyed record, which the writer refuses to partition")
	}
}
