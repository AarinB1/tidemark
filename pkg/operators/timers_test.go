package operators

import (
	"errors"
	"math"
	"slices"
	"testing"
)

// fired is one firing, in the order it happened.
type fired struct {
	key         string
	windowStart int64
}

// registration is one call to Register.
type registration struct {
	fireTime    int64
	key         string
	windowStart int64
}

// advance runs the service to w and returns what fired, in order.
func advance(t *testing.T, s *timerService, w int64) []fired {
	t.Helper()
	var out []fired
	if err := s.Advance(w, func(key string, windowStart int64) error {
		out = append(out, fired{key: key, windowStart: windowStart})
		return nil
	}); err != nil {
		t.Fatalf("Advance(%d): %v", w, err)
	}
	return out
}

func registerAll(s *timerService, regs []registration) {
	for _, r := range regs {
		s.Register(r.fireTime, r.key, r.windowStart)
	}
}

// TestTimerServiceFiresInOrder is the determinism claim.
//
// Firing order must be a function of the registered set alone, never of the
// order the registrations arrived in. Upstream, records reach an operator in
// whatever order the shuffle produced, so registration order varies run to run;
// if the firing order followed it, every downstream comparison would be flaky
// for a reason that reads like a concurrency bug.
//
// Each row therefore registers the SAME set twice, once in a deliberately
// awkward order, and asserts both produce the same list.
func TestTimerServiceFiresInOrder(t *testing.T) {
	tests := []struct {
		name string
		regs []registration
		w    int64
		want []fired
	}{
		{
			name: "by fire time",
			regs: []registration{
				{fireTime: 300, key: "a", windowStart: 300},
				{fireTime: 100, key: "a", windowStart: 100},
				{fireTime: 200, key: "a", windowStart: 200},
			},
			w:    1000,
			want: []fired{{"a", 100}, {"a", 200}, {"a", 300}},
		},
		{
			// The documented tie-break: same fire time, so key bytes decide.
			name: "a tie goes to the lower key",
			regs: []registration{
				{fireTime: 99, key: "c", windowStart: 0},
				{fireTime: 99, key: "a", windowStart: 0},
				{fireTime: 99, key: "b", windowStart: 0},
			},
			w:    99,
			want: []fired{{"a", 0}, {"b", 0}, {"c", 0}},
		},
		{
			// Same fire time and same key, so the window start decides. Real
			// windows of one size never collide this way, since the fire time
			// is derived from the start; the order is pinned anyway so that a
			// second window specification in one operator could not introduce
			// nondeterminism unnoticed.
			name: "a tie on key goes to the lower window start",
			regs: []registration{
				{fireTime: 50, key: "k", windowStart: 30},
				{fireTime: 50, key: "k", windowStart: 10},
				{fireTime: 50, key: "k", windowStart: 20},
			},
			w:    50,
			want: []fired{{"k", 10}, {"k", 20}, {"k", 30}},
		},
		{
			// Keys are byte slices, so the order is bytewise and not any
			// text collation: uppercase sorts before lowercase.
			name: "keys order by bytes",
			regs: []registration{
				{fireTime: 1, key: "b", windowStart: 0},
				{fireTime: 1, key: "B", windowStart: 0},
				{fireTime: 1, key: "\x00", windowStart: 0},
				{fireTime: 1, key: "\xff", windowStart: 0},
			},
			w:    1,
			want: []fired{{"\x00", 0}, {"B", 0}, {"b", 0}, {"\xff", 0}},
		},
		{
			name: "fire time beats key",
			regs: []registration{
				{fireTime: 200, key: "a", windowStart: 0},
				{fireTime: 100, key: "z", windowStart: 0},
			},
			w:    200,
			want: []fired{{"z", 0}, {"a", 0}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newTimerService()
			registerAll(s, tt.regs)
			if got := advance(t, s, tt.w); !slices.Equal(got, tt.want) {
				t.Errorf("fired %v, want %v", got, tt.want)
			}

			// The same set, registered back to front. Same result, or the
			// order is a function of arrival and not of the set.
			reversed := slices.Clone(tt.regs)
			slices.Reverse(reversed)
			s = newTimerService()
			registerAll(s, reversed)
			if got := advance(t, s, tt.w); !slices.Equal(got, tt.want) {
				t.Errorf("registered back to front, fired %v, want %v", got, tt.want)
			}
		})
	}
}

// TestTimerServiceFiresInOrderUnderInterleavedRegistration registers half the
// timers, advances part way, then registers the rest.
//
// This is what a real operator does: records keep arriving between watermarks,
// so the queue is added to while it is being drained. A heap that was only ever
// filled and then emptied would not exercise the sift at all.
func TestTimerServiceFiresInOrderUnderInterleavedRegistration(t *testing.T) {
	s := newTimerService()
	registerAll(s, []registration{
		{fireTime: 500, key: "a", windowStart: 500},
		{fireTime: 100, key: "b", windowStart: 100},
		{fireTime: 900, key: "c", windowStart: 900},
	})

	if got := advance(t, s, 100); !slices.Equal(got, []fired{{"b", 100}}) {
		t.Fatalf("at watermark 100 fired %v, want [{b 100}]", got)
	}

	// New timers arrive, one of them behind the ones still waiting.
	registerAll(s, []registration{
		{fireTime: 300, key: "d", windowStart: 300},
		{fireTime: 700, key: "e", windowStart: 700},
		{fireTime: 500, key: "f", windowStart: 500},
	})

	if got := advance(t, s, 600); !slices.Equal(got, []fired{{"d", 300}, {"a", 500}, {"f", 500}}) {
		t.Fatalf("at watermark 600 fired %v, want [{d 300} {a 500} {f 500}]", got)
	}
	if got := advance(t, s, 1000); !slices.Equal(got, []fired{{"e", 700}, {"c", 900}}) {
		t.Fatalf("at watermark 1000 fired %v, want [{e 700} {c 900}]", got)
	}
	if s.Len() != 0 {
		t.Errorf("%d timers left waiting, want none", s.Len())
	}
}

// TestTimerServiceDeduplicatesRegistration is what keeps a window from emitting
// once per record.
//
// Every record assigned to a window registers that window's timer. Without the
// map, a window holding a thousand records would fire a thousand times and put
// a thousand copies of its result in the sink; the count on each would even be
// correct, so only the row count would give it away.
func TestTimerServiceDeduplicatesRegistration(t *testing.T) {
	s := newTimerService()
	for range 1000 {
		s.Register(100, "k", 0)
	}
	if s.Len() != 1 {
		t.Errorf("1000 registrations of one pair left %d timers, want 1", s.Len())
	}
	if got := advance(t, s, 100); !slices.Equal(got, []fired{{"k", 0}}) {
		t.Errorf("fired %v, want one firing", got)
	}

	// The pair differs only in the window start, and differs only in the key:
	// both are distinct timers, so deduplication is on the pair and not on
	// either half of it.
	s = newTimerService()
	s.Register(100, "k", 0)
	s.Register(100, "k", 1)
	s.Register(100, "j", 0)
	if s.Len() != 3 {
		t.Errorf("three distinct pairs left %d timers, want 3", s.Len())
	}
}

// TestTimerServiceAcceptsReregistrationAfterFiring is the late-record path in
// pkg/operators/window.go. A record arriving for a window that has already
// fired, but has not yet been purged, must be able to schedule a re-fire, which
// it cannot do if the pair is still marked registered.
//
// The re-fire lands on the NEXT watermark rather than inside the Advance that
// is running, which is what stops an operator that registers on every firing
// from looping forever.
func TestTimerServiceAcceptsReregistrationAfterFiring(t *testing.T) {
	s := newTimerService()
	s.Register(100, "k", 0)

	if got := advance(t, s, 150); !slices.Equal(got, []fired{{"k", 0}}) {
		t.Fatalf("fired %v, want one firing", got)
	}

	s.Register(100, "k", 0)
	if s.Len() != 1 {
		t.Fatalf("re-registering a fired pair left %d timers, want 1", s.Len())
	}
	if got := advance(t, s, 150); !slices.Equal(got, []fired{{"k", 0}}) {
		t.Errorf("the re-fire produced %v, want one firing", got)
	}
}

// TestTimerServiceAdvanceBoundaries covers the two ends: a watermark below
// everything fires nothing, and MaxInt64 fires everything.
//
// The MaxInt64 row is the end-of-input flush the gate emits. It is the only
// thing that fires windows still open when the input runs out, so a service
// that treated it as just another large number would leave the tail of every
// run in the operator instead of in the sink.
func TestTimerServiceAdvanceBoundaries(t *testing.T) {
	regs := []registration{
		{fireTime: math.MinInt64, key: "floor", windowStart: math.MinInt64},
		{fireTime: -100, key: "neg", windowStart: -200},
		{fireTime: 0, key: "zero", windowStart: 0},
		{fireTime: 100, key: "a", windowStart: 0},
		{fireTime: math.MaxInt64 - 1, key: "ceil", windowStart: 0},
	}

	tests := []struct {
		name string
		w    int64
		want []fired
	}{
		{
			name: "below every timer",
			w:    math.MinInt64 + 1,
			// Only the timer at the floor itself is due: <= is inclusive.
			want: []fired{{"floor", math.MinInt64}},
		},
		{
			// Inclusive at the boundary. A window firing at end-1 fires when
			// the watermark reaches exactly end-1, not one past it.
			name: "exactly on a fire time",
			w:    -100,
			want: []fired{{"floor", math.MinInt64}, {"neg", -200}},
		},
		{
			name: "negative event times",
			w:    -1,
			want: []fired{{"floor", math.MinInt64}, {"neg", -200}},
		},
		{
			name: "MaxInt64 fires everything",
			w:    math.MaxInt64,
			want: []fired{
				{"floor", math.MinInt64}, {"neg", -200}, {"zero", 0}, {"a", 0}, {"ceil", 0},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newTimerService()
			registerAll(s, regs)
			got := advance(t, s, tt.w)
			if !slices.Equal(got, tt.want) {
				t.Fatalf("at watermark %d fired %v, want %v", tt.w, got, tt.want)
			}
			if want := len(regs) - len(tt.want); s.Len() != want {
				t.Errorf("%d timers left waiting, want %d", s.Len(), want)
			}
		})
	}
}

// TestTimerServiceFiresNothingWhenEmpty checks the empty queue, which every
// watermark hits before the first record and after the last purge.
func TestTimerServiceFiresNothingWhenEmpty(t *testing.T) {
	s := newTimerService()
	if got := advance(t, s, math.MaxInt64); len(got) != 0 {
		t.Errorf("an empty service fired %v", got)
	}
	if s.Len() != 0 {
		t.Errorf("an empty service holds %d timers", s.Len())
	}
}

// TestTimerServiceStopsOnFireError checks that a failing emit stops the drain
// rather than grinding through every remaining timer. The operator's Emit
// fails because the job is being torn down, so no later firing can succeed
// either, and the timers left behind are the ones a Phase 3 snapshot would
// need to keep.
func TestTimerServiceStopsOnFireError(t *testing.T) {
	errFire := errors.New("emit failed")

	s := newTimerService()
	registerAll(s, []registration{
		{fireTime: 1, key: "a", windowStart: 1},
		{fireTime: 2, key: "b", windowStart: 2},
		{fireTime: 3, key: "c", windowStart: 3},
	})

	var got []fired
	err := s.Advance(math.MaxInt64, func(key string, windowStart int64) error {
		got = append(got, fired{key: key, windowStart: windowStart})
		if key == "b" {
			return errFire
		}
		return nil
	})
	if !errors.Is(err, errFire) {
		t.Fatalf("Advance = %v, want %v", err, errFire)
	}
	if !slices.Equal(got, []fired{{"a", 1}, {"b", 2}}) {
		t.Errorf("fired %v, want the drain to stop after b", got)
	}
	if s.Len() != 1 {
		t.Errorf("%d timers left waiting, want the one that never fired", s.Len())
	}
}
