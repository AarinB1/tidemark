package operators

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"slices"

	"github.com/AarinB1/tidemark/pkg/core"
	"github.com/AarinB1/tidemark/pkg/state"
)

// State layout. Both partitions are documented here because pkg/checkpoint
// serialises this state and the restore path has to read back exactly what was
// written.
//
// Aggregates, under state.PrefixUserState:
//
//	key    state.PrefixUserState, then the record's key bytes, then the window
//	       start as a big-endian int64
//	value  the aggregate as a big-endian int64, eight bytes
//
// Timers, under state.PrefixTimer:
//
//	key    state.PrefixTimer, then the fire time as state.EncodeOrderedInt64,
//	       then the record's key bytes, then the window start as a big-endian
//	       int64
//	value  empty
//
// The current watermark, under state.PrefixOperatorState:
//
//	key    state.PrefixOperatorState, then the name "watermark"
//	value  the watermark as state.EncodeOrderedInt64
//
// One entry per subtask, not one per key. It is written where the watermark is
// updated and read where it is used; see currentWatermark.
//
// The leading byte is the key-space discriminator declared in pkg/state. A
// subtask has ONE key space and this operator now writes both partitions of it,
// so without a discriminator a timer key and an aggregate key would be
// distinguishable only by length and by luck, and the purge scan would read
// timers as aggregates.
//
// # Why the timers and the watermark are here at all
//
// They are state. An operator whose timers live in a Go field gets its
// aggregates back on restore with no timer to fire them, so a (key, window)
// whose records all arrived before the checkpoint is silently never emitted --
// no error, no missing key, just a window that is not in the sink. Putting them
// in KeyedState makes the existing snapshot of the key space a snapshot of them
// too, and costs nothing else: nothing in pkg/checkpoint or pkg/runtime changes.
//
// The watermark is the same argument at one entry instead of many. In a Go
// field it comes back as minWatermark, and until the restored sources produce a
// watermark of their own the operator's idea of what has been purged is
// "nothing", so it accepts records it should be dropping as late. At allowed
// lateness 0 -- which is what the equivalence suite runs -- that is a real
// divergence over a narrow window rather than a theoretical one.
//
// # Why fireTime leads
//
// Sorted iteration is part of the KeyedState contract, so ordering the key by
// fire time makes sorted iteration of the timer partition FIRE-TIME ORDER.
// Firing is then a scan that stops at the first fire time greater than the
// watermark, which costs what the timers that fire cost rather than what the
// timers that are pending cost. That is the whole reason for this ordering, and
// it is the reason a heap is no longer needed.
//
// The fire time is written with state.EncodeOrderedInt64 and not big-endian,
// because a negative fire time in two's complement has its top bit set and
// plain big-endian would sort it above every positive one: the window would
// fire only at the MaxInt64 flush at end of input, with the right count and at
// the wrong time. Negative event times are supported input; floorMod exists for
// them.
//
// # Deterministic tie-breaking
//
// Timers with equal fire times are ordered by the bytes that follow the fire
// time, which is the record key and then the window start. That used to be an
// explicit three-field comparator on a heap and is now an emergent property of
// the layout, which is worth stating because it is no longer visible as code:
// nothing sorts, and the order comes out of KeyedState.Iterate.
//
// It is a TOTAL order either way, which is what firing determinism needs.
// Records reach an operator in whatever order the shuffle produced, so a
// firing order that depended on arrival would make every downstream comparison
// flaky for a reason that reads like a concurrency bug.
//
// # Parsing, which is the part someone later gets wrong
//
// A timer key is a variable-length field between two fixed ones, so the split
// is stated once and used everywhere: the FIRST nine bytes are the
// discriminator and the fire time, the LAST eight are the window start, and
// everything between them is the record key. There is no separator and none is
// needed -- the two fixed fields are read from the ends, so the middle is
// whatever is left. See appendTimerKey and parseTimerKey, which are next to
// each other for that reason.
//
// The mapping is injective: two timer keys of the same length split at the same
// places, so equal bytes mean an equal (fireTime, key, windowStart). That is
// what makes Put idempotent and is why there is no longer a dedupe map --
// registering a window's timer on every record writes the same key every time,
// and the fire time is a function of the window start and the size, so the
// triple is one-to-one with the (key, windowStart) pair the map used to hold.
//
// # Aggregate layout notes, unchanged
//
// The window start goes AFTER the record key so that sorted iteration groups
// one key's windows together: a scan for a key is a scan of a contiguous run.
// It is fixed-width, so the split back into (key, start) is the last eight
// bytes and everything before them, with no separator and no ambiguity.
//
// Big-endian there rather than the ordered encoding, and that is not an
// inconsistency: the aggregate partition is grouped BY KEY and only needs one
// key's run to be contiguous, which any total order on the start gives. The
// timer partition is ordered BY the number, which is the stronger requirement
// that state.EncodeOrderedInt64 exists for.
//
// One caveat for both partitions, recorded because it is invisible until it
// bites: the grouping holds only while no record key is a prefix of another.
// Every key in this engine is the generator's fixed eight bytes, so it holds. A
// variable-length key would interleave two keys' runs -- the split above stays
// unambiguous, but the ORDER would no longer be (key, windowStart) -- and would
// need a length prefix.
const windowStartBytes = 8

// prefixBytes is the width of the key-space discriminator that leads every
// composite key. Named rather than written as 1 at each use so that the length
// checks and the splits back out of a key cannot disagree about it.
const prefixBytes = 1

// timerKeyMinBytes is the width of a timer key carrying an EMPTY record key: a
// discriminator, a fire time and a window start. Anything shorter cannot be one.
const timerKeyMinBytes = prefixBytes + state.OrderedInt64Bytes + windowStartBytes

// errCountTooShort is returned by DecodeCount for a value that is not an
// encoded count.
var errCountTooShort = errors.New("value is shorter than an encoded count")

// errStateKeyTooShort is returned when state holds a key that cannot carry a
// window start. Only this operator writes to its own state, so it means the
// layout above and the code below have come apart.
var errStateKeyTooShort = errors.New("state key is shorter than an encoded window start")

// errTimerKeyTooShort is returned when the timer partition holds a key too
// short to carry a fire time and a window start. Same reasoning: only this
// operator writes there.
var errTimerKeyTooShort = errors.New("timer key is shorter than a fire time and a window start")

// WindowCount counts the records in each (key, window).
//
// Windows are half-open, [start, end). A tumbling window is the sliding case
// with slide == size, so there is one assignment path rather than two.
//
// The aggregate is a count. core.Record carries Key, Value and EventTime and no
// numeric field, so a sum would have to invent an encoding for Value that the
// generator does not produce; a count is a sum of ones and exercises the same
// accumulate path.
//
// A fired window is emitted as a record: the key unchanged, EventTime set to
// the window's END-1, and Value the count as eight big-endian bytes.
//
// End-1 is REQUIRED, not conventional. A watermark w asserts that no element
// with event time <= w will arrive. This window fires at w >= end-1, and that
// watermark passed windowStart on its way there, so a record stamped with
// windowStart carries an event time an already-forwarded watermark has gone
// past: every downstream event-time operator would see this operator's entire
// output as late and drop all of it. End-1 is the largest event time the window
// can contain and is exactly the watermark that completes it, so the emitted
// record is the newest thing the window could have held and is never behind the
// watermark that released it.
//
// This is not a Phase 7 concern. Nexmark q5 is a sliding-window count followed
// by a selection over those counts, which is two event-time stages in one job,
// and that lands in Phase 6.
//
// No information is lost: windowStart is EventTime+1-size and every reader
// already knows the size it asked for. The one exception is a window whose end
// is not representable, where end-1 saturates at MaxInt64 and the start cannot
// be recovered; such a window has no downstream event time left to be late
// against either.
//
// One subtask owns one operator and the runtime calls it from one goroutine. No
// locking.
type WindowCount struct {
	size            int64
	slide           int64
	allowedLateness int64

	// state holds one entry per open (key, window) and one per pending timer;
	// see the layout above. It is handed over by Open rather than made here, so
	// the runtime decides which backend the job runs on, and it is the ONLY
	// place this operator keeps anything a restore would need.
	state   state.KeyedState
	scratch []int64
	// keyBuf is the composite key under construction, reused across records and
	// across all three partitions so that assignment does not allocate.
	// KeyedState copies what Put is given, which is what makes reusing it safe.
	keyBuf []byte

	// dropped counts assignments discarded as late. It is a metric and the one
	// thing on this struct derived from records rather than from configuration:
	// it is deliberately NOT in state, because a Put per dropped assignment
	// would put a write on the drop path to keep a number nothing downstream
	// reads. A recovered run under-reports it; the sink contents are unaffected.
	dropped int64
}

var _ core.Operator = (*WindowCount)(nil)

// NewTumblingCount returns a window operator over non-overlapping windows of
// size millis, keeping fired windows open for allowedLateness millis past their
// end.
func NewTumblingCount(size, allowedLateness int64) *WindowCount {
	return NewSlidingCount(size, size, allowedLateness)
}

// NewSlidingCount returns a window operator over windows of size millis
// starting every slide millis.
//
// size must be a whole multiple of slide. The general case would double the
// assignment logic to buy windows whose membership count varies with the event
// time, which no part of this engine needs.
//
// A bad specification panics rather than returning an error. graph.Vertex holds
// a func() core.Operator, which cannot report one, and deferring the check to
// Open would let a job start and then fail in every subtask at once with the
// cause several frames away. transport.NewWriter refuses an empty output group
// the same way and for the same reason.
func NewSlidingCount(size, slide, allowedLateness int64) *WindowCount {
	switch {
	case size <= 0:
		panic(fmt.Sprintf("operators: NewSlidingCount: size is %d, must be > 0", size))
	case slide <= 0:
		panic(fmt.Sprintf("operators: NewSlidingCount: slide is %d, must be > 0", slide))
	case size%slide != 0:
		panic(fmt.Sprintf("operators: NewSlidingCount: size %d is not a multiple of slide %d", size, slide))
	case allowedLateness < 0:
		panic(fmt.Sprintf("operators: NewSlidingCount: allowedLateness is %d, must be >= 0", allowedLateness))
	}
	return &WindowCount{
		size:            size,
		slide:           slide,
		allowedLateness: allowedLateness,
	}
}

// minWatermark is the value before any watermark has arrived, and is what
// currentWatermark reports when state holds no watermark yet. It matches the
// runtime's initial CurrentWatermark.
//
// Nothing is complete and nothing has been purged at that point. Defaulting to
// zero instead would claim every window before 1970 was already gone.
const minWatermark = -1 << 63

// watermarkStateKey is the one name under state.PrefixOperatorState.
//
// A package-level slice rather than a value rebuilt per call, because it is
// read on the record path. Nothing mutates it: KeyedState.Get does not, and Put
// copies what it is given.
var watermarkStateKey = append([]byte{state.PrefixOperatorState}, "watermark"...)

// currentWatermark reads the last watermark this subtask processed.
//
// Read from state on every use rather than cached in a Go field, and that is
// the whole point of this: a field would be correct until a restore and then
// silently wrong, which is the failure this phase exists to close. The runtime
// fills a subtask's KeyedState AFTER Open returns, so there is no point at
// which the operator could load such a field anyway without a second mechanism
// to say when it had become valid.
//
// It costs one Get per record -- hoisted out of the per-window loop in
// ProcessElement, so it is per record and not per assignment. On Memory that is
// a map lookup; on Pebble it is a point read of a key rewritten on every
// watermark, so it is in the memtable. Correctness is the deliverable here and
// a cache with a validity flag is the clever version of this.
//
// # This and core.Context.CurrentWatermark are TWO different values
//
// The runtime keeps its own copy of the last watermark it delivered and hands
// it out through core.Context.CurrentWatermark. This function does not read it
// and must not be replaced by it, which is worth saying plainly because the
// Context is right there and a Get against state looks redundant beside it.
//
// The runtime's copy is NOT restored. It starts at MinInt64 in a fresh
// opContext and is only written when a watermark is delivered, so from the
// moment a subtask is restored until its resumed sources produce a watermark,
// core.Context.CurrentWatermark reports MinInt64 while this reports the value
// the checkpoint recorded. They agree everywhere else, which is exactly what
// makes the pair dangerous: two sources of truth that agree until a restore are
// the shape of the bug this phase was written to fix, and an operator that
// reached for the Context's copy would have that bug back with no test in
// pkg/runtime able to see it.
//
// The late-record check is this one. isPurged is called with the value returned
// here and with nothing else.
func (w *WindowCount) currentWatermark() (int64, error) {
	v, ok := w.state.Get(watermarkStateKey)
	if !ok {
		return minWatermark, nil
	}
	if len(v) < state.OrderedInt64Bytes {
		return 0, fmt.Errorf("operators: WindowCount: the stored watermark is %d bytes, want %d", len(v), state.OrderedInt64Bytes)
	}
	return state.DecodeOrderedInt64(v), nil
}

// setWatermark records the watermark this subtask has processed up to.
//
// state.EncodeOrderedInt64 and not big-endian, for consistency with the fire
// times rather than because anything sorts on it: there is one such entry per
// subtask and nothing scans it. Two encodings of a signed time in one key space
// would be a trap for whoever adds the second scalar.
func (w *WindowCount) setWatermark(wm int64) {
	encoded := state.EncodeOrderedInt64(wm)
	w.state.Put(watermarkStateKey, encoded[:])
}

// Open takes the subtask's keyed state.
//
// Taken here rather than made in the constructor because the constructor is a
// func() core.Operator held by a graph.Vertex and has no Context to ask. A nil
// state is refused rather than replaced with a private map: an operator quietly
// running on state the runtime does not know about would checkpoint as empty
// and restore as empty, and the only symptom would be windows missing after a
// recovery.
func (w *WindowCount) Open(ctx core.Context) error {
	w.state = ctx.State()
	if w.state == nil {
		return errors.New("operators: WindowCount: the runtime provided no keyed state")
	}
	return nil
}

// Dropped returns the number of (record, window) assignments discarded because
// their window had already been purged.
//
// Counted per assignment rather than per record: under a sliding specification
// one record belongs to size/slide windows, and an event time late enough to
// miss one of them may still be in time for a later one.
func (w *WindowCount) Dropped() int64 { return w.dropped }

// floorMod returns a mod b for b > 0, with the result always in [0, b).
//
// Go's % is truncated, not floored, so -1 % 100 is -1 rather than 99 and a
// naive start = eventTime - eventTime%size puts a negative event time in the
// window ABOVE the one containing it. No test built on epoch-based timestamps
// can catch that, because they are all positive, which is why the negative
// event time rows exist.
func floorMod(a, b int64) int64 {
	m := a % b
	if m < 0 {
		m += b
	}
	return m
}

// subFloor returns a - b, clamped to MinInt64 rather than wrapping.
//
// Every caller subtracts a non-negative amount (a floorMod result, a slide).
// Near MinInt64 that subtraction wraps to a large POSITIVE window start, so
// the record is counted in windows near MaxInt64 instead of the ones that
// contain it. Clamping keeps the start at MinInt64: not an aligned window,
// but a window that contains the event. Wrapping is a plausible-looking
// count in a window that does not. Watermark generation saturates for the
// same reason.
func subFloor(a, b int64) int64 {
	d := a - b
	if b > 0 && d > a {
		return math.MinInt64
	}
	return d
}

// addCeil returns a + b, clamped to MaxInt64 rather than wrapping.
//
// Every caller adds a non-negative amount (size-1, size, allowedLateness).
// A wrap makes a fire time or a purge threshold negative, so a window at
// the top of the range fires or is dropped on the next watermark instead of
// when its end actually arrives. Clamping holds the value at MaxInt64: the
// gate's end-of-stream watermark is the only one that can reach it, which
// is when no further event time can arrive anyway.
func addCeil(a, b int64) int64 {
	s := a + b
	if b > 0 && s < a {
		return math.MaxInt64
	}
	return s
}

// windowsFor appends to dst the starts of every window eventTime belongs to and
// returns it.
//
// The largest window start at or below eventTime is the one aligned to slide;
// the rest are that one stepped back by slide, size/slide times in total. The
// order is descending and does not matter: the composite key decides where a
// count accumulates and the timer partition's sort order decides the order
// windows fire in.
//
// Starts that would fall below MinInt64 are clamped rather than wrapped; the
// loop stops when a further step cannot go lower, so a sliding assignment
// near the floor does not append the same start twice and does not jump to
// MaxInt64.
//
// dst is reused by the caller so assignment does not allocate per record.
func (w *WindowCount) windowsFor(dst []int64, eventTime int64) []int64 {
	dst = dst[:0]
	start := subFloor(eventTime, floorMod(eventTime, w.slide))
	for n := w.size / w.slide; n > 0; n-- {
		dst = append(dst, start)
		next := subFloor(start, w.slide)
		if next == start {
			break
		}
		start = next
	}
	return dst
}

// fireTimeOf returns the watermark at which the window starting at start is
// complete, which is its end-1, saturating.
func (w *WindowCount) fireTimeOf(start int64) int64 { return addCeil(start, w.size-1) }

// ProcessElement adds rec to every window it belongs to and arms each of them.
//
// A window whose state has been purged is past saving, so the assignment is
// dropped and counted. The condition is the same expression purge uses, which
// is what keeps a dropped record from silently resurrecting a window that has
// already been reported and forgotten.
//
// No allocation per record and no string conversion: both composite keys are
// appended into one reused buffer, and rec.Key is handed straight to it.
func (w *WindowCount) ProcessElement(rec *core.Record, ctx core.Context) error {
	// Once per record, above the loop: the watermark cannot change while this
	// call runs, so reading it per assignment would be the same answer at
	// size/slide times the cost.
	watermark, err := w.currentWatermark()
	if err != nil {
		return err
	}
	w.scratch = w.windowsFor(w.scratch, rec.EventTime)
	for _, start := range w.scratch {
		if w.isPurged(watermark, start) {
			w.dropped++
			continue
		}
		w.keyBuf = appendStateKey(w.keyBuf[:0], rec.Key, start)
		count, err := w.currentCount(w.keyBuf, start)
		if err != nil {
			return err
		}
		var value [8]byte
		binary.BigEndian.PutUint64(value[:], uint64(count+1))
		w.state.Put(w.keyBuf, value[:])

		// Armed on every record. Writing the same composite key again is
		// idempotent, which is what replaced the dedupe map: the key is a
		// function of (fireTime, rec.Key, start) and the fire time is itself a
		// function of start and the size. A window that has already fired had
		// its timer deleted when it did, so this re-arms it and the updated
		// count goes out on the next watermark.
		w.keyBuf = appendTimerKey(w.keyBuf[:0], w.fireTimeOf(start), rec.Key, start)
		w.state.Put(w.keyBuf, nil)
	}
	return nil
}

// currentCount reads the aggregate held for one composite key, or zero if the
// window has no state yet.
//
// A stored value that does not decode is reported rather than treated as zero.
// Only this operator writes to its own state, so a short value means the layout
// and the code have come apart, and continuing from zero would fold a real
// count into a fresh one and produce a plausible number.
func (w *WindowCount) currentCount(stateKey []byte, start int64) (int64, error) {
	v, ok := w.state.Get(stateKey)
	if !ok {
		return 0, nil
	}
	count, err := DecodeCount(v)
	if err != nil {
		return 0, fmt.Errorf("window starting at %d: %w", start, err)
	}
	return count, nil
}

// appendStateKey appends the composite AGGREGATE key for (key, windowStart) to
// dst and returns it. See the layout at the top of this file:
//
//	state.PrefixUserState || key || windowStart, big-endian int64
//
// It takes dst so the caller can reuse one buffer across records: KeyedState
// copies what Put is given, so nothing retains the slice.
func appendStateKey(dst []byte, key []byte, windowStart int64) []byte {
	dst = append(dst, state.PrefixUserState)
	dst = append(dst, key...)
	var buf [windowStartBytes]byte
	binary.BigEndian.PutUint64(buf[:], uint64(windowStart))
	return append(dst, buf[:]...)
}

// windowStartOf reads the window start back out of a composite AGGREGATE key.
//
// The start is the last eight bytes whatever the discriminator is, so a timer
// key would also give up its window start here; every caller is inside the
// user-state partition and the timer path uses parseTimerKey, which returns the
// record key as well.
func windowStartOf(stateKey []byte) (int64, error) {
	if len(stateKey) < prefixBytes+windowStartBytes {
		return 0, fmt.Errorf("%d bytes: %w", len(stateKey), errStateKeyTooShort)
	}
	return int64(binary.BigEndian.Uint64(stateKey[len(stateKey)-windowStartBytes:])), nil
}

// appendTimerKey appends the composite TIMER key to dst and returns it. See the
// layout at the top of this file:
//
//	state.PrefixTimer || fireTime, state.EncodeOrderedInt64 || key ||
//	windowStart, big-endian int64
//
// The record key sits BETWEEN two fixed-width fields and carries no length of
// its own; parseTimerKey is directly below and reads the fixed fields off both
// ends. Change one and the other stops being its inverse, which is why they are
// adjacent.
func appendTimerKey(dst []byte, fireTime int64, key []byte, windowStart int64) []byte {
	dst = append(dst, state.PrefixTimer)
	fire := state.EncodeOrderedInt64(fireTime)
	dst = append(dst, fire[:]...)
	dst = append(dst, key...)
	var buf [windowStartBytes]byte
	binary.BigEndian.PutUint64(buf[:], uint64(windowStart))
	return append(dst, buf[:]...)
}

// parseTimerKey splits a timer key back into its three fields. It is the
// inverse of appendTimerKey directly above.
//
// The returned record key ALIASES timerKey; a caller that keeps it past the
// life of the key it came from has to copy it. See collectDueTimers, which
// copies the whole key once and then aliases into that copy.
func parseTimerKey(timerKey []byte) (fireTime int64, key []byte, windowStart int64, err error) {
	if len(timerKey) < timerKeyMinBytes {
		return 0, nil, 0, fmt.Errorf("%d bytes: %w", len(timerKey), errTimerKeyTooShort)
	}
	// The first nine bytes are the discriminator and the fire time; the last
	// eight are the window start; the record key is whatever is between them,
	// which for an empty key is nothing at all.
	fireTime = state.DecodeOrderedInt64(timerKey[prefixBytes:])
	key = timerKey[prefixBytes+state.OrderedInt64Bytes : len(timerKey)-windowStartBytes]
	windowStart = int64(binary.BigEndian.Uint64(timerKey[len(timerKey)-windowStartBytes:]))
	return fireTime, key, windowStart, nil
}

// dueTimer is one timer the scan found, held until the scan has ended.
type dueTimer struct {
	// stateKey is a COPY of the key the scan was handed. KeyedState hands out a
	// key that is valid only for the duration of the callback, and this one
	// outlives it: it is what Delete is called with after the scan.
	stateKey []byte
	// key aliases stateKey, so it lives exactly as long.
	key         []byte
	windowStart int64
}

// ProcessWatermark fires every window the watermark completes, then purges the
// ones it puts out of reach.
//
// Firing at end-1 rather than at end is what "no element with event time <= w
// will arrive" means: at w == end-1 nothing else can land in [start, end), and
// waiting for end would hold every window one millisecond longer for nothing.
//
// The operator emits here, inside ProcessWatermark, and the runtime broadcasts
// the watermark only after this returns. That ordering is load-bearing:
// broadcasting first would put the watermark ahead of the records it is meant
// to bound, so a downstream operator would see event time pass a window before
// the records closing it arrived. They are all in-band, which is invariant 5,
// but in-band only guarantees ORDER is preserved, not that the order was right
// to begin with.
//
// # Collect, then fire
//
// The due timers are collected by a scan that then ENDS, and only after it has
// ended are they fired and deleted. Firing must not happen inside the scan, and
// this is not tidiness: fire reads and the re-arm in ProcessElement writes keys
// OTHER than the one the callback was handed, and KeyedState.Iterate leaves
// that undefined precisely because the two backends disagree. Memory looks each
// key up again as it reaches it, so a key written mid-scan may or may not be
// visited; Pebble's iterator reads a view fixed when the scan began and would
// not see it at all. The divergence would appear only on a re-fire, which is
// the rarest path in this operator and the one nothing would think to check.
func (w *WindowCount) ProcessWatermark(wm int64, ctx core.Context) error {
	w.setWatermark(wm)
	due, err := w.collectDueTimers(wm)
	if err != nil {
		return err
	}
	for _, t := range due {
		// Deleted BEFORE firing, so that a fire which re-arms the same window
		// leaves a timer behind rather than having it removed underneath. That
		// is the same order the heap had: pop, then fire.
		w.state.Delete(t.stateKey)
		if err := w.fire(t.key, t.windowStart, ctx); err != nil {
			return err
		}
	}
	return w.purge(wm)
}

// collectDueTimers returns every timer whose fire time is at or before wm, in
// fire-time order.
//
// The scan walks the key space in ascending byte order, which puts the whole
// user-state partition first and then the timer partition in fire-time order.
// It skips the aggregates, and STOPS at the first timer that is not due: every
// timer after that one has a fire time at least as large, so the cost is what
// the firing timers cost and not what the pending ones cost. That is what the
// fire time leading the key buys, and it is what replaced the heap.
func (w *WindowCount) collectDueTimers(wm int64) ([]dueTimer, error) {
	var due []dueTimer
	var scanErr error
	w.state.Iterate(func(stateKey, value []byte) bool {
		if len(stateKey) == 0 {
			scanErr = errors.New("operators: WindowCount: state holds a zero-length key, which carries no discriminator")
			return false
		}
		switch {
		case stateKey[0] < state.PrefixTimer:
			// An aggregate. The timer partition sorts after it.
			return true
		case stateKey[0] > state.PrefixTimer:
			// Past the timer partition; nothing this operator writes lives here.
			return false
		}
		// The key handed to a callback is valid only for that call, and this
		// one outlives the scan -- Delete is called with it after the scan has
		// ended -- so it is copied and then parsed, and the record key aliases
		// the copy. Parsing the copy rather than the original is what keeps the
		// offsets in parseTimerKey and nowhere else.
		cloned := slices.Clone(stateKey)
		fireTime, key, windowStart, err := parseTimerKey(cloned)
		if err != nil {
			scanErr = err
			return false
		}
		if fireTime > wm {
			return false
		}
		due = append(due, dueTimer{stateKey: cloned, key: key, windowStart: windowStart})
		return true
	})
	if scanErr != nil {
		return nil, scanErr
	}
	return due, nil
}

// fire emits the current aggregate for one window.
//
// key aliases a copy made by collectDueTimers, which nothing reuses, so the
// emitted record can hold it without a second copy: the copy is per firing
// timer rather than per record.
func (w *WindowCount) fire(key []byte, start int64, ctx core.Context) error {
	w.keyBuf = appendStateKey(w.keyBuf[:0], key, start)
	value, ok := w.state.Get(w.keyBuf)
	if !ok {
		// Unreachable: purge runs after the firing loop and only removes
		// windows whose timers that same call has already fired. Reported
		// rather than emitted as a zero, because a window count of zero is
		// indistinguishable from a real answer once it reaches the sink.
		return fmt.Errorf("window [%d, %d) for key %x fired with no state", start, start+w.size, key)
	}
	count, err := DecodeCount(value)
	if err != nil {
		return fmt.Errorf("window [%d, %d) for key %x: %w", start, start+w.size, key, err)
	}
	// End-1, saturating: the largest event time this window can contain, and
	// exactly the watermark that fired it. See the type comment for why the
	// window start would make the whole output late downstream.
	ctx.Emit(&core.Record{
		Key:       key,
		Value:     encodeCount(count),
		EventTime: w.fireTimeOf(start),
	})
	return nil
}

// isPurged reports whether the window starting at start is past its allowed
// lateness at watermark and so no longer held.
//
// The watermark is a parameter rather than a field because it lives in state;
// each caller reads it once and passes it down, which also makes it obvious
// that one call sees one watermark throughout.
//
// Every caller passes the value from currentWatermark, which reads state, and
// never core.Context.CurrentWatermark, which is the runtime's own copy and is
// not restored. See the note on currentWatermark.
func (w *WindowCount) isPurged(watermark, start int64) bool {
	return watermark > addCeil(addCeil(start, w.size), w.allowedLateness)
}

// purge drops the state of every window the watermark has moved past.
//
// A scan of the open windows on every watermark. That is O(open windows) rather
// than O(purged), and it is the obvious implementation: the alternative is a
// second timer per window, which would double the timer partition to save a
// scan the firing path is already paying for.
//
// It is confined to the user-state partition and STOPS at the first key outside
// it, which the layout puts contiguously at the end. That confinement is not
// decoration: a timer key also carries its window start in its last eight
// bytes, so windowStartOf would happily read one and this scan would delete
// timers it was never asked about. Nothing would error; windows would just stop
// firing.
//
// KeyedState.Iterate permits the callback to delete the entry it is handed, and
// only that entry, which is what this does. Deleting a different one is
// undefined across backends; see the note on the interface. Nothing here
// depends on the order the scan runs in beyond the partitions being contiguous.
func (w *WindowCount) purge(watermark int64) error {
	var err error
	w.state.Iterate(func(stateKey, value []byte) bool {
		if len(stateKey) == 0 {
			err = errors.New("operators: WindowCount: state holds a zero-length key, which carries no discriminator")
			return false
		}
		if stateKey[0] != state.PrefixUserState {
			// Past the aggregates. Timers are not purged here: a window that is
			// purgeable fired earlier in this same call, and firing deleted its
			// timer.
			return false
		}
		start, decodeErr := windowStartOf(stateKey)
		if decodeErr != nil {
			err = decodeErr
			return false
		}
		if w.isPurged(watermark, start) {
			w.state.Delete(stateKey)
		}
		return true
	})
	return err
}

// OnEndOfStream does nothing, deliberately.
//
// The windows still open when the input runs out are flushed by the gate's
// MaxInt64 watermark, which arrives immediately before end-of-stream. Flushing
// here as well would be a second mechanism for one job: whichever of the two
// broke, the other would cover for it, and the tests would keep passing until
// something removed the survivor.
func (w *WindowCount) OnEndOfStream(ctx core.Context) error { return nil }

// Snapshot refuses rather than writing nothing.
//
// Unlike Map and Filter this operator holds state that a recovery would need,
// so a zero-byte snapshot is not a correct snapshot of nothing: it is a claim
// that there is nothing to keep, and a run recovered from it would lose every
// open window without any error to point at.
//
// Nothing calls it. A subtask's checkpoint payload is what state.WriteTo wrote,
// and every one of this operator's aggregates and timers is in the KeyedState
// that writes, so there is nothing left for this method to serialise. It stays
// a refusal rather than becoming a no-op so that a later phase which starts
// routing snapshots through core.Operator.Snapshot has to come back here.
func (w *WindowCount) Snapshot(out io.Writer) error {
	return errors.New("operators: WindowCount cannot be snapshotted through core.Operator: its state is the subtask's KeyedState, which pkg/checkpoint serialises directly")
}

// Restore refuses for the same reason Snapshot does.
func (w *WindowCount) Restore(r io.Reader) error {
	return errors.New("operators: WindowCount cannot be restored through core.Operator: its state is the subtask's KeyedState, which the runtime restores directly")
}

func (w *WindowCount) Close() error { return nil }

// encodeCount renders a count as eight big-endian bytes.
//
// Fixed width and byte-ordered, matching the generator's keys, so a count sorts
// the same way it compares and stays stable across changes. Not
// encoding/json or encoding/gob: reflection-based serialisation is out of
// scope anywhere on the data path.
func encodeCount(n int64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(n))
	return buf[:]
}

// DecodeCount reads a count written by this operator. It is exported because
// the sink contents of a windowed job are what the batch oracle is compared
// against, and that comparison happens outside this package.
func DecodeCount(value []byte) (int64, error) {
	if len(value) < 8 {
		return 0, fmt.Errorf("%d bytes: %w", len(value), errCountTooShort)
	}
	return int64(binary.BigEndian.Uint64(value)), nil
}
