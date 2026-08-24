package operators

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"

	"github.com/AarinB1/tidemark/pkg/core"
	"github.com/AarinB1/tidemark/pkg/state"
)

// State layout. Both halves are documented here because Phase 3b serialises
// this state and the restore path has to read back exactly what was written.
//
//	key    state.PrefixUserState, then the record's key bytes, then the window
//	       start as a big-endian int64
//	value  the aggregate as a big-endian int64, eight bytes
//
// The leading byte is the key-space discriminator declared in pkg/state. This
// operator's aggregates are user state, so every key it writes begins with
// state.PrefixUserState. It is not decoration: a subtask has ONE key space, and
// Phase 6 moves event-time timers into it behind state.PrefixTimer. Without a
// discriminator a timer key and an aggregate key would be distinguishable only
// by length and by luck, and the scan this operator already runs on every
// watermark would read timers as aggregates. The byte is claimed now because
// the snapshot format is written in this phase and repartitioning a key space
// afterwards invalidates every checkpoint on disk.
//
// The window start goes AFTER the record key so that sorted iteration groups
// one key's windows together: a scan for a key is a scan of a contiguous run,
// which is what the Pebble backend in Phase 3b wants and what Memory's sorted
// Iterate already gives. It is fixed-width, so the split back into (key, start)
// is the last eight bytes and everything before them, with no separator and no
// ambiguity: two composite keys can only be equal if they are the same length,
// hence the same key length, hence the same key and the same start.
//
// Big-endian for the same reason the generator's keys are: byte order and
// numeric order agree over non-negative values, so a window start sorts where
// it compares. They part company below zero, since a negative int64 has its top
// bit set and sorts above every positive one. That costs nothing here, because
// the grouping this layout is for is by KEY and the run for one key stays
// contiguous whatever order its window starts fall in.
//
// One caveat, recorded because it is invisible until it bites: the grouping
// holds only while no record key is a prefix of another. Every key in this
// engine is the generator's fixed eight bytes, so it holds. A variable-length
// key would interleave two keys' runs and would need a length prefix.
//
// The windowKey struct that used to key an in-memory map is gone with it, and
// so is the per-record string conversion its Phase 2 comment flagged as this
// operator's hottest allocation: KeyedState takes bytes, so the composite key
// is appended into one reused buffer and never converted. One conversion per
// record remains and it is NOT this one. timerService deduplicates on a struct
// holding a Go string, so registering a timer still needs a string; that is
// timers.go's to fix and not this file's.
const windowStartBytes = 8

// prefixBytes is the width of the key-space discriminator that leads every
// composite key. Named rather than written as 1 at each use so that the length
// check below and the split back out of a key cannot disagree about it.
const prefixBytes = 1

// errCountTooShort is returned by DecodeCount for a value that is not an
// encoded count.
var errCountTooShort = errors.New("value is shorter than an encoded count")

// errStateKeyTooShort is returned when state holds a key that cannot carry a
// window start. Only this operator writes to its own state, so it means the
// layout above and the code below have come apart.
var errStateKeyTooShort = errors.New("state key is shorter than an encoded window start")

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

	// state holds one entry per open (key, window); see the layout above. It is
	// handed over by Open rather than made here, so the runtime decides which
	// backend the job runs on.
	state   state.KeyedState
	timers  *timerService
	scratch []int64
	// keyBuf is the composite key under construction, reused across records so
	// that assignment does not allocate. KeyedState.Put copies what it is
	// given, which is what makes reusing it safe.
	keyBuf []byte

	// watermark is the last one delivered. It decides what has been purged and
	// therefore what is late enough to drop.
	watermark int64
	dropped   int64
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
		timers:          newTimerService(),
		// No watermark has been delivered, so nothing is complete and nothing
		// has been purged. Starting at zero would claim every window before
		// 1970 was already gone.
		watermark: minWatermark,
	}
}

// minWatermark is the value before any watermark has arrived. It matches the
// runtime's initial CurrentWatermark.
const minWatermark = -1 << 63

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
// order is descending and does not matter: the map decides where a count
// accumulates and the timer heap decides the order windows fire in.
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

// ProcessElement adds rec to every window it belongs to and arms each of them.
//
// A window whose state has been purged is past saving, so the assignment is
// dropped and counted. The condition is the same expression purge uses, which
// is what keeps a dropped record from silently resurrecting a window that has
// already been reported and forgotten.
func (w *WindowCount) ProcessElement(rec *core.Record, ctx core.Context) error {
	// One conversion per record, and it is the timer service's: timerKey holds
	// a Go string. Reused for the composite state key below, which takes a
	// string only so that this is the sole conversion rather than a second one.
	key := string(rec.Key)
	w.scratch = w.windowsFor(w.scratch, rec.EventTime)
	for _, start := range w.scratch {
		if w.isPurged(start) {
			w.dropped++
			continue
		}
		w.keyBuf = appendStateKey(w.keyBuf[:0], key, start)
		count, err := w.currentCount(w.keyBuf, start)
		if err != nil {
			return err
		}
		var value [8]byte
		binary.BigEndian.PutUint64(value[:], uint64(count+1))
		w.state.Put(w.keyBuf, value[:])
		// Registered on every record, deduplicated by the timer service. A
		// window that has already fired was deregistered when it did, so this
		// re-arms it and the updated count goes out on the next watermark.
		w.timers.Register(addCeil(start, w.size-1), key, start)
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

// appendStateKey appends the composite state key for (key, windowStart) to dst
// and returns it. See the layout at the top of this file:
//
//	state.PrefixUserState || key || windowStart, big-endian int64
//
// It takes dst so the caller can reuse one buffer across records: KeyedState
// copies what Put is given, so nothing retains the slice.
func appendStateKey(dst []byte, key string, windowStart int64) []byte {
	dst = append(dst, state.PrefixUserState)
	dst = append(dst, key...)
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(windowStart))
	return append(dst, buf[:]...)
}

// windowStartOf reads the window start back out of a composite state key.
//
// The start is the last eight bytes whatever the discriminator is, so the read
// itself does not change. The length check does: a key must now carry the
// discriminator as well as the start, and a key one byte short would otherwise
// be read as a start overlapping the prefix.
func windowStartOf(stateKey []byte) (int64, error) {
	if len(stateKey) < prefixBytes+windowStartBytes {
		return 0, fmt.Errorf("%d bytes: %w", len(stateKey), errStateKeyTooShort)
	}
	return int64(binary.BigEndian.Uint64(stateKey[len(stateKey)-windowStartBytes:])), nil
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
func (w *WindowCount) ProcessWatermark(wm int64, ctx core.Context) error {
	w.watermark = wm
	if err := w.timers.Advance(wm, func(key string, start int64) error {
		return w.fire(key, start, ctx)
	}); err != nil {
		return err
	}
	return w.purge()
}

// fire emits the current aggregate for one window.
func (w *WindowCount) fire(key string, start int64, ctx core.Context) error {
	w.keyBuf = appendStateKey(w.keyBuf[:0], key, start)
	value, ok := w.state.Get(w.keyBuf)
	if !ok {
		// Unreachable: purge runs after Advance and only removes windows whose
		// timers that same Advance has already fired. Reported rather than
		// emitted as a zero, because a window count of zero is indistinguishable
		// from a real answer once it reaches the sink.
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
		Key:       []byte(key),
		Value:     encodeCount(count),
		EventTime: addCeil(start, w.size-1),
	})
	return nil
}

// isPurged reports whether the window starting at start is past its allowed
// lateness and so no longer held.
func (w *WindowCount) isPurged(start int64) bool {
	return w.watermark > addCeil(addCeil(start, w.size), w.allowedLateness)
}

// purge drops the state of every window the watermark has moved past.
//
// A scan of the open windows on every watermark. That is O(open windows) rather
// than O(purged), and it is the obvious implementation: the alternative is a
// second timer per window, which would collide with the firing timer in a
// service that deduplicates on (key, windowStart).
//
// KeyedState.Iterate permits the callback to delete the entry it is handed,
// which is what this does. Nothing here depends on the order the scan runs in;
// the order is sorted anyway, because Phase 3b's snapshots need it to be.
func (w *WindowCount) purge() error {
	var err error
	w.state.Iterate(func(stateKey, value []byte) bool {
		start, decodeErr := windowStartOf(stateKey)
		if decodeErr != nil {
			err = decodeErr
			return false
		}
		if w.isPurged(start) {
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
// open window without any error to point at. Serialising window state and
// pending timers is the state backend's job in Phase 3, and nothing calls
// Snapshot before then.
func (w *WindowCount) Snapshot(out io.Writer) error {
	return errors.New("operators: WindowCount cannot be snapshotted: window state arrives with the state backend in Phase 3")
}

// Restore refuses for the same reason Snapshot does.
func (w *WindowCount) Restore(r io.Reader) error {
	return errors.New("operators: WindowCount cannot be restored: window state arrives with the state backend in Phase 3")
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
