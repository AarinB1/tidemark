package operators

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"slices"

	"github.com/AarinB1/tidemark/pkg/core"
	"github.com/AarinB1/tidemark/pkg/sources"
	"github.com/AarinB1/tidemark/pkg/state"
)

// The Nexmark queries.
//
// q0, q1 and q2 are stateless and need no new operator TYPE. All three are
// operators.Map: a function from a record to a record, with a nil result
// standing for "no output", which is what Map already documents. Snapshot
// writes zero bytes and Restore drains and discards, because that is what Map
// does and there is nothing here for either of them to carry.
//
// # Why q2 is a Map and not a Filter
//
// operators.Filter is the shape q2 wants -- a predicate over records -- and it
// is not used, so this is a deliberate departure worth writing down. A
// predicate is func(*core.Record) bool and cannot report an error, and q2 has
// one to report: it has to read the type discriminator and the auction id out
// of the value, and a value that does not decode means the layout in
// pkg/sources and the code here have come apart.
//
// A Filter would have to answer false for such a record, which is the same
// answer it gives a bid on the wrong auction. The record would then be absent
// from the output for a reason indistinguishable from the query's own, and the
// symptom would be a selectivity that had quietly moved -- in the one query
// whose purpose is its selectivity. WindowCount refuses a short value for the
// same reason rather than reading it as a zero.
//
// Filter still fits a predicate that cannot fail, and nothing here changes it.

// Q1Factor is the fixed conversion q1 applies to a bid's price.
//
// Nexmark q1 converts dollars to euros. Prices here are integers, so the factor
// is one too; the value is arbitrary and only has to be neither 0 nor 1, either
// of which would make the query indistinguishable from a passthrough that had
// lost the multiplication.
const Q1Factor = 89

// Q2Divisor is the auction-id divisor q2's predicate uses by default.
//
// Nexmark q2 selects bids on a handful of named auctions. The modulo is the
// standard restatement of it, and three is chosen for the SELECTIVITY it
// produces rather than for anything about the number: over the event mix and
// the auction id space the equivalence suite runs, it keeps about a third of
// the stream. A divisor keeping ninety-nine per cent or one per cent would
// leave q2 exercising nothing, and selectivity is the whole reason q2 is in
// this set. See TestQ2Selectivity, which measures it and holds it to a band.
const Q2Divisor = 3

// NewQ0 returns the passthrough: every event, unchanged.
//
// The raw transport ceiling. It is a Map with the identity function rather than
// a new type, and it is worth having a name for even so: the equivalence and
// chaos suites name the query they are running, and a job built on
// NewMap(identity) would leave the reader to work out that it was q0.
func NewQ0() *Map {
	return NewMap(func(rec *core.Record) (*core.Record, error) { return rec, nil })
}

// NewQ1 returns the currency conversion: every bid's price multiplied by
// factor, everything else dropped.
//
// Dropping the persons and the auctions is q1 rather than a filter in front of
// it. Nexmark q1 is defined over the BID stream, so the events that are not
// bids are not its input -- and a Map that returns nil is how this engine says
// so without a second operator.
//
// The key and the event time are untouched. A bid keys on its auction, and q1
// changes neither the auction nor when the bid happened.
//
// The multiplication is uint64 and is allowed to wrap, exactly as the oracle's
// is. A caller who wants prices that mean something keeps factor*PriceRange
// inside the range; a caller who does not still gets a comparison that holds,
// because both sides wrap identically.
func NewQ1(factor uint64) *Map {
	return NewMap(func(rec *core.Record) (*core.Record, error) {
		bid, ok, err := bidOf(rec, "q1")
		if err != nil || !ok {
			return nil, err
		}
		bid.Price *= factor
		return &core.Record{
			Key:       rec.Key,
			Value:     sources.EncodeBid(bid),
			EventTime: rec.EventTime,
		}, nil
	})
}

// NewQ2 returns the selection: the bids whose auction id is a multiple of
// divisor, unchanged, and nothing else.
//
// The record is forwarded as it arrived rather than rebuilt. q2 selects; it
// does not project, and a rebuilt record would be a second encoding of the
// same event and a second thing that could disagree with the oracle.
//
// A divisor of zero panics rather than returning an error, for the reason
// NewSlidingCount panics: a graph.Vertex holds a func() core.Operator, which
// cannot report one, and deferring the check would let a job start and then
// divide by zero in every subtask at once with the cause several frames away.
func NewQ2(divisor uint64) *Map {
	if divisor == 0 {
		panic("operators: NewQ2: divisor is 0")
	}
	return NewMap(func(rec *core.Record) (*core.Record, error) {
		bid, ok, err := bidOf(rec, "q2")
		if err != nil || !ok {
			return nil, err
		}
		if bid.Auction%divisor != 0 {
			return nil, nil
		}
		return rec, nil
	})
}

// bidOf decodes rec as a bid, reporting whether it is one.
//
// Three outcomes and they are kept apart: a bid, not a bid, or a value that
// does not decode. The middle one is ordinary -- the stream carries persons and
// auctions and every bid query drops them -- and the last one is a layout
// disagreement between pkg/sources and this file, which is reported rather than
// folded into the middle one. Folding them together is exactly the mistake the
// note at the top of this file is about.
//
// query names the caller so an error says which query was reading when the
// layout came apart, since all of them read the same bytes.
func bidOf(rec *core.Record, query string) (sources.Bid, bool, error) {
	typ, err := sources.NexmarkTypeOf(rec.Value)
	if err != nil {
		return sources.Bid{}, false, fmt.Errorf("operators: %s: %w", query, err)
	}
	if typ != sources.EventBid {
		return sources.Bid{}, false, nil
	}
	bid, err := sources.DecodeBid(rec.Value)
	if err != nil {
		return sources.Bid{}, false, fmt.Errorf("operators: %s: %w", query, err)
	}
	return bid, true, nil
}

// ---------------------------------------------------------------------------
// q7: the highest bid in each (auction, tumbling window).
// ---------------------------------------------------------------------------

// MaxBid is q7.
//
// A tumbling window over bids, emitting the winning bid of each (key, window).
// The key is a bid's auction id, because that is what a bid's Record.Key holds
// and what the shuffle partitions on. A maximum over a whole window regardless
// of auction is not something one keyed operator can compute: at a parallelism
// above one, each subtask would see a share of the window's bids and report the
// maximum of that share, with nothing to say it had.
//
// # State layout
//
// The same three partitions WindowCount writes, in the same shapes. Only the
// aggregate VALUE differs, which is the whole of what this operator changes.
//
// Aggregates, under state.PrefixUserState:
//
//	key    state.PrefixUserState, then the record's key bytes, then the window
//	       start as a big-endian int64
//	value  the winning bid so far, 24 bytes; see WinningBid
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
// # What a q7 value holds, and why it holds the auction
//
// Three big-endian uint64s: the price, the bidder and the auction, in that
// order. It is the running maximum, replaced whenever a better bid arrives, and
// it is emitted verbatim as the fired record's value -- one encoding, written
// down once, so the sink and the comparison read what the state held.
//
// The auction is in there even though it is also the record key, and that is
// deliberate. The tie rule's last tier compares auctions, so the comparator
// needs it as an argument; carrying it in the value keeps betterBid a function
// of two values rather than of two values and an ambient key, and it makes a
// committed row self-describing without a reader having to decode the key.
//
// # Why the watermark is in state and not on the context
//
// core.Context.CurrentWatermark() is not restored across a checkpoint. An
// operator with a lateness rule that read it would come back thinking nothing
// had been purged, would accept records it should be dropping, and would emit a
// (key, window) the sink already holds. WindowCount keeps its watermark under
// state.PrefixOperatorState for exactly that reason and this does the same.
// Nothing in this file calls CurrentWatermark.
type MaxBid struct {
	size            int64
	allowedLateness int64

	// state is the subtask's keyed state, handed over by Open. It is the ONLY
	// place this operator keeps anything a restore would need.
	state state.KeyedState
	// keyBuf is the composite key under construction, reused across records and
	// across both partitions. KeyedState copies what Put is given, which is
	// what makes reusing it safe.
	keyBuf []byte

	// dropped counts bids discarded because their window had been purged, and
	// onTime counts the ones that were not. Both are metrics on the Go struct
	// rather than in state, for the reason WindowCount.dropped is: a Put per
	// record to keep a number nothing downstream reads would put a write on
	// every path. A recovered run under-reports both; the sink is unaffected.
	dropped int64
	onTime  int64
}

var _ core.Operator = (*MaxBid)(nil)

// winningBidBytes is the width of a q7 aggregate value: price, bidder,
// auction.
const winningBidBytes = 3 * 8

// WinningBid is what a q7 value holds: the best bid of a (key, window) under
// q7's rule.
type WinningBid struct {
	Price   uint64
	Bidder  uint64
	Auction uint64
}

// EncodeWinningBid renders w as the 24 bytes documented on MaxBid.
func EncodeWinningBid(w WinningBid) []byte {
	var buf [winningBidBytes]byte
	binary.BigEndian.PutUint64(buf[0:8], w.Price)
	binary.BigEndian.PutUint64(buf[8:16], w.Bidder)
	binary.BigEndian.PutUint64(buf[16:24], w.Auction)
	return buf[:]
}

// DecodeWinningBid reads a value written by q7.
//
// Exported because the sink contents of a q7 job are what the batch oracle is
// compared against, and that comparison happens outside this package. Exactly
// 24 bytes, not at least: only this operator writes these values, so a
// different length means the layout and the code have come apart, and reading
// the first 24 bytes of something else would produce three plausible numbers.
func DecodeWinningBid(value []byte) (WinningBid, error) {
	if len(value) != winningBidBytes {
		return WinningBid{}, fmt.Errorf("operators: q7: a winning bid is %d bytes, want %d",
			len(value), winningBidBytes)
	}
	return WinningBid{
		Price:   binary.BigEndian.Uint64(value[0:8]),
		Bidder:  binary.BigEndian.Uint64(value[8:16]),
		Auction: binary.BigEndian.Uint64(value[16:24]),
	}, nil
}

// betterBid reports whether a beats b: the highest price wins, ties go to the
// lowest bidder id, and remaining ties to the lowest auction id.
//
// A STRICT TOTAL order, and that is the requirement rather than the aesthetic.
// Records reach an operator in whatever order the shuffle and the scheduler
// produced. A winner decided by "greater price, otherwise keep what is there"
// would depend on arrival order, so two runs of one job would commit different
// bids for one window and the oracle comparison would fail intermittently --
// looking exactly like a windowing bug, which is the trap TopoOrder and the
// timer partition's fire-time ordering already avoid elsewhere.
//
// The auction tier is unreachable from inside this operator, where the auction
// is the grouping key and is therefore equal on both sides. It is written
// because the rule has to be total to be a rule, and it is tested directly for
// the same reason; see TestBetterBidIsATotalOrder.
func betterBid(a, b WinningBid) bool {
	if a.Price != b.Price {
		return a.Price > b.Price
	}
	if a.Bidder != b.Bidder {
		return a.Bidder < b.Bidder
	}
	return a.Auction < b.Auction
}

// NewQ7 returns q7 over non-overlapping windows of size millis, keeping fired
// windows open for allowedLateness millis past their end.
//
// Tumbling only. q7 is a tumbling query and a sliding variant would double the
// assignment path to buy something nothing asks for; q5 is where the sliding
// assignment is exercised, on the operator that already has one.
//
// A bad specification panics rather than returning an error, for the reason
// NewSlidingCount panics: graph.Vertex holds a func() core.Operator that cannot
// report one, and deferring the check would fail in every subtask at once with
// the cause several frames away.
func NewQ7(size, allowedLateness int64) *MaxBid {
	switch {
	case size <= 0:
		panic(fmt.Sprintf("operators: NewQ7: size is %d, must be > 0", size))
	case allowedLateness < 0:
		panic(fmt.Sprintf("operators: NewQ7: allowedLateness is %d, must be >= 0", allowedLateness))
	}
	return &MaxBid{size: size, allowedLateness: allowedLateness}
}

// Open takes the subtask's keyed state.
//
// A nil state is refused rather than replaced with a private map: an operator
// running on state the runtime does not know about would checkpoint as empty
// and restore as empty, and the only symptom would be windows missing after a
// recovery.
func (m *MaxBid) Open(ctx core.Context) error {
	m.state = ctx.State()
	if m.state == nil {
		return errors.New("operators: q7: the runtime provided no keyed state")
	}
	return nil
}

// Dropped returns the number of bids discarded because their window had already
// been purged.
func (m *MaxBid) Dropped() int64 { return m.dropped }

// OnTime returns the number of bids accepted into a window.
//
// The companion to Dropped, and it exists so that "nothing was dropped" cannot
// be satisfied by an operator that received nothing. Zero drops out of zero
// records is not evidence of anything.
func (m *MaxBid) OnTime() int64 { return m.onTime }

// windowStartOfTime returns the start of the tumbling window containing t.
//
// floorMod rather than %, because Go's remainder takes the sign of the dividend
// and a negative event time would land in the window ABOVE the one containing
// it. subFloor clamps rather than wrapping near MinInt64, where the subtraction
// would otherwise produce a large positive start.
func (m *MaxBid) windowStartOfTime(t int64) int64 {
	return subFloor(t, floorMod(t, m.size))
}

// fireTimeOf returns the watermark at which the window starting at start is
// complete, which is its end-1, saturating.
func (m *MaxBid) fireTimeOf(start int64) int64 { return addCeil(start, m.size-1) }

// isPurged reports whether the window starting at start is past its allowed
// lateness at watermark. The same expression the purge scan uses, so a dropped
// record cannot resurrect a window that has already been reported.
func (m *MaxBid) isPurged(watermark, start int64) bool {
	return watermark > addCeil(addCeil(start, m.size), m.allowedLateness)
}

// ProcessElement folds one bid into its window's running maximum and arms the
// window's timer.
//
// Events that are not bids are dropped, which is the query rather than a filter
// bolted onto it: q7 is defined over the bid stream, the same way q1 and q2
// are. It is done here rather than in a vertex in front because the price has
// to be decoded out of the value regardless, so a separate filter would decode
// every event twice and shuffle in between. q5's first stage is the exception,
// and it is an exception for a reason: WindowCount counts records without
// looking at them, so it cannot do this and needs the filter as its own vertex.
//
// A record whose window has been purged is past saving: it is dropped and
// counted rather than reopening a window the sink already holds.
func (m *MaxBid) ProcessElement(rec *core.Record, ctx core.Context) error {
	bid, ok, err := bidOf(rec, "q7")
	if err != nil || !ok {
		return err
	}

	watermark, err := loadWatermark(m.state)
	if err != nil {
		return fmt.Errorf("operators: q7: %w", err)
	}
	start := m.windowStartOfTime(rec.EventTime)
	if m.isPurged(watermark, start) {
		m.dropped++
		return nil
	}
	m.onTime++

	candidate := WinningBid{Price: bid.Price, Bidder: bid.Bidder, Auction: bid.Auction}
	m.keyBuf = appendStateKey(m.keyBuf[:0], rec.Key, start)
	if held, ok := m.state.Get(m.keyBuf); ok {
		current, err := DecodeWinningBid(held)
		if err != nil {
			return fmt.Errorf("operators: q7: window starting at %d: %w", start, err)
		}
		if !betterBid(candidate, current) {
			candidate = current
		}
	}
	m.state.Put(m.keyBuf, EncodeWinningBid(candidate))

	// Armed on every record. Writing the same composite key again is
	// idempotent: it is a function of (fireTime, rec.Key, start) and the fire
	// time is itself a function of start and the size. A window that has
	// already fired had its timer deleted when it did, so this re-arms it and
	// the updated maximum goes out on the next watermark.
	m.keyBuf = appendTimerKey(m.keyBuf[:0], m.fireTimeOf(start), rec.Key, start)
	m.state.Put(m.keyBuf, nil)
	return nil
}

// ProcessWatermark fires every window the watermark completes, then purges the
// ones it puts out of reach.
//
// The watermark is recorded in state FIRST, so that a checkpoint taken between
// two records carries the operator's own idea of event time rather than
// depending on the runtime to hand it back.
//
// The due timers are collected by a scan that then ENDS, and only afterwards
// are they fired and deleted. Firing must not happen inside the scan: fire
// reads keys other than the one the callback was handed, and KeyedState.Iterate
// leaves that undefined because the two backends disagree about it.
func (m *MaxBid) ProcessWatermark(wm int64, ctx core.Context) error {
	storeWatermark(m.state, wm)
	due, err := collectDue(m.state, wm)
	if err != nil {
		return fmt.Errorf("operators: q7: %w", err)
	}
	for _, t := range due {
		// Deleted BEFORE firing, so that a fire which re-arms the same window
		// leaves a timer behind rather than having it removed underneath.
		m.state.Delete(t.stateKey)
		if err := m.fire(t.key, t.windowStart, ctx); err != nil {
			return err
		}
	}
	return purgeWindows(m.state, func(stateKey []byte) (bool, error) {
		// q7's aggregate key is prefix || key || windowStart, so the start is
		// the last eight bytes. See purgeWindows for why this is not shared.
		start, err := windowStartOf(stateKey)
		if err != nil {
			return false, err
		}
		return m.isPurged(wm, start), nil
	})
}

// fire emits the winning bid of one window.
//
// The record's key is the window's key unchanged, its value is the stored
// winner verbatim, and its event time is the window's END-1.
//
// End-1 is required rather than conventional, for the reason WindowCount
// documents: a watermark w asserts that nothing with event time <= w will
// arrive, this window fires at w >= end-1, and that watermark has already
// passed the window start. A record stamped with the start would be behind a
// watermark that has already been forwarded, so every downstream event-time
// operator would see this operator's whole output as late.
func (m *MaxBid) fire(key []byte, start int64, ctx core.Context) error {
	m.keyBuf = appendStateKey(m.keyBuf[:0], key, start)
	value, ok := m.state.Get(m.keyBuf)
	if !ok {
		// Unreachable: the purge runs after the firing loop and only removes
		// windows whose timers that same call has already fired. Reported
		// rather than emitted as a zero, because a winning bid of zero is
		// indistinguishable from a real answer once it reaches the sink.
		return fmt.Errorf("operators: q7: window [%d, %d) for key %x fired with no state",
			start, start+m.size, key)
	}
	// Decoded and re-encoded rather than forwarded, so that a value the layout
	// cannot explain is reported here instead of reaching the sink.
	winner, err := DecodeWinningBid(value)
	if err != nil {
		return fmt.Errorf("operators: q7: window [%d, %d) for key %x: %w", start, start+m.size, key, err)
	}
	ctx.Emit(&core.Record{
		Key:       key,
		Value:     EncodeWinningBid(winner),
		EventTime: m.fireTimeOf(start),
	})
	return nil
}

// OnEndOfStream does nothing, deliberately.
//
// The windows still open when the input runs out are flushed by the gate's
// MaxInt64 watermark, which arrives immediately before end-of-stream. Flushing
// here as well would be a second mechanism for one job: whichever of the two
// broke, the other would cover for it.
func (m *MaxBid) OnEndOfStream(ctx core.Context) error { return nil }

// Snapshot refuses rather than writing nothing, for the reason WindowCount's
// does: this operator holds state a recovery would need, and a zero-byte
// snapshot is a claim that it does not.
func (m *MaxBid) Snapshot(w io.Writer) error {
	return errors.New("operators: q7 cannot be snapshotted through core.Operator: its state is the subtask's KeyedState, which pkg/checkpoint serialises directly")
}

// Restore refuses for the same reason Snapshot does.
func (m *MaxBid) Restore(r io.Reader) error {
	return errors.New("operators: q7 cannot be restored through core.Operator: its state is the subtask's KeyedState, which the runtime restores directly")
}

func (m *MaxBid) Close() error { return nil }

// ---------------------------------------------------------------------------
// Shared machinery for the operators in this file.
//
// These are free functions rather than methods, and they are here rather than
// in window.go, because the two new operators below and above both need them
// and window.go is not this step's to edit. They read and write the SAME
// partitions WindowCount does, in the same shapes, using its own
// appendStateKey, appendTimerKey and parseTimerKey -- so there is one timer
// layout in this package and not three, which matters because the chaos census
// parses that layout from outside pkg/operators.
// ---------------------------------------------------------------------------

// loadWatermark reads the watermark stored under state.PrefixOperatorState, or
// minWatermark if none has been stored.
//
// Read from state on every use rather than cached in a Go field. A field would
// be correct until a restore and then silently wrong, which is the failure this
// exists to close, and the runtime fills a subtask's KeyedState AFTER Open
// returns, so there is no point at which an operator could load such a field
// without a second mechanism to say when it had become valid.
func loadWatermark(st state.KeyedState) (int64, error) {
	v, ok := st.Get(watermarkStateKey)
	if !ok {
		return minWatermark, nil
	}
	if len(v) < state.OrderedInt64Bytes {
		return 0, fmt.Errorf("the stored watermark is %d bytes, want %d", len(v), state.OrderedInt64Bytes)
	}
	return state.DecodeOrderedInt64(v), nil
}

// storeWatermark records the watermark a subtask has processed up to. One entry
// per subtask, replaced rather than appended to.
func storeWatermark(st state.KeyedState, wm int64) {
	encoded := state.EncodeOrderedInt64(wm)
	st.Put(watermarkStateKey, encoded[:])
}

// collectDue returns every timer at or before wm, in fire-time order.
//
// The scan walks the key space in ascending byte order, which puts the whole
// user-state partition first and then the timer partition in fire-time order.
// It skips the aggregates and STOPS at the first timer that is not due: every
// timer after that one has a fire time at least as large, so the cost is what
// the firing timers cost rather than what the pending ones cost. That is what
// the fire time leading the key buys.
//
// The key handed to a callback is valid only for that call and this one
// outlives the scan -- Delete is called with it afterwards -- so it is cloned
// and then parsed, and the returned record key aliases the clone.
func collectDue(st state.KeyedState, wm int64) ([]dueTimer, error) {
	var due []dueTimer
	var scanErr error
	st.Iterate(func(stateKey, value []byte) bool {
		if len(stateKey) == 0 {
			scanErr = errors.New("state holds a zero-length key, which carries no discriminator")
			return false
		}
		switch {
		case stateKey[0] < state.PrefixTimer:
			return true // an aggregate; the timer partition sorts after it
		case stateKey[0] > state.PrefixTimer:
			return false // past the timer partition
		}
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

// purgeWindows deletes every aggregate purged reports true for.
//
// purged is handed the WHOLE state key rather than a window start, and that is
// not generality for its own sake. The two operators using this keep their
// window start in different places: q7's aggregate key is
// prefix || key || windowStart, so the start is the last eight bytes, and q5
// stage 2's is prefix || windowKey || auction, so the last eight bytes are the
// AUCTION. A shared helper that read the start off the end would hand stage 2
// an auction id -- a number near zero against a watermark near 1.7e12 -- and
// every aggregate it held would be purged on every watermark. It would still
// produce output, because a window whose counts arrive and fire inside one
// watermark call never notices; only a window whose counts arrive earlier than
// its fire time does, and then it fires with nothing or with a share of its
// auctions. That was a real bug and this signature is what closes it.
//
// Confined to the user-state partition and STOPS at the first key outside it,
// which the layout puts contiguously at the end. That confinement is not
// decoration either: a timer key also carries a window start in its last eight
// bytes, so an unconfined scan would delete timers it was never asked about and
// windows would simply stop firing, with nothing to report.
//
// KeyedState.Iterate permits the callback to delete the entry it is handed and
// only that entry, which is what this does.
func purgeWindows(st state.KeyedState, purged func(stateKey []byte) (bool, error)) error {
	var err error
	st.Iterate(func(stateKey, value []byte) bool {
		if len(stateKey) == 0 {
			err = errors.New("state holds a zero-length key, which carries no discriminator")
			return false
		}
		if stateKey[0] != state.PrefixUserState {
			// Past the aggregates. Timers are not purged here: a window that is
			// purgeable fired earlier in this same call, and firing deleted its
			// timer.
			return false
		}
		drop, decodeErr := purged(stateKey)
		if decodeErr != nil {
			err = decodeErr
			return false
		}
		if drop {
			st.Delete(stateKey)
		}
		return true
	})
	return err
}

// ---------------------------------------------------------------------------
// q5: hot items. Two event-time stages.
// ---------------------------------------------------------------------------

// The shape of q5, written down here because it is four vertices and no one of
// them says what the query is:
//
//	source -> NewBidsOnly()               a Map: keeps the bids
//	       -> NewSlidingCount(size,slide) stage 1: bids per (auction, window)
//	       -> NewQ5Rekey(size)            a Map: re-keys onto the window
//	       -> NewQ5HotItems(size,late)    stage 2: the auctions at the maximum
//	       -> sink
//
// # Why the filter is its own vertex here and inside the operator elsewhere
//
// q1, q2 and q7 drop the non-bid events inside themselves, because each of them
// has to decode the value anyway and a separate vertex would decode every event
// twice with a shuffle in between. Stage 1 is operators.WindowCount, which
// counts records WITHOUT looking at them: it cannot do the filtering, and a
// person event reaching it would open a window keyed on a person id -- a row
// the oracle does not have, on a key nothing will ever bid on. So the filter
// gets its own vertex, and only here.
//
// # Why the re-key is its own vertex
//
// Stage 2 groups by WINDOW, and a record's key is what the shuffle partitions
// on, so stage 1's output has to carry the window as its key by the time it
// crosses the edge into stage 2. An operator's emitted key is what the writer
// behind it partitions on, so the change has to happen in the vertex BEFORE
// that edge -- which is what this Map is. Doing it inside stage 2 would be too
// late: the record would already have been sent to whichever subtask the
// auction id chose, and each subtask would compute the maximum of its own share
// of the window with nothing to say it had.
//
// # Why stage 1's emission is at end-1, and why that is load-bearing here
//
// WindowCount stamps a fired window with its END-1 rather than its start. Until
// Phase 3c it did not, and this is the first pipeline where the difference is
// reachable: a watermark w that fires the window [start, end) has already gone
// past start, so a record stamped with start arrives at stage 2 behind a
// watermark that has already been forwarded, and stage 2 -- which has a
// lateness rule -- would see the ENTIRE upstream output as late and drop all of
// it. The output would be empty, with no error anywhere, and it would look like
// a windowing bug in stage 2.
//
// Stage 2 counts what it accepts as well as what it drops for that reason; see
// HotItems.OnTime.

// NewBidsOnly returns a Map keeping the bid events and dropping the rest.
//
// A Map and not a Filter for the reason at the top of this file: the type
// discriminator has to be read out of the value, and a value that does not
// decode is a layout disagreement rather than an event of the wrong type. A
// predicate cannot tell the two apart.
//
// The record is forwarded as it arrived rather than rebuilt. This vertex
// selects; a rebuilt record would be a second encoding of one event.
func NewBidsOnly() *Map {
	return NewMap(func(rec *core.Record) (*core.Record, error) {
		typ, err := sources.NexmarkTypeOf(rec.Value)
		if err != nil {
			return nil, fmt.Errorf("operators: q5: %w", err)
		}
		if typ != sources.EventBid {
			return nil, nil
		}
		return rec, nil
	})
}

// auctionCountBytes is the width of a stage-1 output value.
const auctionCountBytes = 16

// AuctionCount is what stage 1 hands stage 2: one auction's bid count in one
// window.
//
// The auction moves from the KEY into the VALUE, because the key becomes the
// window. Nothing is lost and nothing is duplicated: after the re-key the
// record says (window, auction, count) with the window in the key and the
// other two in the value.
type AuctionCount struct {
	Auction uint64
	Count   int64
}

// EncodeAuctionCount renders ac as sixteen bytes: the auction then the count,
// both big-endian.
func EncodeAuctionCount(ac AuctionCount) []byte {
	var buf [auctionCountBytes]byte
	binary.BigEndian.PutUint64(buf[0:8], ac.Auction)
	binary.BigEndian.PutUint64(buf[8:16], uint64(ac.Count))
	return buf[:]
}

// DecodeAuctionCount reads a value written by the re-key.
//
// Exactly sixteen bytes, not at least: only the re-key writes these, so another
// length means the layout and the code have come apart, and reading the first
// sixteen bytes of something else would produce two plausible numbers.
func DecodeAuctionCount(value []byte) (AuctionCount, error) {
	if len(value) != auctionCountBytes {
		return AuctionCount{}, fmt.Errorf("operators: q5: an auction count is %d bytes, want %d",
			len(value), auctionCountBytes)
	}
	return AuctionCount{
		Auction: binary.BigEndian.Uint64(value[0:8]),
		Count:   int64(binary.BigEndian.Uint64(value[8:16])),
	}, nil
}

// WindowKey renders a window start as the eight big-endian bytes stage 2 is
// keyed on.
//
// Big-endian and fixed width so that every one of a window's counts hashes to
// one subtask and so that stage 2's composite state keys split unambiguously.
// It is the same shape a Nexmark id key has, and deliberately so: the engine
// has one key encoding.
func WindowKey(windowStart int64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(windowStart))
	return buf[:]
}

// WindowKeyStart reads a window start back out of a stage-2 key.
func WindowKeyStart(key []byte) (int64, error) {
	if len(key) != 8 {
		return 0, fmt.Errorf("operators: q5: a window key is %d bytes, want 8", len(key))
	}
	return int64(binary.BigEndian.Uint64(key)), nil
}

// NewQ5Rekey returns the Map between q5's two stages: it moves the auction out
// of the key and the window in.
//
// size is the sliding window size stage 1 was built with, and it is how the
// window start is recovered: a fired window carries its END-1, so the start is
// EventTime-(size-1). Nothing is lost by that -- every reader already knows the
// size it asked for -- but the two do have to agree, and a mismatch here is
// silent: it would shift every window by a constant and every row would still
// look plausible. Stage 2 checks the agreement rather than trusting it; see
// HotItems.ProcessElement.
//
// The event time is untouched. It is still the window's end-1, which is exactly
// the watermark that completes the window, so stage 2 receives its input ahead
// of the watermark that releases it rather than behind it.
func NewQ5Rekey(size int64) *Map {
	if size <= 0 {
		panic(fmt.Sprintf("operators: NewQ5Rekey: size is %d, must be > 0", size))
	}
	return NewMap(func(rec *core.Record) (*core.Record, error) {
		auction, err := sources.NexmarkKeyID(rec.Key)
		if err != nil {
			return nil, fmt.Errorf("operators: q5 re-key: %w", err)
		}
		count, err := DecodeCount(rec.Value)
		if err != nil {
			return nil, fmt.Errorf("operators: q5 re-key: %w", err)
		}
		return &core.Record{
			Key:       WindowKey(subFloor(rec.EventTime, size-1)),
			Value:     EncodeAuctionCount(AuctionCount{Auction: auction, Count: count}),
			EventTime: rec.EventTime,
		}, nil
	})
}

// HotItems is q5's second stage: per window, the auctions with the highest bid
// count.
//
// ALL of the auctions attaining the maximum, not one of them. That is Nexmark's
// own semantic -- "num >= ALL" -- and it is what makes a tie deterministic
// without a rule to break it with: a tie is several output rows, and the set of
// rows is what the sink is compared on. Within one window the rows come out in
// ascending AUCTION order, which is not arranged by a sort but falls out of the
// state layout below; see ProcessWatermark.
//
// # State layout
//
// Aggregates, under state.PrefixUserState:
//
//	key    state.PrefixUserState, then the eight-byte window key, then the
//	       auction as a big-endian uint64
//	value  the count as a big-endian int64, eight bytes
//
// Both fields after the discriminator are fixed width, so the split is by
// offset and needs no separator. The window key leads, so one window's counts
// are a contiguous run in ascending byte order and the auctions within it are
// ascending too -- which is where the emission order comes from.
//
// Timers, under state.PrefixTimer, in the layout WindowCount and q7 use:
//
//	key    state.PrefixTimer, then the fire time as state.EncodeOrderedInt64,
//	       then the window key, then the window start as a big-endian int64
//	value  empty
//
// The window key and the trailing window start are the same number written
// twice, which is redundant and is kept anyway: there is then ONE timer layout
// in this package rather than three, and the chaos census parses that layout
// from outside pkg/operators. Eight bytes per open window is the price.
//
// The current watermark, under state.PrefixOperatorState:
//
//	key    state.PrefixOperatorState, then the name "watermark"
//	value  the watermark as state.EncodeOrderedInt64
//
// # The watermark, which is the trap this stage exists to walk past
//
// This operator has a lateness rule, and core.Context.CurrentWatermark() is
// sitting right there looking like the obvious source for it. It is NOT
// restored across a checkpoint: a recovered operator would read the runtime's
// initial minimum, conclude that nothing had been purged, accept records it
// should be dropping, and re-emit a (window, auction) the sink already holds.
// So the watermark lives in this operator's own KeyedState, exactly as
// WindowCount's does, and nothing in this file calls CurrentWatermark. See
// TestNoOperatorReadsTheWatermarkFromTheContext, which is what watches this.
type HotItems struct {
	size            int64
	allowedLateness int64

	state  state.KeyedState
	keyBuf []byte

	// dropped counts stage-1 records discarded as late, and onTime counts the
	// ones accepted. Both are on the Go struct rather than in state, for the
	// reason WindowCount.dropped is.
	//
	// onTime is not decoration. Stage 2 seeing its whole input as late produces
	// EMPTY output and no error, which reads as a windowing bug; a test that
	// only asserted "nothing was dropped" would pass against a stage that
	// received nothing at all. The pair is what makes that assertion mean
	// something.
	dropped int64
	onTime  int64
}

var _ core.Operator = (*HotItems)(nil)

// hotItemKeyBytes is the width of a stage-2 aggregate key: the discriminator,
// the window key and the auction.
const hotItemKeyBytes = prefixBytes + 8 + 8

// NewQ5HotItems returns q5's second stage over windows of size millis, keeping
// fired windows open for allowedLateness millis past their end.
//
// size is the SAME size stage 1 and the re-key were built with. This operator
// could avoid knowing it -- the fire time is the event time its input carries
// -- and takes it anyway so that it can CHECK the agreement: a window key and
// an event time that do not correspond is a mis-wired pipeline, and the symptom
// without the check is every window shifted by a constant with every row still
// looking plausible.
//
// A bad specification panics rather than returning an error, for the reason
// NewSlidingCount panics.
func NewQ5HotItems(size, allowedLateness int64) *HotItems {
	switch {
	case size <= 0:
		panic(fmt.Sprintf("operators: NewQ5HotItems: size is %d, must be > 0", size))
	case allowedLateness < 0:
		panic(fmt.Sprintf("operators: NewQ5HotItems: allowedLateness is %d, must be >= 0", allowedLateness))
	}
	return &HotItems{size: size, allowedLateness: allowedLateness}
}

// Open takes the subtask's keyed state, refusing a nil one.
func (h *HotItems) Open(ctx core.Context) error {
	h.state = ctx.State()
	if h.state == nil {
		return errors.New("operators: q5 stage 2: the runtime provided no keyed state")
	}
	return nil
}

// Dropped returns the number of stage-1 records discarded as late.
func (h *HotItems) Dropped() int64 { return h.dropped }

// OnTime returns the number of stage-1 records accepted into a window.
//
// The number that says stage 2 is doing anything at all. See the note on the
// struct: an all-late stage 2 emits nothing and reports nothing.
func (h *HotItems) OnTime() int64 { return h.onTime }

// appendHotItemKey appends the composite AGGREGATE key for (windowKey,
// auction). See the layout on HotItems:
//
//	state.PrefixUserState || windowKey, 8 bytes || auction, big-endian uint64
//
// parseHotItemKey is directly below and is its inverse. Both fields are fixed
// width, so the split is by offset; change one and the other stops being its
// inverse, which is why they are adjacent.
func appendHotItemKey(dst []byte, windowKey []byte, auction uint64) []byte {
	dst = append(dst, state.PrefixUserState)
	dst = append(dst, windowKey...)
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], auction)
	return append(dst, buf[:]...)
}

// parseHotItemKey splits an aggregate key back into its two fields. The
// returned window key ALIASES stateKey.
func parseHotItemKey(stateKey []byte) (windowKey []byte, auction uint64, err error) {
	if len(stateKey) != hotItemKeyBytes {
		return nil, 0, fmt.Errorf("operators: q5 stage 2: a state key is %d bytes, want %d",
			len(stateKey), hotItemKeyBytes)
	}
	return stateKey[prefixBytes : prefixBytes+8], binary.BigEndian.Uint64(stateKey[prefixBytes+8:]), nil
}

// fireTimeOf returns the watermark at which the window starting at start is
// complete, which is its end-1, saturating.
func (h *HotItems) fireTimeOf(start int64) int64 { return addCeil(start, h.size-1) }

// isPurged reports whether the window starting at start is past its allowed
// lateness at watermark. The same expression WindowCount and q7 use, so that a
// dropped record cannot resurrect a window that has already been reported.
func (h *HotItems) isPurged(watermark, start int64) bool {
	return watermark > addCeil(addCeil(start, h.size), h.allowedLateness)
}

// ProcessElement folds one stage-1 count into its window and arms the window's
// timer.
//
// The record's KEY is the window and its VALUE is (auction, count). Its event
// time is the window's end-1, which is checked against the key rather than
// trusted: the two are the same window said twice, and a pipeline whose re-key
// was built with a different size than this stage would otherwise shift every
// window by a constant with nothing to point at.
func (h *HotItems) ProcessElement(rec *core.Record, ctx core.Context) error {
	windowStart, err := WindowKeyStart(rec.Key)
	if err != nil {
		return err
	}
	if want := h.fireTimeOf(windowStart); rec.EventTime != want {
		return fmt.Errorf("operators: q5 stage 2: a record keyed on window %d carries event time %d, "+
			"but a window of size %d completes at %d: the re-key and this stage disagree about the size",
			windowStart, rec.EventTime, h.size, want)
	}
	ac, err := DecodeAuctionCount(rec.Value)
	if err != nil {
		return err
	}

	watermark, err := loadWatermark(h.state)
	if err != nil {
		return fmt.Errorf("operators: q5 stage 2: %w", err)
	}
	if h.isPurged(watermark, windowStart) {
		h.dropped++
		return nil
	}
	h.onTime++

	// Replaced, not accumulated. Stage 1 emits one record per (auction,
	// window) when that window fires, so a second record for the same pair is
	// a RE-FIRE carrying the updated count rather than an increment to add.
	// Adding would double the count of any window stage 1 re-fired.
	h.keyBuf = appendHotItemKey(h.keyBuf[:0], rec.Key, ac.Auction)
	h.state.Put(h.keyBuf, encodeCount(ac.Count))

	// Armed on every record; the key is a function of (fireTime, window) so
	// writing it again is idempotent.
	h.keyBuf = appendTimerKey(h.keyBuf[:0], rec.EventTime, rec.Key, windowStart)
	h.state.Put(h.keyBuf, nil)
	return nil
}

// hotWindow is one due window's running maximum, built during the single scan
// ProcessWatermark makes.
type hotWindow struct {
	max int64
	// winners are the auctions at max, in the order the scan met them, which
	// is ascending. See ProcessWatermark.
	winners []uint64
}

// ProcessWatermark fires every window the watermark completes, then purges the
// ones it puts out of reach.
//
// # One scan, not one per window
//
// The due timers are collected first, then ONE pass over the aggregate
// partition finds every due window's maximum together. A scan per firing window
// would be O(windows fired * aggregates held) on a watermark that completes
// several windows, which a sliding specification does routinely.
//
// That pass runs in ascending byte order, and the aggregate key is
// (window, auction), so a window's counts arrive contiguously with the auctions
// ascending. The winners of a tie therefore come out in ascending auction order
// with nothing sorted: the order is a property of the layout, which is worth
// stating because it is no longer visible as code.
//
// # Collect, then fire
//
// Firing happens after both scans have ENDED. fire reads and Put writes keys
// OTHER than the one a callback was handed, and KeyedState.Iterate leaves that
// undefined precisely because the two backends disagree: Memory looks each key
// up again as it reaches it, Pebble reads a view fixed when the scan began.
func (h *HotItems) ProcessWatermark(wm int64, ctx core.Context) error {
	storeWatermark(h.state, wm)
	due, err := collectDue(h.state, wm)
	if err != nil {
		return fmt.Errorf("operators: q5 stage 2: %w", err)
	}
	if len(due) == 0 {
		return h.purge(wm)
	}

	// The due windows, by window key, so the scan below can recognise one in
	// constant time.
	windows := make(map[string]*hotWindow, len(due))
	for _, t := range due {
		windows[string(t.key)] = &hotWindow{}
	}

	var scanErr error
	h.state.Iterate(func(stateKey, value []byte) bool {
		if len(stateKey) == 0 {
			scanErr = errors.New("operators: q5 stage 2: state holds a zero-length key")
			return false
		}
		if stateKey[0] != state.PrefixUserState {
			return false // past the aggregates
		}
		windowKey, auction, err := parseHotItemKey(stateKey)
		if err != nil {
			scanErr = err
			return false
		}
		w, ok := windows[string(windowKey)]
		if !ok {
			return true // not firing on this watermark
		}
		count, err := DecodeCount(value)
		if err != nil {
			scanErr = fmt.Errorf("operators: q5 stage 2: auction %d: %w", auction, err)
			return false
		}
		switch {
		case len(w.winners) == 0 || count > w.max:
			w.max = count
			w.winners = append(w.winners[:0], auction)
		case count == w.max:
			w.winners = append(w.winners, auction)
		}
		return true
	})
	if scanErr != nil {
		return scanErr
	}

	for _, t := range due {
		// Deleted BEFORE emitting, so that anything arriving later re-arms the
		// window rather than having its timer removed underneath.
		h.state.Delete(t.stateKey)
		w := windows[string(t.key)]
		if len(w.winners) == 0 {
			// Unreachable: the purge runs after this loop and only removes
			// windows whose timers this call has already fired, so a due timer
			// always has aggregates. Reported rather than passed over, because
			// a window that silently produced no row is indistinguishable from
			// a window with no bids.
			return fmt.Errorf("operators: q5 stage 2: window %d fired with no counts", t.windowStart)
		}
		for _, auction := range w.winners {
			ctx.Emit(&core.Record{
				Key:       sources.NexmarkKey(auction),
				Value:     encodeCount(w.max),
				EventTime: h.fireTimeOf(t.windowStart),
			})
		}
	}
	return h.purge(wm)
}

// purge drops the aggregates of every window the watermark has moved past.
//
// The window start is read out of the WINDOW KEY, which leads the composite
// key, and NOT off the end of it -- the last eight bytes here are the auction.
// See purgeWindows: reading the end would hand this an auction id and purge
// every aggregate on every watermark.
func (h *HotItems) purge(wm int64) error {
	return purgeWindows(h.state, func(stateKey []byte) (bool, error) {
		windowKey, _, err := parseHotItemKey(stateKey)
		if err != nil {
			return false, err
		}
		start, err := WindowKeyStart(windowKey)
		if err != nil {
			return false, err
		}
		return h.isPurged(wm, start), nil
	})
}

// OnEndOfStream does nothing. The windows still open when the input runs out
// are flushed by the gate's MaxInt64 watermark, which arrives immediately
// before end-of-stream; flushing here as well would be a second mechanism for
// one job.
func (h *HotItems) OnEndOfStream(ctx core.Context) error { return nil }

// Snapshot refuses rather than writing nothing: this operator's state is the
// subtask's KeyedState, which pkg/checkpoint serialises directly, and a
// zero-byte snapshot would be a claim that there is nothing to keep.
func (h *HotItems) Snapshot(w io.Writer) error {
	return errors.New("operators: q5 stage 2 cannot be snapshotted through core.Operator: its state is the subtask's KeyedState, which pkg/checkpoint serialises directly")
}

// Restore refuses for the same reason Snapshot does.
func (h *HotItems) Restore(r io.Reader) error {
	return errors.New("operators: q5 stage 2 cannot be restored through core.Operator: its state is the subtask's KeyedState, which the runtime restores directly")
}

func (h *HotItems) Close() error { return nil }
