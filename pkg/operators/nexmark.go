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
	return purgeWindows(m.state, func(start int64) bool { return m.isPurged(wm, start) })
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

// purgeWindows deletes the aggregate of every window purged reports true for.
//
// Confined to the user-state partition and STOPS at the first key outside it,
// which the layout puts contiguously at the end. That confinement is not
// decoration: a timer key also carries a window start in its last eight bytes,
// so an unconfined scan would delete timers it was never asked about and
// windows would simply stop firing, with nothing to report.
//
// KeyedState.Iterate permits the callback to delete the entry it is handed and
// only that entry, which is what this does.
func purgeWindows(st state.KeyedState, purged func(windowStart int64) bool) error {
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
		start, decodeErr := windowStartOf(stateKey)
		if decodeErr != nil {
			err = decodeErr
			return false
		}
		if purged(start) {
			st.Delete(stateKey)
		}
		return true
	})
	return err
}
