package operators

import (
	"fmt"

	"github.com/AarinB1/tidemark/pkg/core"
	"github.com/AarinB1/tidemark/pkg/sources"
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
