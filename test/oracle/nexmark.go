package oracle

import (
	"cmp"
	"fmt"
	"slices"
	"strings"

	"github.com/AarinB1/tidemark/pkg/core"
	"github.com/AarinB1/tidemark/pkg/sources"
)

// The Nexmark oracles: one per query, each a brute-force second writing of what
// the engine is supposed to produce.
//
// Same rules as the windowed oracle above, and for the same reasons.
//
// It imports pkg/core and pkg/sources and NOTHING from pkg/runtime,
// pkg/transport or pkg/operators. If one of these needed a gate, a timer or a
// watermark it would be a second implementation of the engine rather than an
// oracle, and two implementations of one design agree about the same mistakes.
// What is under test is the filtering, the windowing and the aggregation, so
// those are what is written out again here from scratch.
//
// The input is REGENERATED from the config rather than handed over. A
// materialised slice would be a second thing that could be wrong, and at the
// counts the chaos suite runs it would be the memory high-water mark of the
// whole test.
//
// The record LAYOUT is shared, through sources.DecodeBid and its neighbours,
// and that is not a hole in the independence. The engine and the oracle have to
// read the same events for a comparison between them to mean anything, so a
// second decoder here would not add independence: it would let the two sides
// disagree about their input, and the symptom would be a diff in the result
// that has nothing to do with the query.
//
// There is no sort in any of them. Every one assigns and accumulates into a map
// in a single pass, because Phase 6b and the chaos suite each run them many
// times; sorting happens only in the Sorted* helpers, which exist for the
// comparison and are not part of the computation.
//
// There is no lateness model in any of them either, deliberately. The engine
// drops a record whose window has been purged; nothing here can, because
// nothing here has a watermark. Every comparison is run at a lateness where no
// record is ever late and asserts the engine's drop count is zero, which turns
// "the oracle does not model this" into a checked precondition. An oracle
// complicated enough to model lateness is not an oracle.

// NexmarkRow is one output record of a stateless query, flattened for
// comparison: the three fields of a core.Record with the byte slices as
// strings so the whole thing is a comparable map key.
type NexmarkRow struct {
	Key       string
	Value     string
	EventTime int64
}

// rowOf flattens a record. Used by the stateless oracles and by the tests that
// compare engine output against them.
func rowOf(rec *core.Record) NexmarkRow {
	return NexmarkRow{Key: string(rec.Key), Value: string(rec.Value), EventTime: rec.EventTime}
}

// CompareNexmarkRows orders rows by key, then value, then event time. A total
// order, so that two multisets flattened by SortedNexmarkRows compare
// element-wise and the first difference reported is a real one.
func CompareNexmarkRows(a, b NexmarkRow) int {
	if c := strings.Compare(a.Key, b.Key); c != 0 {
		return c
	}
	if c := strings.Compare(a.Value, b.Value); c != 0 {
		return c
	}
	return cmp.Compare(a.EventTime, b.EventTime)
}

// SortedNexmarkRows flattens a multiset into a sorted slice with one entry per
// copy.
//
// A MULTISET and not a set. Two events can legitimately encode to the same
// bytes at the same time -- the lag can put two offsets on one millisecond and
// the fields can coincide -- so collapsing copies would let the engine lose one
// of them without the comparison noticing.
//
// The sort is for the comparison and is not part of any computation. The sink
// holds a set of records in whatever order its subtasks wrote them; comparing
// emission order would be a broken test.
func SortedNexmarkRows(m map[NexmarkRow]int64) []NexmarkRow {
	total := int64(0)
	for _, n := range m {
		total += n
	}
	out := make([]NexmarkRow, 0, total)
	for row, n := range m {
		for range n {
			out = append(out, row)
		}
	}
	slices.SortFunc(out, CompareNexmarkRows)
	return out
}

// readNexmark regenerates the input and feeds every event to visit.
//
// One reader for all five oracles, so that no query can differ from another in
// what it was shown. It is the only place any of them touches a source.
func readNexmark(cfg sources.NexmarkConfig, visit func(*core.Record) error) error {
	src := sources.NewNexmark(cfg)
	if err := src.Open(nil); err != nil {
		return fmt.Errorf("oracle: nexmark: open: %w", err)
	}
	defer func() { _ = src.Close() }()
	for {
		rec, ok, err := src.Next()
		if err != nil {
			return fmt.Errorf("oracle: nexmark: next: %w", err)
		}
		if !ok {
			return nil
		}
		if err := visit(rec); err != nil {
			return err
		}
	}
}

// NexmarkQ0 is the passthrough: every event, unchanged.
//
// It is the raw transport ceiling, so the oracle is the identity and the whole
// content of the comparison is that nothing was lost, duplicated or altered on
// the way through the engine. Under the chaos suite that is the strongest of
// the five: a stateless query has no window to hide a dropped record inside.
func NexmarkQ0(cfg sources.NexmarkConfig) (map[NexmarkRow]int64, error) {
	out := make(map[NexmarkRow]int64)
	if err := readNexmark(cfg, func(rec *core.Record) error {
		q0Accumulate(out, rec)
		return nil
	}); err != nil {
		return nil, err
	}
	return out, nil
}

func q0Accumulate(out map[NexmarkRow]int64, rec *core.Record) { out[rowOf(rec)]++ }

// NexmarkQ1 converts every bid's price by factor and drops everything else.
//
// Nexmark q1 is a currency conversion over the BID stream, so the events that
// are not bids are not part of its input. Dropping them is the query rather
// than a filter bolted onto it, which is why there is no separate predicate
// here and no separate operator in the engine.
//
// The multiplication is uint64 and is allowed to wrap. Both sides wrap
// identically, so the comparison stays exact; a caller who wants prices that
// mean something keeps factor*PriceRange inside the range.
func NexmarkQ1(cfg sources.NexmarkConfig, factor uint64) (map[NexmarkRow]int64, error) {
	out := make(map[NexmarkRow]int64)
	if err := readNexmark(cfg, func(rec *core.Record) error {
		return q1Accumulate(out, rec, factor)
	}); err != nil {
		return nil, err
	}
	return out, nil
}

func q1Accumulate(out map[NexmarkRow]int64, rec *core.Record, factor uint64) error {
	typ, err := sources.NexmarkTypeOf(rec.Value)
	if err != nil {
		return fmt.Errorf("oracle: q1: %w", err)
	}
	if typ != sources.EventBid {
		return nil
	}
	bid, err := sources.DecodeBid(rec.Value)
	if err != nil {
		return fmt.Errorf("oracle: q1: %w", err)
	}
	bid.Price *= factor
	out[NexmarkRow{
		Key:       string(rec.Key),
		Value:     string(sources.EncodeBid(bid)),
		EventTime: rec.EventTime,
	}]++
	return nil
}

// NexmarkQ2 keeps the bids whose auction id is a multiple of divisor.
//
// Nexmark q2 selects bids on a handful of named auctions; the modulo is the
// standard restatement of that, and it is a knob rather than a constant so that
// the SELECTIVITY is something a test can choose and measure. A filter passing
// ninety-nine per cent or one per cent of its input exercises nothing, and
// selectivity is the reason q2 is in this set at all.
//
// The predicate is written out again here rather than shared with the operator.
// It is the logic under test; sharing it would make the comparison vacuous.
func NexmarkQ2(cfg sources.NexmarkConfig, divisor uint64) (map[NexmarkRow]int64, error) {
	if divisor == 0 {
		return nil, fmt.Errorf("oracle: q2: divisor is 0")
	}
	out := make(map[NexmarkRow]int64)
	if err := readNexmark(cfg, func(rec *core.Record) error {
		return q2Accumulate(out, rec, divisor)
	}); err != nil {
		return nil, err
	}
	return out, nil
}

func q2Accumulate(out map[NexmarkRow]int64, rec *core.Record, divisor uint64) error {
	typ, err := sources.NexmarkTypeOf(rec.Value)
	if err != nil {
		return fmt.Errorf("oracle: q2: %w", err)
	}
	if typ != sources.EventBid {
		return nil
	}
	bid, err := sources.DecodeBid(rec.Value)
	if err != nil {
		return fmt.Errorf("oracle: q2: %w", err)
	}
	if bid.Auction%divisor != 0 {
		return nil
	}
	out[rowOf(rec)]++
	return nil
}

// HotItemKey identifies one q5 result row.
type HotItemKey struct {
	WindowStart int64
	Auction     uint64
}

// HotItem is one q5 result row flattened for comparison.
type HotItem struct {
	WindowStart int64
	Auction     uint64
	Count       int64
}

// NexmarkQ5 returns, for every window that received a bid, the auctions with
// the highest bid count in that window.
//
// ALL of the auctions attaining the maximum, not one of them. That is the
// Nexmark semantic -- "num >= ALL" -- and it is what makes the tie rule
// deterministic without inventing one: a tie produces several rows, the set of
// rows is what the sink is compared on, and the engine emits them in ascending
// auction order because its state is keyed that way.
//
// Two passes over maps and no sort. The first assigns each bid to its windows
// and counts per (auction, window), which is the sliding assignment written out
// again; the second walks those counts twice, once to find each window's
// maximum and once to keep the auctions at it.
func NexmarkQ5(cfg sources.NexmarkConfig, spec Spec) (map[HotItemKey]int64, error) {
	if err := spec.check(); err != nil {
		return nil, err
	}
	counts := make(map[HotItemKey]int64)
	if err := readNexmark(cfg, func(rec *core.Record) error {
		return q5Accumulate(counts, rec, spec)
	}); err != nil {
		return nil, err
	}
	return q5Select(counts), nil
}

// q5Accumulate adds one event to the per-(auction, window) counts.
//
// The window assignment is a deliberate second writing of the one in
// pkg/operators/window.go, exactly as accumulate above is. Non-bids are not
// part of q5's input and are skipped here, which is the same shape the engine's
// filter gives it.
func q5Accumulate(counts map[HotItemKey]int64, rec *core.Record, spec Spec) error {
	typ, err := sources.NexmarkTypeOf(rec.Value)
	if err != nil {
		return fmt.Errorf("oracle: q5: %w", err)
	}
	if typ != sources.EventBid {
		return nil
	}
	bid, err := sources.DecodeBid(rec.Value)
	if err != nil {
		return fmt.Errorf("oracle: q5: %w", err)
	}
	start := rec.EventTime - floorMod(rec.EventTime, spec.Slide)
	for n := spec.Size / spec.Slide; n > 0; n-- {
		counts[HotItemKey{WindowStart: start, Auction: bid.Auction}]++
		start -= spec.Slide
	}
	return nil
}

// q5Select keeps, per window, the auctions whose count is that window's
// maximum.
func q5Select(counts map[HotItemKey]int64) map[HotItemKey]int64 {
	best := make(map[int64]int64)
	for k, n := range counts {
		if cur, ok := best[k.WindowStart]; !ok || n > cur {
			best[k.WindowStart] = n
		}
	}
	out := make(map[HotItemKey]int64)
	for k, n := range counts {
		if n == best[k.WindowStart] {
			out[k] = n
		}
	}
	return out
}

// SortedHotItems flattens q5's answer into rows ordered by window then auction.
func SortedHotItems(m map[HotItemKey]int64) []HotItem {
	out := make([]HotItem, 0, len(m))
	for k, n := range m {
		out = append(out, HotItem{WindowStart: k.WindowStart, Auction: k.Auction, Count: n})
	}
	slices.SortFunc(out, CompareHotItems)
	return out
}

// CompareHotItems orders by window start, then auction, then count. Count is
// last so that two result sets differing only in a count still sort alongside
// each other and the first difference reported is the interesting one.
func CompareHotItems(a, b HotItem) int {
	if c := cmp.Compare(a.WindowStart, b.WindowStart); c != 0 {
		return c
	}
	if c := cmp.Compare(a.Auction, b.Auction); c != 0 {
		return c
	}
	return cmp.Compare(a.Count, b.Count)
}

// MaxBidKey identifies one q7 result row: an auction and a tumbling window.
//
// Per AUCTION and window, because a bid's Record.Key is its auction id and that
// is what the engine partitions on. A maximum over a whole window regardless of
// auction cannot be computed by one keyed operator at a parallelism above 1 --
// each subtask would see a share of the window's bids and report the maximum of
// that share -- so it is not what the engine computes and not what this
// oracle computes either.
type MaxBidKey struct {
	WindowStart int64
	Auction     uint64
}

// MaxBid is the winning bid of one (auction, window).
type MaxBid struct {
	Price  uint64
	Bidder uint64
}

// MaxBidRow is one q7 result row flattened for comparison.
type MaxBidRow struct {
	WindowStart int64
	Auction     uint64
	Price       uint64
	Bidder      uint64
}

// NexmarkQ7 returns the highest bid in each (auction, tumbling window).
//
// Ties are broken by the lowest bidder id and then by the lowest auction id.
// The comparator is total for a reason that has nothing to do with taste:
// records reach an operator in whatever order the shuffle produced, so a
// winner that depended on arrival order would make this comparison flaky and
// the flake would read as a windowing bug. The auction tier is unreachable
// through this grouping -- the auction is constant within a key -- and is
// written anyway so that the rule is a total order rather than one that
// happens to be total here; see BetterBid, which is tested directly.
func NexmarkQ7(cfg sources.NexmarkConfig, size int64) (map[MaxBidKey]MaxBid, error) {
	if err := (Spec{Size: size, Slide: size}).check(); err != nil {
		return nil, err
	}
	out := make(map[MaxBidKey]MaxBid)
	if err := readNexmark(cfg, func(rec *core.Record) error {
		return q7Accumulate(out, rec, size)
	}); err != nil {
		return nil, err
	}
	return out, nil
}

func q7Accumulate(out map[MaxBidKey]MaxBid, rec *core.Record, size int64) error {
	typ, err := sources.NexmarkTypeOf(rec.Value)
	if err != nil {
		return fmt.Errorf("oracle: q7: %w", err)
	}
	if typ != sources.EventBid {
		return nil
	}
	bid, err := sources.DecodeBid(rec.Value)
	if err != nil {
		return fmt.Errorf("oracle: q7: %w", err)
	}
	k := MaxBidKey{
		WindowStart: rec.EventTime - floorMod(rec.EventTime, size),
		Auction:     bid.Auction,
	}
	cur, ok := out[k]
	if !ok || BetterBid(bid.Price, bid.Bidder, k.Auction, cur.Price, cur.Bidder, k.Auction) {
		out[k] = MaxBid{Price: bid.Price, Bidder: bid.Bidder}
	}
	return nil
}

// BetterBid reports whether (price, bidder, auction) beats (price2, bidder2,
// auction2) under q7's rule: the highest price wins, ties go to the lowest
// bidder id, and remaining ties to the lowest auction id.
//
// Exported so that it can be tested directly on inputs the query cannot
// produce. The auction tier is unreachable inside q7, where the auction is the
// grouping key, so a test that only ran the query would leave a third of this
// rule unexercised while looking like it covered it.
func BetterBid(price, bidder, auction, price2, bidder2, auction2 uint64) bool {
	if price != price2 {
		return price > price2
	}
	if bidder != bidder2 {
		return bidder < bidder2
	}
	return auction < auction2
}

// SortedMaxBids flattens q7's answer into rows ordered by window then auction.
func SortedMaxBids(m map[MaxBidKey]MaxBid) []MaxBidRow {
	out := make([]MaxBidRow, 0, len(m))
	for k, v := range m {
		out = append(out, MaxBidRow{
			WindowStart: k.WindowStart, Auction: k.Auction, Price: v.Price, Bidder: v.Bidder,
		})
	}
	slices.SortFunc(out, CompareMaxBids)
	return out
}

// CompareMaxBids orders by window start, then auction, then price, then bidder.
func CompareMaxBids(a, b MaxBidRow) int {
	if c := cmp.Compare(a.WindowStart, b.WindowStart); c != 0 {
		return c
	}
	if c := cmp.Compare(a.Auction, b.Auction); c != 0 {
		return c
	}
	if c := cmp.Compare(a.Price, b.Price); c != 0 {
		return c
	}
	return cmp.Compare(a.Bidder, b.Bidder)
}

// The *Records forms below are the oracles over events already in hand.
//
// They exist so that the oracles themselves can be checked against fixtures
// written out by a person. The oracle is what everything else is compared
// against, so it is the one place in this repository where the expected values
// must not be produced by any code in it -- and nobody can compute a splitmix64
// stream in their head. The exported forms above regenerate their input and
// call straight into these, so the fixture checks the same arithmetic the
// engine is compared against rather than a copy of it.

func q0Records(recs []*core.Record) map[NexmarkRow]int64 {
	out := make(map[NexmarkRow]int64)
	for _, rec := range recs {
		q0Accumulate(out, rec)
	}
	return out
}

func q1Records(recs []*core.Record, factor uint64) (map[NexmarkRow]int64, error) {
	out := make(map[NexmarkRow]int64)
	for _, rec := range recs {
		if err := q1Accumulate(out, rec, factor); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func q2Records(recs []*core.Record, divisor uint64) (map[NexmarkRow]int64, error) {
	out := make(map[NexmarkRow]int64)
	for _, rec := range recs {
		if err := q2Accumulate(out, rec, divisor); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func q5Records(recs []*core.Record, spec Spec) (map[HotItemKey]int64, error) {
	if err := spec.check(); err != nil {
		return nil, err
	}
	counts := make(map[HotItemKey]int64)
	for _, rec := range recs {
		if err := q5Accumulate(counts, rec, spec); err != nil {
			return nil, err
		}
	}
	return q5Select(counts), nil
}

func q7Records(recs []*core.Record, size int64) (map[MaxBidKey]MaxBid, error) {
	if err := (Spec{Size: size, Slide: size}).check(); err != nil {
		return nil, err
	}
	out := make(map[MaxBidKey]MaxBid)
	for _, rec := range recs {
		if err := q7Accumulate(out, rec, size); err != nil {
			return nil, err
		}
	}
	return out, nil
}
