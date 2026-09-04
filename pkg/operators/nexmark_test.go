package operators

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"
	"slices"
	"strings"
	"testing"

	"github.com/AarinB1/tidemark/pkg/core"
	"github.com/AarinB1/tidemark/pkg/sources"
	"github.com/AarinB1/tidemark/pkg/state"
)

// nexmarkConfig is the input the query tests run over. It is the shape the
// equivalence suite uses, at a size one operator can be driven through by hand.
func nexmarkConfig(seed uint64, count int64) sources.NexmarkConfig {
	return sources.NexmarkConfig{
		Seed:               seed,
		Count:              count,
		AuctionCardinality: 64,
		PersonCardinality:  16,
		PriceRange:         1000,
		CategoryCount:      4,
		AuctionDuration:    5000,
		BaseEventTime:      1700000000000,
		EventTimeStep:      10,
		MaxLag:             200,
	}
}

// nexmarkEvents reads a source's whole output into a slice, for a test that
// drives one operator directly.
func nexmarkEvents(t *testing.T, cfg sources.NexmarkConfig) []*core.Record {
	t.Helper()
	src := sources.NewNexmark(cfg)
	if err := src.Open(nil); err != nil {
		t.Fatalf("Open: %v", err)
	}
	var out []*core.Record
	for {
		rec, ok, err := src.Next()
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if !ok {
			return out
		}
		out = append(out, rec)
	}
}

// runQuery feeds recs through op and returns what it emitted.
func runQuery(t *testing.T, op core.Operator, recs []*core.Record) []*core.Record {
	t.Helper()
	ctx := &emitContext{}
	if err := op.Open(ctx); err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i, rec := range recs {
		if err := op.ProcessElement(rec, ctx); err != nil {
			t.Fatalf("ProcessElement(%d): %v", i, err)
		}
	}
	return ctx.emitted
}

// bidRecord builds one bid the way the source would.
func bidRecord(auction, bidder, price uint64, t int64) *core.Record {
	b := sources.Bid{Auction: auction, Bidder: bidder, Price: price, DateTime: t}
	return &core.Record{Key: sources.NexmarkKey(auction), Value: sources.EncodeBid(b), EventTime: t}
}

func personRecord(id uint64, t int64) *core.Record {
	p := sources.Person{ID: id, DateTime: t}
	return &core.Record{Key: sources.NexmarkKey(id), Value: sources.EncodePerson(p), EventTime: t}
}

func auctionRecord(id uint64, t int64) *core.Record {
	a := sources.Auction{ID: id, DateTime: t, Expires: t + 1}
	return &core.Record{Key: sources.NexmarkKey(id), Value: sources.EncodeAuction(a), EventTime: t}
}

// mixedEvents is a small hand-written stream carrying all three types, so a
// query that failed to drop the non-bids shows up rather than being averaged
// away in a generated stream.
func mixedEvents() []*core.Record {
	return []*core.Record{
		personRecord(1, 10),
		auctionRecord(7, 15),
		bidRecord(7, 1, 50, 20),
		bidRecord(8, 2, 90, 30),
		bidRecord(9, 3, 7, 40),
		personRecord(2, 50),
		bidRecord(10, 4, 0, 60),
		auctionRecord(9, 70),
		bidRecord(6, 5, 1000, 80),
	}
}

func sameRecord(a, b *core.Record) bool {
	return bytes.Equal(a.Key, b.Key) && bytes.Equal(a.Value, b.Value) && a.EventTime == b.EventTime
}

// TestQ0IsAPassthrough.
//
// Every event, unchanged, INCLUDING the persons and the auctions. That is the
// difference between q0 and the other four, and it is the only thing a
// passthrough can be checked for.
func TestQ0IsAPassthrough(t *testing.T) {
	in := mixedEvents()
	got := runQuery(t, NewQ0(), in)
	if len(got) != len(in) {
		t.Fatalf("q0 emitted %d records for %d events", len(got), len(in))
	}
	for i := range in {
		if !sameRecord(got[i], in[i]) {
			t.Fatalf("q0 changed event %d: key %x value %x time %d, want key %x value %x time %d",
				i, got[i].Key, got[i].Value, got[i].EventTime, in[i].Key, in[i].Value, in[i].EventTime)
		}
	}

	// The types are counted rather than assumed, so a fixture that quietly
	// stopped carrying persons would not make this vacuous.
	counts := map[sources.NexmarkEventType]int{}
	for _, rec := range got {
		typ, err := sources.NexmarkTypeOf(rec.Value)
		if err != nil {
			t.Fatalf("NexmarkTypeOf: %v", err)
		}
		counts[typ]++
	}
	for _, typ := range []sources.NexmarkEventType{sources.EventPerson, sources.EventAuction, sources.EventBid} {
		if counts[typ] == 0 {
			t.Errorf("q0 emitted no %s events; the fixture cannot say a passthrough passes everything", typ)
		}
	}
}

// TestQ1ConvertsPricesAndDropsTheRest.
func TestQ1ConvertsPricesAndDropsTheRest(t *testing.T) {
	const factor = 7
	in := mixedEvents()
	got := runQuery(t, NewQ1(factor), in)

	// Hand-computed off mixedEvents: the five bids, prices times seven.
	want := []*core.Record{
		bidRecord(7, 1, 350, 20),
		bidRecord(8, 2, 630, 30),
		bidRecord(9, 3, 49, 40),
		bidRecord(10, 4, 0, 60),
		bidRecord(6, 5, 7000, 80),
	}
	if len(got) != len(want) {
		t.Fatalf("q1 emitted %d records, want %d", len(got), len(want))
	}
	for i := range want {
		if !sameRecord(got[i], want[i]) {
			b, _ := sources.DecodeBid(got[i].Value)
			w, _ := sources.DecodeBid(want[i].Value)
			t.Fatalf("q1 record %d is %+v at key %x time %d, want %+v at key %x time %d",
				i, b, got[i].Key, got[i].EventTime, w, want[i].Key, want[i].EventTime)
		}
	}

	// A factor of zero would flatten every price and a factor of one would make
	// q1 a passthrough over bids; either would pass a test that only checked
	// the shape. This says the price actually moved.
	if bytes.Equal(got[0].Value, in[2].Value) {
		t.Error("q1 left a bid's value unchanged")
	}
}

// TestQ2KeepsTheMatchingBidsUnchanged.
func TestQ2KeepsTheMatchingBidsUnchanged(t *testing.T) {
	in := mixedEvents()

	tests := []struct {
		divisor uint64
		want    []*core.Record
	}{
		// Auction ids in the fixture are 7, 8, 9, 10 and 6.
		{divisor: 3, want: []*core.Record{bidRecord(9, 3, 7, 40), bidRecord(6, 5, 1000, 80)}},
		{divisor: 2, want: []*core.Record{bidRecord(8, 2, 90, 30), bidRecord(10, 4, 0, 60), bidRecord(6, 5, 1000, 80)}},
		{divisor: 7, want: []*core.Record{bidRecord(7, 1, 50, 20)}},
	}
	for _, tt := range tests {
		got := runQuery(t, NewQ2(tt.divisor), in)
		if len(got) != len(tt.want) {
			t.Fatalf("divisor %d: q2 emitted %d records, want %d", tt.divisor, len(got), len(tt.want))
		}
		for i := range tt.want {
			if !sameRecord(got[i], tt.want[i]) {
				t.Fatalf("divisor %d: q2 record %d is key %x time %d, want key %x time %d",
					tt.divisor, i, got[i].Key, got[i].EventTime, tt.want[i].Key, tt.want[i].EventTime)
			}
		}
	}
}

// TestQ2Selectivity is why q2 is in this set, and it is a measurement rather
// than a claim.
//
// A filter passing ninety-nine per cent or one per cent of its input exercises
// nothing: at the top it is a passthrough with extra steps and at the bottom
// every downstream assertion runs on a handful of records. The band below is
// wide -- a fifth to two thirds -- because it is guarding against a divisor
// that has drifted into either of those, not pinning a number.
//
// The figure is logged as well as asserted, because it is what the phase
// reports.
func TestQ2Selectivity(t *testing.T) {
	const count = 20000
	events := nexmarkEvents(t, nexmarkConfig(1, count))

	bids := 0
	for _, rec := range events {
		typ, err := sources.NexmarkTypeOf(rec.Value)
		if err != nil {
			t.Fatalf("NexmarkTypeOf: %v", err)
		}
		if typ == sources.EventBid {
			bids++
		}
	}

	kept := len(runQuery(t, NewQ2(Q2Divisor), events))
	ofAll := float64(kept) / float64(len(events))
	ofBids := float64(kept) / float64(bids)
	t.Logf("q2 at divisor %d kept %d of %d events (%.1f%%), which is %d of %d bids (%.1f%%)",
		Q2Divisor, kept, len(events), 100*ofAll, kept, bids, 100*ofBids)

	if ofAll < 0.20 || ofAll > 0.65 {
		t.Errorf("q2 passes %.1f%% of the stream; outside [20%%, 65%%] it is a passthrough or a trickle, and either exercises nothing",
			100*ofAll)
	}
}

// TestNexmarkStatelessQueriesSnapshotNothing.
//
// q0, q1 and q2 hold nothing across records, so a zero-byte snapshot IS a
// correct snapshot of them -- unlike WindowCount, which refuses. Restore drains
// what it is given rather than ignoring it, which keeps the stream positioned
// for whatever follows.
func TestNexmarkStatelessQueriesSnapshotNothing(t *testing.T) {
	queries := []struct {
		name string
		op   core.Operator
	}{
		{"q0", NewQ0()},
		{"q1", NewQ1(Q1Factor)},
		{"q2", NewQ2(Q2Divisor)},
	}
	for _, q := range queries {
		t.Run(q.name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := q.op.Snapshot(&buf); err != nil {
				t.Fatalf("Snapshot: %v", err)
			}
			if buf.Len() != 0 {
				t.Errorf("Snapshot wrote %d bytes, want none", buf.Len())
			}

			r := strings.NewReader("some bytes another operator wrote")
			if err := q.op.Restore(r); err != nil {
				t.Fatalf("Restore: %v", err)
			}
			if n, _ := io.Copy(io.Discard, r); n != 0 {
				t.Errorf("Restore left %d bytes unread", n)
			}
		})
	}
}

// TestNexmarkStatelessQueriesEmitNothingOnControlElements. Watermarks broadcast
// and the runtime forwards them; an operator that emitted one here would double
// it.
func TestNexmarkStatelessQueriesEmitNothingOnControlElements(t *testing.T) {
	for _, op := range []core.Operator{NewQ0(), NewQ1(Q1Factor), NewQ2(Q2Divisor)} {
		ctx := &emitContext{}
		if err := op.Open(ctx); err != nil {
			t.Fatalf("Open: %v", err)
		}
		if err := op.ProcessWatermark(1000, ctx); err != nil {
			t.Fatalf("ProcessWatermark: %v", err)
		}
		if err := op.OnEndOfStream(ctx); err != nil {
			t.Fatalf("OnEndOfStream: %v", err)
		}
		if err := op.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if len(ctx.emitted) != 0 {
			t.Errorf("a stateless query emitted %d records on control elements", len(ctx.emitted))
		}
	}
}

// TestNexmarkQueriesReportAMalformedValueRatherThanDroppingIt.
//
// This is the reason q2 is a Map and not a Filter, asserted rather than
// explained. A predicate cannot report an error, so a Filter would have to
// answer false for a value it could not decode -- the same answer it gives a
// bid on the wrong auction -- and the record would go missing for a reason
// indistinguishable from the query's own.
//
// The counterfactual is in the test: the same malformed record through a
// Filter with the natural predicate is silently dropped, and the assertion is
// that the two behave differently.
func TestNexmarkQueriesReportAMalformedValueRatherThanDroppingIt(t *testing.T) {
	// A value whose discriminator says "bid" and which is too short to be one.
	truncated := sources.EncodeBid(sources.Bid{Auction: 1, Bidder: 2, Price: 3, DateTime: 4})
	malformed := []*core.Record{
		{Key: sources.NexmarkKey(1), Value: truncated[:len(truncated)-1], EventTime: 4},
		{Key: sources.NexmarkKey(1), Value: []byte{0x7F, 0, 0}, EventTime: 4},
		{Key: sources.NexmarkKey(1), Value: nil, EventTime: 4},
	}

	for i, bad := range malformed {
		for _, q := range []struct {
			name string
			op   core.Operator
		}{
			{"q1", NewQ1(Q1Factor)},
			{"q2", NewQ2(Q2Divisor)},
		} {
			ctx := &emitContext{}
			if err := q.op.Open(ctx); err != nil {
				t.Fatalf("Open: %v", err)
			}
			if err := q.op.ProcessElement(bad, ctx); err == nil {
				t.Errorf("%s accepted malformed value %d (%x) and emitted %d records",
					q.name, i, bad.Value, len(ctx.emitted))
			}
		}

		// The counterfactual. A Filter over the same predicate cannot report
		// anything, so it drops the record and returns nil.
		ctx := &emitContext{}
		filter := NewFilter(func(rec *core.Record) bool {
			bid, err := sources.DecodeBid(rec.Value)
			return err == nil && bid.Auction%Q2Divisor == 0
		})
		if err := filter.ProcessElement(bad, ctx); err != nil || len(ctx.emitted) != 0 {
			t.Fatalf("the counterfactual does not hold: a Filter over malformed value %d returned %v and emitted %d records",
				i, err, len(ctx.emitted))
		}
	}

	// A well-formed value still goes through, so the assertion above is about
	// the malformed ones and not about the queries refusing everything.
	if got := runQuery(t, NewQ2(1), []*core.Record{bidRecord(4, 1, 2, 3)}); len(got) != 1 {
		t.Errorf("q2 at divisor 1 emitted %d records for one bid, want 1", len(got))
	}
}

// TestNewQ2RejectsAZeroDivisor. A panic rather than an error, because
// graph.Vertex holds a func() core.Operator that cannot return one; deferring
// the check would divide by zero in every subtask at once.
func TestNewQ2RejectsAZeroDivisor(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("NewQ2(0) did not panic")
		}
	}()
	NewQ2(0)
}

// TestNexmarkStatelessQueriesAreAFunctionOfTheRecordAlone.
//
// Running the same stream twice through one operator gives the same answer, and
// running it through two operators gives the same answer as running it through
// one. Neither holds for an operator that accumulated anything, which is what
// "stateless" has to mean here: q0, q1 and q2 are checkpointed as zero bytes,
// so anything they did keep would silently vanish on a restore.
func TestNexmarkStatelessQueriesAreAFunctionOfTheRecordAlone(t *testing.T) {
	events := nexmarkEvents(t, nexmarkConfig(2, 2000))

	for _, q := range []struct {
		name string
		make func() core.Operator
	}{
		{"q0", func() core.Operator { return NewQ0() }},
		{"q1", func() core.Operator { return NewQ1(Q1Factor) }},
		{"q2", func() core.Operator { return NewQ2(Q2Divisor) }},
	} {
		t.Run(q.name, func(t *testing.T) {
			op := q.make()
			first := runQuery(t, op, events)
			// The SAME operator, a second time: an operator holding a counter
			// would answer differently on the second pass.
			ctx := &emitContext{}
			if err := op.Open(ctx); err != nil {
				t.Fatalf("Open: %v", err)
			}
			for _, rec := range events {
				if err := op.ProcessElement(rec, ctx); err != nil {
					t.Fatalf("ProcessElement: %v", err)
				}
			}
			second := ctx.emitted
			fresh := runQuery(t, q.make(), events)

			if len(first) != len(second) || len(first) != len(fresh) {
				t.Fatalf("%s emitted %d, then %d on a second pass, and %d on a fresh operator",
					q.name, len(first), len(second), len(fresh))
			}
			for i := range first {
				if !sameRecord(first[i], second[i]) || !sameRecord(first[i], fresh[i]) {
					t.Fatalf("%s record %d differs between passes", q.name, i)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// q7
// ---------------------------------------------------------------------------

// winner is what a fired q7 window looks like once decoded.
type winner struct {
	key         uint64
	windowStart int64
	price       uint64
	bidder      uint64
	auction     uint64
}

// maxBidHarness drives one q7 operator and decodes what it emits.
type maxBidHarness struct {
	t    *testing.T
	op   *MaxBid
	ctx  *emitContext
	seen int
}

func newMaxBidHarness(t *testing.T, op *MaxBid) *maxBidHarness {
	t.Helper()
	h := &maxBidHarness{t: t, op: op, ctx: &emitContext{}}
	if err := op.Open(h.ctx); err != nil {
		t.Fatalf("Open: %v", err)
	}
	return h
}

func (h *maxBidHarness) bid(auction, bidder, price uint64, at int64) {
	h.t.Helper()
	if err := h.op.ProcessElement(bidRecord(auction, bidder, price, at), h.ctx); err != nil {
		h.t.Fatalf("ProcessElement(auction %d, bidder %d, price %d, t %d): %v", auction, bidder, price, at, err)
	}
}

func (h *maxBidHarness) event(rec *core.Record) {
	h.t.Helper()
	if err := h.op.ProcessElement(rec, h.ctx); err != nil {
		h.t.Fatalf("ProcessElement: %v", err)
	}
}

// watermark delivers wm and returns the windows that fired in response.
func (h *maxBidHarness) watermark(wm int64) []winner {
	h.t.Helper()
	h.ctx.watermark = wm
	if err := h.op.ProcessWatermark(wm, h.ctx); err != nil {
		h.t.Fatalf("ProcessWatermark(%d): %v", wm, err)
	}
	return h.take()
}

// take decodes everything emitted since the last call.
//
// The window start is DERIVED, not read: the operator stamps a fired window
// with its end-1, so the start is EventTime-(size-1) and the harness undoes the
// same saturating arithmetic the operator applied. Reading EventTime as a start
// would shift every expectation in this file by size-1 and leave every row
// looking plausible.
func (h *maxBidHarness) take() []winner {
	h.t.Helper()
	var out []winner
	for _, r := range h.ctx.emitted[h.seen:] {
		w, err := DecodeWinningBid(r.Value)
		if err != nil {
			h.t.Fatalf("DecodeWinningBid: %v", err)
		}
		key, err := sources.NexmarkKeyID(r.Key)
		if err != nil {
			h.t.Fatalf("NexmarkKeyID: %v", err)
		}
		out = append(out, winner{
			key:         key,
			windowStart: subFloor(r.EventTime, h.op.size-1),
			price:       w.Price, bidder: w.Bidder, auction: w.Auction,
		})
	}
	h.seen = len(h.ctx.emitted)
	return out
}

// TestQ7EmitsTheMaximumPricePerWindow.
func TestQ7EmitsTheMaximumPricePerWindow(t *testing.T) {
	h := newMaxBidHarness(t, NewQ7(100, 0))
	h.bid(7, 1, 50, 10)
	h.bid(7, 2, 90, 20)
	h.bid(7, 3, 10, 30)
	h.bid(8, 1, 5, 40)
	// A second window for auction 7, so the assertion is per (key, window) and
	// not per key.
	h.bid(7, 4, 1, 120)

	got := h.watermark(99)
	want := []winner{
		{key: 7, windowStart: 0, price: 90, bidder: 2, auction: 7},
		{key: 8, windowStart: 0, price: 5, bidder: 1, auction: 8},
	}
	if !slices.Equal(got, want) {
		t.Fatalf("at watermark 99 the operator emitted %+v, want %+v", got, want)
	}

	got = h.watermark(199)
	want = []winner{{key: 7, windowStart: 100, price: 1, bidder: 4, auction: 7}}
	if !slices.Equal(got, want) {
		t.Fatalf("at watermark 199 the operator emitted %+v, want %+v", got, want)
	}
}

// TestQ7FiresAtEndMinusOneAndNotBefore.
//
// A watermark w asserts that nothing with event time <= w will arrive, so a
// window [start, end) is complete at w == end-1 and not before. Firing at end
// would hold every window one millisecond longer for nothing; firing at end-2
// would report a window a later record could still join.
func TestQ7FiresAtEndMinusOneAndNotBefore(t *testing.T) {
	h := newMaxBidHarness(t, NewQ7(100, 0))
	h.bid(7, 1, 50, 10)

	for _, wm := range []int64{0, 50, 98} {
		if got := h.watermark(wm); len(got) != 0 {
			t.Fatalf("watermark %d fired %+v; the window [0, 100) completes at 99", wm, got)
		}
	}
	if got := h.watermark(99); len(got) != 1 {
		t.Fatalf("watermark 99 fired %+v, want the one window", got)
	}
	// And the emitted event time IS end-1, not the start. A downstream
	// event-time stage would see the whole output as late otherwise.
	if got := h.ctx.emitted[0].EventTime; got != 99 {
		t.Errorf("the fired window carries event time %d, want 99 (its end-1)", got)
	}
}

// TestQ7BreaksTiesDeterministically.
//
// The two tiers this operator can reach: equal prices go to the lowest bidder,
// and that is asserted through the query rather than only through the
// comparator. The auction tier is unreachable here -- the auction is the
// grouping key -- and is tested directly in TestBetterBidIsATotalOrderOperator.
func TestQ7BreaksTiesDeterministically(t *testing.T) {
	tests := []struct {
		name string
		bids [][3]uint64 // auction, bidder, price
		want winner
	}{
		{
			name: "equal price goes to the lowest bidder",
			bids: [][3]uint64{{7, 5, 90}, {7, 2, 90}, {7, 9, 90}},
			want: winner{key: 7, windowStart: 0, price: 90, bidder: 2, auction: 7},
		},
		{
			name: "a higher price beats a lower bidder",
			bids: [][3]uint64{{7, 1, 90}, {7, 9, 91}},
			want: winner{key: 7, windowStart: 0, price: 91, bidder: 9, auction: 7},
		},
		{
			name: "the lowest bidder wins whatever order they arrive in",
			bids: [][3]uint64{{7, 2, 90}, {7, 5, 90}},
			want: winner{key: 7, windowStart: 0, price: 90, bidder: 2, auction: 7},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newMaxBidHarness(t, NewQ7(100, 0))
			for i, b := range tt.bids {
				h.bid(b[0], b[1], b[2], int64(10+i))
			}
			got := h.watermark(99)
			if !slices.Equal(got, []winner{tt.want}) {
				t.Fatalf("emitted %+v, want %+v", got, tt.want)
			}
		})
	}
}

// TestQ7IsIndependentOfArrivalOrder.
//
// Records reach an operator in whatever order the shuffle and the scheduler
// produced. A winner decided by "greater price, otherwise keep what is there"
// would depend on that order, so two runs of one job would commit different
// bids for one window and the oracle comparison would fail intermittently --
// looking exactly like a windowing bug.
//
// Every permutation of a set containing a two-way price tie, so the assertion
// is over the arrival orders rather than over a couple of them.
func TestQ7IsIndependentOfArrivalOrder(t *testing.T) {
	bids := [][3]uint64{{7, 5, 90}, {7, 2, 90}, {7, 8, 42}, {7, 1, 7}}

	var want []winner
	for _, perm := range permutations(len(bids)) {
		h := newMaxBidHarness(t, NewQ7(100, 0))
		for i, at := range perm {
			b := bids[at]
			h.bid(b[0], b[1], b[2], int64(10+i))
		}
		got := h.watermark(99)
		if want == nil {
			want = got
			continue
		}
		if !slices.Equal(got, want) {
			t.Fatalf("arrival order %v produced %+v, and another order produced %+v", perm, got, want)
		}
	}
	if !slices.Equal(want, []winner{{key: 7, windowStart: 0, price: 90, bidder: 2, auction: 7}}) {
		t.Fatalf("every order produced %+v, which is not the hand-computed winner", want)
	}
}

// permutations returns every permutation of [0, n).
func permutations(n int) [][]int {
	idx := make([]int, n)
	for i := range idx {
		idx[i] = i
	}
	var out [][]int
	var rec func(k int)
	rec = func(k int) {
		if k == n {
			out = append(out, slices.Clone(idx))
			return
		}
		for i := k; i < n; i++ {
			idx[k], idx[i] = idx[i], idx[k]
			rec(k + 1)
			idx[k], idx[i] = idx[i], idx[k]
		}
	}
	rec(0)
	return out
}

// TestBetterBidIsATotalOrderOperator exercises the tier q7 cannot reach.
//
// The auction tier is unreachable inside the operator, where the auction is the
// grouping key. Testing the rule only through the query would leave a third of
// it unexercised while looking covered.
func TestBetterBidIsATotalOrderOperator(t *testing.T) {
	for p1 := uint64(0); p1 < 3; p1++ {
		for b1 := uint64(0); b1 < 3; b1++ {
			for a1 := uint64(0); a1 < 3; a1++ {
				for p2 := uint64(0); p2 < 3; p2++ {
					for b2 := uint64(0); b2 < 3; b2++ {
						for a2 := uint64(0); a2 < 3; a2++ {
							x := WinningBid{Price: p1, Bidder: b1, Auction: a1}
							y := WinningBid{Price: p2, Bidder: b2, Auction: a2}
							ab, ba := betterBid(x, y), betterBid(y, x)
							if x == y && (ab || ba) {
								t.Fatalf("%+v beats itself", x)
							}
							if x != y && ab == ba {
								t.Fatalf("%+v and %+v are both %v", x, y, ab)
							}
						}
					}
				}
			}
		}
	}
	// The three tiers, one row each, so a comparator that had lost a tier
	// fails here rather than only in the antisymmetry sweep.
	if !betterBid(WinningBid{Price: 2}, WinningBid{Price: 1}) {
		t.Error("a higher price does not win")
	}
	if !betterBid(WinningBid{Price: 1, Bidder: 1}, WinningBid{Price: 1, Bidder: 2}) {
		t.Error("an equal price does not go to the lower bidder")
	}
	if !betterBid(WinningBid{Price: 1, Bidder: 1, Auction: 1}, WinningBid{Price: 1, Bidder: 1, Auction: 2}) {
		t.Error("an equal price and bidder do not go to the lower auction")
	}
}

// TestQ7DropsEventsThatAreNotBids.
//
// q7 is defined over the bid stream. A person or an auction reaching the
// window operator would open a window keyed on a person id, which is a row the
// oracle does not have and a key nothing will ever bid on.
func TestQ7DropsEventsThatAreNotBids(t *testing.T) {
	h := newMaxBidHarness(t, NewQ7(100, 0))
	h.event(personRecord(1, 10))
	h.event(auctionRecord(2, 20))
	if got := len(partitionKeys(h.ctx.State(), state.PrefixUserState)); got != 0 {
		t.Fatalf("%d windows opened for events that are not bids, want none", got)
	}
	if got := h.watermark(99); len(got) != 0 {
		t.Fatalf("a person and an auction fired %+v", got)
	}
	// A bid on the same ids still opens a window, so the assertion above is
	// about the types and not about the operator ignoring everything.
	h.bid(1, 1, 5, 10)
	if got := h.watermark(199); len(got) != 1 {
		t.Fatalf("a bid fired %+v, want one window", got)
	}
}

// TestQ7DropsAndCountsABidAfterPurge.
//
// A window past its allowed lateness has been fired and reported. A record
// arriving for it must be dropped rather than reopening it: reopening emits a
// (key, window) the sink already holds, and at a transactional sink that is a
// second committed row for one window.
func TestQ7DropsAndCountsABidAfterPurge(t *testing.T) {
	h := newMaxBidHarness(t, NewQ7(100, 0))
	h.bid(7, 1, 50, 10)
	if got := h.watermark(250); len(got) != 1 {
		t.Fatalf("watermark 250 fired %+v, want the one window", got)
	}
	if got := len(partitionKeys(h.ctx.State(), state.PrefixUserState)); got != 0 {
		t.Fatalf("%d aggregates survived the purge, want none", got)
	}

	before := h.op.OnTime()
	h.bid(7, 9, 900, 10)
	if got := h.op.Dropped(); got != 1 {
		t.Errorf("the operator dropped %d bids, want 1", got)
	}
	if got := h.op.OnTime(); got != before {
		t.Errorf("a dropped bid was counted on time")
	}
	if got := len(partitionKeys(h.ctx.State(), state.PrefixUserState)); got != 0 {
		t.Errorf("a late bid opened %d windows, want none", got)
	}
	if got := h.watermark(math.MaxInt64); len(got) != 0 {
		t.Errorf("the flush emitted %+v for a window the sink already holds", got)
	}
}

// TestQ7StateLayout pins the three partitions and the value payload.
//
// The expected bytes are built here by hand rather than by calling the
// encoders, which would be the operator agreeing with itself. The value is the
// one thing this operator changes about WindowCount's layout, so it is the one
// thing that most needs pinning.
func TestQ7StateLayout(t *testing.T) {
	h := newMaxBidHarness(t, NewQ7(100, 0))
	h.bid(7, 3, 90, 10)
	h.ctx.watermark = 0
	if err := h.op.ProcessWatermark(0, h.ctx); err != nil {
		t.Fatalf("ProcessWatermark: %v", err)
	}

	st := h.ctx.State()

	// The aggregate: 0x00 || key || windowStart big-endian.
	wantKey := append([]byte{state.PrefixUserState}, sources.NexmarkKey(7)...)
	var start [8]byte
	binary.BigEndian.PutUint64(start[:], 0)
	wantKey = append(wantKey, start[:]...)

	value, ok := st.Get(wantKey)
	if !ok {
		t.Fatalf("no aggregate stored under %x", wantKey)
	}
	// price 90, bidder 3, auction 7, each big-endian, in that order.
	wantValue := []byte{
		0, 0, 0, 0, 0, 0, 0, 90,
		0, 0, 0, 0, 0, 0, 0, 3,
		0, 0, 0, 0, 0, 0, 0, 7,
	}
	if !bytes.Equal(value, wantValue) {
		t.Errorf("the aggregate value is %x, want %x", value, wantValue)
	}
	if len(value) != winningBidBytes {
		t.Errorf("the aggregate value is %d bytes, want %d", len(value), winningBidBytes)
	}

	// The timer: 0x01 || fireTime ordered || key || windowStart big-endian.
	wantTimer := []byte{state.PrefixTimer}
	fire := state.EncodeOrderedInt64(99)
	wantTimer = append(wantTimer, fire[:]...)
	wantTimer = append(wantTimer, sources.NexmarkKey(7)...)
	wantTimer = append(wantTimer, start[:]...)
	if _, ok := st.Get(wantTimer); !ok {
		t.Errorf("no timer stored under %x; the partition holds %x", wantTimer, partitionKeys(st, state.PrefixTimer))
	}

	// The watermark: 0x02 || "watermark".
	wantWatermark := append([]byte{state.PrefixOperatorState}, "watermark"...)
	stored, ok := st.Get(wantWatermark)
	if !ok {
		t.Fatalf("no watermark stored under %x", wantWatermark)
	}
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], uint64(0)^(1<<63))
	if !bytes.Equal(stored, encoded[:]) {
		t.Errorf("the watermark is stored as %x, want %x", stored, encoded)
	}

	// Nothing outside the three partitions.
	st.Iterate(func(k, v []byte) bool {
		switch k[0] {
		case state.PrefixUserState, state.PrefixTimer, state.PrefixOperatorState:
		default:
			t.Errorf("the operator wrote a key under discriminator %#x: %x", k[0], k)
		}
		return true
	})
}

// TestWinningBidRoundTrips, and refuses anything that is not exactly 24 bytes.
func TestWinningBidRoundTrips(t *testing.T) {
	for _, w := range []WinningBid{
		{},
		{Price: 1, Bidder: 2, Auction: 3},
		{Price: math.MaxUint64, Bidder: math.MaxUint64, Auction: math.MaxUint64},
	} {
		got, err := DecodeWinningBid(EncodeWinningBid(w))
		if err != nil || got != w {
			t.Errorf("%+v round-tripped to (%+v, %v)", w, got, err)
		}
	}
	for _, bad := range [][]byte{nil, make([]byte, 23), make([]byte, 25), encodeCount(5)} {
		if _, err := DecodeWinningBid(bad); err == nil {
			t.Errorf("a %d-byte value was accepted as a winning bid", len(bad))
		}
	}
}

// TestRestoredQ7RecoversItsWatermark is the divergence the state-backed
// watermark closes, asserted against the run that does not recover it.
//
// The script purges a window, then restores. A bid for that window arriving
// after the restore is LATE and must be dropped: the window has been fired and
// reported, and accepting the bid emits a (key, window) the sink already holds.
// An operator whose watermark came back as minWatermark thinks nothing has been
// purged, so it accepts the bid, opens the window a second time and emits it
// again.
//
// The counterfactual is in the test rather than in a comment: the same bid is
// fed to an operator opened on EMPTY state, and the assertion is that the two
// disagree. Without it this would pass against an operator that dropped the bid
// for some unrelated reason.
func TestRestoredQ7RecoversItsWatermark(t *testing.T) {
	h := newMaxBidHarness(t, NewQ7(100, 0))
	h.bid(7, 1, 50, 10)
	if got := h.watermark(250); len(got) != 1 {
		t.Fatalf("the run before the checkpoint fired %+v, want the one window", got)
	}

	restored := restoreMaxBid(t, h, NewQ7(100, 0))
	if got, err := loadWatermark(restored.ctx.State()); err != nil || got != 250 {
		t.Fatalf("the restored operator's watermark is (%d, %v), want (250, nil)", got, err)
	}

	restored.bid(7, 9, 900, 10)
	if got := restored.op.Dropped(); got != 1 {
		t.Errorf("the restored operator dropped %d bids, want 1", got)
	}
	if got := restored.watermark(math.MaxInt64); len(got) != 0 {
		t.Errorf("the restored operator emitted %+v for a window the sink already holds", got)
	}

	// The same bid against an operator that did not recover a watermark. This
	// is what a Go field produces: the window comes back, with a different
	// winner, and the sink ends up with two rows for one (key, window).
	fresh := newMaxBidHarness(t, NewQ7(100, 0))
	fresh.bid(7, 9, 900, 10)
	if got := fresh.op.Dropped(); got != 0 {
		t.Fatalf("an operator on empty state dropped %d, want 0: the counterfactual does not hold", got)
	}
	if got := fresh.watermark(math.MaxInt64); len(got) != 1 {
		t.Fatalf("an operator on empty state emitted %+v, want the duplicate window: the counterfactual does not hold", got)
	}
}

// restoreMaxBid serialises a harness's state, reads it back into fresh state,
// and opens op on it. It is the restore path the runtime takes, in miniature.
func restoreMaxBid(t *testing.T, h *maxBidHarness, op *MaxBid) *maxBidHarness {
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
	return &maxBidHarness{t: t, op: op, ctx: ctx}
}

// TestNewQ7RejectsABadSpecification.
func TestNewQ7RejectsABadSpecification(t *testing.T) {
	for _, tt := range []struct {
		name           string
		size, lateness int64
	}{
		{"zero size", 0, 0},
		{"negative size", -1, 0},
		{"negative lateness", 100, -1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Errorf("NewQ7(%d, %d) did not panic", tt.size, tt.lateness)
				}
			}()
			NewQ7(tt.size, tt.lateness)
		})
	}
}

// TestQ7OpenRefusesWithoutState. An operator quietly running on state the
// runtime does not know about would checkpoint as empty and restore as empty.
func TestQ7OpenRefusesWithoutState(t *testing.T) {
	if err := NewQ7(100, 0).Open(&nilStateContext{}); err == nil {
		t.Error("q7 opened with no keyed state")
	}
}

// TestQ7SnapshotRefuses. Its state is the subtask's KeyedState, which
// pkg/checkpoint serialises directly; a zero-byte snapshot would be a claim
// that there is nothing to keep.
func TestQ7SnapshotRefuses(t *testing.T) {
	op := NewQ7(100, 0)
	if err := op.Snapshot(io.Discard); err == nil {
		t.Error("Snapshot wrote a snapshot")
	}
	if err := op.Restore(strings.NewReader("")); err == nil {
		t.Error("Restore accepted a snapshot")
	}
}
