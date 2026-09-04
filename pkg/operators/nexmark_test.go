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

// ---------------------------------------------------------------------------
// q5
// ---------------------------------------------------------------------------

// hotRow is what a fired q5 window looks like once decoded.
type hotRow struct {
	windowStart int64
	auction     uint64
	count       int64
}

// hotHarness drives q5's SECOND STAGE on its own, against stage-1 output
// written out by hand.
//
// In isolation first, and the pipeline only afterwards, because a two-stage
// failure is hard to localise: an empty result says one of the two stages is
// wrong and nothing about which. Every property of stage 2 that can be stated
// without stage 1 is stated here.
type hotHarness struct {
	t    *testing.T
	op   *HotItems
	ctx  *emitContext
	seen int
}

func newHotHarness(t *testing.T, op *HotItems) *hotHarness {
	t.Helper()
	h := &hotHarness{t: t, op: op, ctx: &emitContext{}}
	if err := op.Open(h.ctx); err != nil {
		t.Fatalf("Open: %v", err)
	}
	return h
}

// stage1Record builds one stage-1 output record by hand: keyed on the window,
// carrying (auction, count), stamped with the window's end-1.
func stage1Record(windowStart, size int64, auction uint64, count int64) *core.Record {
	return &core.Record{
		Key:       WindowKey(windowStart),
		Value:     EncodeAuctionCount(AuctionCount{Auction: auction, Count: count}),
		EventTime: windowStart + size - 1,
	}
}

func (h *hotHarness) count(windowStart int64, auction uint64, count int64) {
	h.t.Helper()
	rec := stage1Record(windowStart, h.op.size, auction, count)
	if err := h.op.ProcessElement(rec, h.ctx); err != nil {
		h.t.Fatalf("ProcessElement(window %d, auction %d, count %d): %v", windowStart, auction, count, err)
	}
}

func (h *hotHarness) watermark(wm int64) []hotRow {
	h.t.Helper()
	h.ctx.watermark = wm
	if err := h.op.ProcessWatermark(wm, h.ctx); err != nil {
		h.t.Fatalf("ProcessWatermark(%d): %v", wm, err)
	}
	return h.take()
}

// take decodes everything emitted since the last call, deriving the window
// start from the emitted end-1 rather than reading it.
func (h *hotHarness) take() []hotRow {
	h.t.Helper()
	var out []hotRow
	for _, r := range h.ctx.emitted[h.seen:] {
		auction, err := sources.NexmarkKeyID(r.Key)
		if err != nil {
			h.t.Fatalf("NexmarkKeyID: %v", err)
		}
		count, err := DecodeCount(r.Value)
		if err != nil {
			h.t.Fatalf("DecodeCount: %v", err)
		}
		out = append(out, hotRow{
			windowStart: subFloor(r.EventTime, h.op.size-1), auction: auction, count: count,
		})
	}
	h.seen = len(h.ctx.emitted)
	return out
}

// TestQ5Stage2SelectsTheMaximumPerWindow, in isolation.
func TestQ5Stage2SelectsTheMaximumPerWindow(t *testing.T) {
	h := newHotHarness(t, NewQ5HotItems(100, 0))
	h.count(0, 7, 3)
	h.count(0, 8, 5)
	h.count(0, 9, 1)
	// A second window, so the maximum is per window and not global.
	h.count(100, 7, 2)
	h.count(100, 8, 1)

	got := h.watermark(99)
	want := []hotRow{{windowStart: 0, auction: 8, count: 5}}
	if !slices.Equal(got, want) {
		t.Fatalf("watermark 99 emitted %+v, want %+v", got, want)
	}

	got = h.watermark(199)
	want = []hotRow{{windowStart: 100, auction: 7, count: 2}}
	if !slices.Equal(got, want) {
		t.Fatalf("watermark 199 emitted %+v, want %+v", got, want)
	}
}

// TestQ5Stage2EmitsEveryAuctionAtTheMaximumInAuctionOrder.
//
// A tie is several rows, which is Nexmark's "num >= ALL", and the rows come out
// in ascending auction order. That order is not sorted anywhere: the aggregate
// key is (window, auction) and the scan runs in ascending byte order, so it
// falls out of the layout. It is asserted because it is invisible as code -- and
// because the emission order of ONE operator call is deterministic by
// construction, unlike the order of a whole job's sink, which is not.
//
// The counts are fed in DESCENDING auction order so that a stage emitting in
// arrival order would produce the reverse and fail here.
func TestQ5Stage2EmitsEveryAuctionAtTheMaximumInAuctionOrder(t *testing.T) {
	h := newHotHarness(t, NewQ5HotItems(100, 0))
	for _, auction := range []uint64{9, 7, 4, 2} {
		h.count(0, auction, 5)
	}
	h.count(0, 11, 4) // below the maximum, so not a winner

	got := h.watermark(99)
	want := []hotRow{
		{windowStart: 0, auction: 2, count: 5},
		{windowStart: 0, auction: 4, count: 5},
		{windowStart: 0, auction: 7, count: 5},
		{windowStart: 0, auction: 9, count: 5},
	}
	if !slices.Equal(got, want) {
		t.Fatalf("emitted %+v, want %+v", got, want)
	}
}

// TestQ5Stage2FiresAtEndMinusOneAndStampsItsOutputThere.
func TestQ5Stage2FiresAtEndMinusOneAndStampsItsOutputThere(t *testing.T) {
	h := newHotHarness(t, NewQ5HotItems(100, 0))
	h.count(0, 7, 1)

	for _, wm := range []int64{0, 50, 98} {
		if got := h.watermark(wm); len(got) != 0 {
			t.Fatalf("watermark %d fired %+v; the window [0, 100) completes at 99", wm, got)
		}
	}
	if got := h.watermark(99); len(got) != 1 {
		t.Fatalf("watermark 99 fired %+v, want the one window", got)
	}
	if got := h.ctx.emitted[0].EventTime; got != 99 {
		t.Errorf("the fired window carries event time %d, want 99 (its end-1)", got)
	}
}

// TestQ5Stage2ReplacesACountRatherThanAccumulatingIt.
//
// Stage 1 emits one record per (auction, window) when that window fires, and
// re-fires the window with an UPDATED count if a further record reaches it
// before the purge. Stage 2 must therefore replace, not add: adding would
// double the count of every window stage 1 re-fired, which happens whenever a
// record arrives between a window firing and its purge.
func TestQ5Stage2ReplacesACountRatherThanAccumulatingIt(t *testing.T) {
	h := newHotHarness(t, NewQ5HotItems(100, 0))
	h.count(0, 7, 3)
	h.count(0, 7, 4) // the re-fire: four, not seven
	h.count(0, 8, 5)

	got := h.watermark(99)
	want := []hotRow{{windowStart: 0, auction: 8, count: 5}}
	if !slices.Equal(got, want) {
		t.Fatalf("emitted %+v, want %+v: a stage that accumulated would make auction 7 the winner at seven", got, want)
	}
}

// TestQ5Stage2RejectsAKeyAndEventTimeThatDisagree.
//
// The key says which window and the event time says when it completes; they are
// the same window twice. A re-key built with a different size than this stage
// produces records where they disagree, and without this check the result is
// every window shifted by a constant with every row still looking plausible.
func TestQ5Stage2RejectsAKeyAndEventTimeThatDisagree(t *testing.T) {
	h := newHotHarness(t, NewQ5HotItems(100, 0))
	bad := &core.Record{
		Key:       WindowKey(0),
		Value:     EncodeAuctionCount(AuctionCount{Auction: 7, Count: 1}),
		EventTime: 199, // the end-1 of a window of size 200
	}
	if err := h.op.ProcessElement(bad, h.ctx); err == nil {
		t.Error("stage 2 accepted a record whose key and event time describe different windows")
	}

	// A malformed key and a malformed value are refused too, rather than being
	// read as some other window or some other count.
	for _, rec := range []*core.Record{
		{Key: []byte{1, 2, 3}, Value: EncodeAuctionCount(AuctionCount{Auction: 7, Count: 1}), EventTime: 99},
		{Key: WindowKey(0), Value: []byte{1, 2, 3}, EventTime: 99},
		{Key: WindowKey(0), Value: encodeCount(1), EventTime: 99},
	} {
		if err := h.op.ProcessElement(rec, h.ctx); err == nil {
			t.Errorf("stage 2 accepted a record with key %x and value %x", rec.Key, rec.Value)
		}
	}
}

// TestQ5Stage2DropsAndCountsALateRecord.
func TestQ5Stage2DropsAndCountsALateRecord(t *testing.T) {
	h := newHotHarness(t, NewQ5HotItems(100, 0))
	h.count(0, 7, 3)
	if got := h.watermark(250); len(got) != 1 {
		t.Fatalf("watermark 250 fired %+v, want the one window", got)
	}
	if got := len(partitionKeys(h.ctx.State(), state.PrefixUserState)); got != 0 {
		t.Fatalf("%d aggregates survived the purge, want none", got)
	}

	onTime := h.op.OnTime()
	h.count(0, 9, 99)
	if got := h.op.Dropped(); got != 1 {
		t.Errorf("stage 2 dropped %d records, want 1", got)
	}
	if got := h.op.OnTime(); got != onTime {
		t.Error("a dropped record was counted on time")
	}
	if got := len(partitionKeys(h.ctx.State(), state.PrefixUserState)); got != 0 {
		t.Errorf("a late record opened %d windows, want none", got)
	}
	if got := h.watermark(math.MaxInt64); len(got) != 0 {
		t.Errorf("the flush emitted %+v for a window the sink already holds", got)
	}
}

// TestQ5Stage2StateLayout pins the three partitions, by hand.
func TestQ5Stage2StateLayout(t *testing.T) {
	h := newHotHarness(t, NewQ5HotItems(100, 0))
	h.count(0, 7, 3)
	h.ctx.watermark = 0
	if err := h.op.ProcessWatermark(0, h.ctx); err != nil {
		t.Fatalf("ProcessWatermark: %v", err)
	}
	st := h.ctx.State()

	// The aggregate: 0x00 || windowKey || auction big-endian.
	wantKey := []byte{state.PrefixUserState}
	wantKey = append(wantKey, WindowKey(0)...)
	wantKey = append(wantKey, 0, 0, 0, 0, 0, 0, 0, 7)
	value, ok := st.Get(wantKey)
	if !ok {
		t.Fatalf("no aggregate under %x; the partition holds %x", wantKey, partitionKeys(st, state.PrefixUserState))
	}
	if want := []byte{0, 0, 0, 0, 0, 0, 0, 3}; !bytes.Equal(value, want) {
		t.Errorf("the aggregate value is %x, want %x", value, want)
	}

	// The timer: 0x01 || fireTime ordered || windowKey || windowStart
	// big-endian. The last two are the same number written twice, which keeps
	// one timer layout in this package; see the note on HotItems.
	wantTimer := []byte{state.PrefixTimer}
	fire := state.EncodeOrderedInt64(99)
	wantTimer = append(wantTimer, fire[:]...)
	wantTimer = append(wantTimer, WindowKey(0)...)
	wantTimer = append(wantTimer, WindowKey(0)...)
	if _, ok := st.Get(wantTimer); !ok {
		t.Errorf("no timer under %x; the partition holds %x", wantTimer, partitionKeys(st, state.PrefixTimer))
	}

	// The watermark: 0x02 || "watermark".
	wantWatermark := append([]byte{state.PrefixOperatorState}, "watermark"...)
	stored, ok := st.Get(wantWatermark)
	if !ok {
		t.Fatalf("no watermark under %x", wantWatermark)
	}
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], uint64(0)^(1<<63))
	if !bytes.Equal(stored, encoded[:]) {
		t.Errorf("the watermark is stored as %x, want %x", stored, encoded)
	}

	st.Iterate(func(k, v []byte) bool {
		switch k[0] {
		case state.PrefixUserState, state.PrefixTimer, state.PrefixOperatorState:
		default:
			t.Errorf("stage 2 wrote a key under discriminator %#x: %x", k[0], k)
		}
		return true
	})
}

// TestQ5Stage2GroupsAWindowsAuctionsTogether.
//
// The emission order in ascending auction rests on one window's counts being a
// contiguous run with the auctions ascending. That is a property of the key
// layout rather than of any code, so it is asserted against the key space
// directly: interleaved runs would still produce the right winners and the
// wrong order.
func TestQ5Stage2GroupsAWindowsAuctionsTogether(t *testing.T) {
	h := newHotHarness(t, NewQ5HotItems(100, 0))
	for _, w := range []int64{200, 0, 100} {
		for _, a := range []uint64{9, 2, 5} {
			h.count(w, a, 1)
		}
	}

	type pair struct {
		window  int64
		auction uint64
	}
	var seen []pair
	for _, k := range partitionKeys(h.ctx.State(), state.PrefixUserState) {
		windowKey, auction, err := parseHotItemKey(k)
		if err != nil {
			t.Fatalf("parseHotItemKey: %v", err)
		}
		start, err := WindowKeyStart(windowKey)
		if err != nil {
			t.Fatalf("WindowKeyStart: %v", err)
		}
		seen = append(seen, pair{start, auction})
	}
	want := []pair{
		{0, 2}, {0, 5}, {0, 9},
		{100, 2}, {100, 5}, {100, 9},
		{200, 2}, {200, 5}, {200, 9},
	}
	if !slices.Equal(seen, want) {
		t.Errorf("the aggregate partition iterates as %+v, want %+v", seen, want)
	}
}

// TestRestoredQ5Stage2RecoversItsWatermark is THE trap of this step, asserted.
//
// core.Context.CurrentWatermark() is not restored across a checkpoint. An
// operator with a lateness rule that read it would come back believing nothing
// had been purged, accept a record it should drop, and emit a (window, auction)
// the sink already holds.
//
// The counterfactual is in the test rather than in a comment: the same record
// is fed to a stage opened on EMPTY state, and the assertion is that the two
// disagree. Without it this would pass against a stage that dropped the record
// for some unrelated reason.
//
// This is the test the step 6 mutation check is aimed at.
func TestRestoredQ5Stage2RecoversItsWatermark(t *testing.T) {
	h := newHotHarness(t, NewQ5HotItems(100, 0))
	h.count(0, 7, 3)
	if got := h.watermark(250); len(got) != 1 {
		t.Fatalf("the run before the checkpoint fired %+v, want the one window", got)
	}

	restored := restoreHotItems(t, h, NewQ5HotItems(100, 0))
	if got, err := loadWatermark(restored.ctx.State()); err != nil || got != 250 {
		t.Fatalf("the restored stage's watermark is (%d, %v), want (250, nil)", got, err)
	}

	restored.count(0, 9, 99)
	if got := restored.op.Dropped(); got != 1 {
		t.Errorf("the restored stage dropped %d records, want 1", got)
	}
	if got := restored.watermark(math.MaxInt64); len(got) != 0 {
		t.Errorf("the restored stage emitted %+v for a window the sink already holds", got)
	}

	// The same record against a stage that did not recover a watermark. This is
	// what reading it off the context produces: the window comes back, with a
	// different winner, and the sink ends up with two rows for one window.
	fresh := newHotHarness(t, NewQ5HotItems(100, 0))
	fresh.count(0, 9, 99)
	if got := fresh.op.Dropped(); got != 0 {
		t.Fatalf("a stage on empty state dropped %d, want 0: the counterfactual does not hold", got)
	}
	if got := fresh.watermark(math.MaxInt64); len(got) != 1 {
		t.Fatalf("a stage on empty state emitted %+v, want the duplicate window: the counterfactual does not hold", got)
	}
}

func restoreHotItems(t *testing.T, h *hotHarness, op *HotItems) *hotHarness {
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
	return &hotHarness{t: t, op: op, ctx: ctx}
}

// TestQ5RekeyMovesTheWindowIntoTheKeyAndTheAuctionIntoTheValue.
func TestQ5RekeyMovesTheWindowIntoTheKeyAndTheAuctionIntoTheValue(t *testing.T) {
	const size = 1000
	rekey := NewQ5Rekey(size)

	// A stage-1 output record: keyed on the auction, carrying the count,
	// stamped with the window's end-1.
	in := &core.Record{Key: sources.NexmarkKey(42), Value: encodeCount(7), EventTime: 4999}
	out := runQuery(t, rekey, []*core.Record{in})
	if len(out) != 1 {
		t.Fatalf("the re-key emitted %d records for one, want 1", len(out))
	}

	start, err := WindowKeyStart(out[0].Key)
	if err != nil {
		t.Fatalf("WindowKeyStart: %v", err)
	}
	if start != 4000 {
		t.Errorf("the re-key produced window start %d, want 4000 (4999+1-1000)", start)
	}
	ac, err := DecodeAuctionCount(out[0].Value)
	if err != nil {
		t.Fatalf("DecodeAuctionCount: %v", err)
	}
	if ac.Auction != 42 || ac.Count != 7 {
		t.Errorf("the re-key produced %+v, want auction 42 count 7", ac)
	}
	if out[0].EventTime != in.EventTime {
		t.Errorf("the re-key changed the event time to %d, want %d: stage 2 relies on it still being the window's end-1",
			out[0].EventTime, in.EventTime)
	}

	// A malformed key or value is reported rather than read as some other
	// auction or count.
	for _, bad := range []*core.Record{
		{Key: []byte{1, 2, 3}, Value: encodeCount(1), EventTime: 4999},
		{Key: sources.NexmarkKey(1), Value: []byte{1, 2}, EventTime: 4999},
	} {
		ctx := &emitContext{}
		if err := rekey.ProcessElement(bad, ctx); err == nil {
			t.Errorf("the re-key accepted key %x value %x", bad.Key, bad.Value)
		}
	}
}

// TestBidsOnlyKeepsTheBids. Stage 1 counts records without looking at them, so
// this vertex is what stops a person event from opening a window keyed on a
// person id.
func TestBidsOnlyKeepsTheBids(t *testing.T) {
	in := mixedEvents()
	got := runQuery(t, NewBidsOnly(), in)
	if len(got) != 5 {
		t.Fatalf("bidsOnly emitted %d records, want the 5 bids", len(got))
	}
	for i, rec := range got {
		typ, err := sources.NexmarkTypeOf(rec.Value)
		if err != nil {
			t.Fatalf("NexmarkTypeOf: %v", err)
		}
		if typ != sources.EventBid {
			t.Errorf("record %d is a %s", i, typ)
		}
	}
	ctx := &emitContext{}
	if err := NewBidsOnly().ProcessElement(&core.Record{Key: sources.NexmarkKey(1), Value: []byte{0x7F}}, ctx); err == nil {
		t.Error("bidsOnly accepted a value with an unknown discriminator")
	}
}

// TestNewQ5RejectsABadSpecification.
//
// The sliding size must be a whole multiple of the slide, and that is rejected
// at construction. Stage 1 IS operators.NewSlidingCount, so that check is
// already written; this says q5 inherits it rather than working around it, and
// covers stage 2's and the re-key's own guards.
func TestNewQ5RejectsABadSpecification(t *testing.T) {
	mustPanic := func(name string, f func()) {
		t.Helper()
		defer func() {
			if recover() == nil {
				t.Errorf("%s did not panic", name)
			}
		}()
		f()
	}
	mustPanic("stage 1 with a size that is not a multiple of the slide", func() { NewSlidingCount(1000, 300, 0) })
	mustPanic("stage 1 with a zero slide", func() { NewSlidingCount(1000, 0, 0) })
	mustPanic("the re-key with a zero size", func() { NewQ5Rekey(0) })
	mustPanic("stage 2 with a zero size", func() { NewQ5HotItems(0, 0) })
	mustPanic("stage 2 with a negative lateness", func() { NewQ5HotItems(100, -1) })

	// The valid one, so the assertions above are about the specifications and
	// not about the constructors panicking always.
	NewSlidingCount(1000, 250, 0)
	NewQ5Rekey(1000)
	NewQ5HotItems(1000, 0)
}

// TestQ5Stage2OpenRefusesWithoutStateAndSnapshotRefuses.
func TestQ5Stage2OpenRefusesWithoutStateAndSnapshotRefuses(t *testing.T) {
	if err := NewQ5HotItems(100, 0).Open(&nilStateContext{}); err == nil {
		t.Error("stage 2 opened with no keyed state")
	}
	op := NewQ5HotItems(100, 0)
	if err := op.Snapshot(io.Discard); err == nil {
		t.Error("Snapshot wrote a snapshot")
	}
	if err := op.Restore(strings.NewReader("")); err == nil {
		t.Error("Restore accepted a snapshot")
	}
}

// TestAuctionCountRoundTrips, refusing anything that is not exactly sixteen
// bytes.
func TestAuctionCountRoundTrips(t *testing.T) {
	for _, ac := range []AuctionCount{
		{}, {Auction: 1, Count: 2}, {Auction: math.MaxUint64, Count: math.MaxInt64},
	} {
		got, err := DecodeAuctionCount(EncodeAuctionCount(ac))
		if err != nil || got != ac {
			t.Errorf("%+v round-tripped to (%+v, %v)", ac, got, err)
		}
	}
	for _, bad := range [][]byte{nil, make([]byte, 15), make([]byte, 17), encodeCount(1)} {
		if _, err := DecodeAuctionCount(bad); err == nil {
			t.Errorf("a %d-byte value was accepted as an auction count", len(bad))
		}
	}
	for _, start := range []int64{0, -1, math.MinInt64, math.MaxInt64, 1700000000000} {
		got, err := WindowKeyStart(WindowKey(start))
		if err != nil || got != start {
			t.Errorf("window start %d round-tripped to (%d, %v)", start, got, err)
		}
	}
	if _, err := WindowKeyStart([]byte{1}); err == nil {
		t.Error("a one-byte window key was accepted")
	}
}

// ---------------------------------------------------------------------------
// The two stages together, driven by hand.
// ---------------------------------------------------------------------------

// q5Pipeline runs stage 1, the re-key and stage 2 in the order the runtime
// runs them: an operator emits during ProcessWatermark, and the runtime
// forwards the watermark only AFTER that call returns, so everything a stage
// emits reaches the next stage ahead of the watermark that released it.
//
// Driven by hand rather than through pkg/runtime, so that a failure names a
// stage. The engine end to end, at parallelism, against the batch oracle, is
// the equivalence suite's job.
type q5Pipeline struct {
	t      *testing.T
	size   int64
	stage1 *WindowCount
	ctx1   *emitContext
	rekey  *Map
	ctx2   *emitContext
	stage2 *HotItems
	ctx3   *emitContext

	// stampAtWindowStart is the counterfactual: stage 1 emitting a fired
	// window at its START rather than its end-1, which is what it did before
	// Phase 3c. See TestQ5Stage2ReceivesOnTimeRecords.
	stampAtWindowStart bool

	seen1, seen2, seen3 int
}

func newQ5Pipeline(t *testing.T, size, slide, lateness int64) *q5Pipeline {
	t.Helper()
	p := &q5Pipeline{
		t: t, size: size,
		stage1: NewSlidingCount(size, slide, lateness), ctx1: &emitContext{},
		rekey: NewQ5Rekey(size), ctx2: &emitContext{},
		stage2: NewQ5HotItems(size, lateness), ctx3: &emitContext{},
	}
	for _, o := range []struct {
		op  core.Operator
		ctx core.Context
	}{{p.stage1, p.ctx1}, {p.rekey, p.ctx2}, {p.stage2, p.ctx3}} {
		if err := o.op.Open(o.ctx); err != nil {
			t.Fatalf("Open: %v", err)
		}
	}
	return p
}

// bid feeds one bid into stage 1, through the bids-only filter that sits in
// front of it in the job.
func (p *q5Pipeline) bid(auction uint64, at int64) {
	p.t.Helper()
	rec := bidRecord(auction, 1, 1, at)
	filtered := runQuery(p.t, NewBidsOnly(), []*core.Record{rec})
	if len(filtered) != 1 {
		p.t.Fatalf("the bids-only filter dropped a bid")
	}
	if err := p.stage1.ProcessElement(filtered[0], p.ctx1); err != nil {
		p.t.Fatalf("stage 1 ProcessElement: %v", err)
	}
}

// watermark delivers wm through all three stages in the runtime's order and
// returns what stage 2 emitted.
func (p *q5Pipeline) watermark(wm int64) []hotRow {
	p.t.Helper()

	p.ctx1.watermark = wm
	if err := p.stage1.ProcessWatermark(wm, p.ctx1); err != nil {
		p.t.Fatalf("stage 1 ProcessWatermark(%d): %v", wm, err)
	}
	for _, rec := range p.ctx1.emitted[p.seen1:] {
		if p.stampAtWindowStart {
			rec = &core.Record{
				Key: rec.Key, Value: rec.Value,
				EventTime: subFloor(rec.EventTime, p.size-1),
			}
		}
		if err := p.rekey.ProcessElement(rec, p.ctx2); err != nil {
			p.t.Fatalf("the re-key: %v", err)
		}
	}
	p.seen1 = len(p.ctx1.emitted)

	for _, rec := range p.ctx2.emitted[p.seen2:] {
		if err := p.stage2.ProcessElement(rec, p.ctx3); err != nil {
			p.t.Fatalf("stage 2 ProcessElement: %v", err)
		}
	}
	p.seen2 = len(p.ctx2.emitted)

	p.ctx3.watermark = wm
	if err := p.stage2.ProcessWatermark(wm, p.ctx3); err != nil {
		p.t.Fatalf("stage 2 ProcessWatermark(%d): %v", wm, err)
	}

	var out []hotRow
	for _, r := range p.ctx3.emitted[p.seen3:] {
		auction, err := sources.NexmarkKeyID(r.Key)
		if err != nil {
			p.t.Fatalf("NexmarkKeyID: %v", err)
		}
		count, err := DecodeCount(r.Value)
		if err != nil {
			p.t.Fatalf("DecodeCount: %v", err)
		}
		out = append(out, hotRow{windowStart: subFloor(r.EventTime, p.size-1), auction: auction, count: count})
	}
	p.seen3 = len(p.ctx3.emitted)
	return out
}

// TestQ5PipelineOverAHandWrittenStream, tumbling first so the expected rows can
// be counted off the input without a sliding assignment in the way.
func TestQ5PipelineOverAHandWrittenStream(t *testing.T) {
	p := newQ5Pipeline(t, 100, 100, 0)
	// Window [0, 100): auction 7 three times, auction 8 once.
	p.bid(7, 10)
	p.bid(7, 20)
	p.bid(7, 30)
	p.bid(8, 40)
	// Window [100, 200): auction 8 twice, auction 9 twice -- a tie.
	p.bid(8, 110)
	p.bid(8, 120)
	p.bid(9, 130)
	p.bid(9, 140)

	got := p.watermark(99)
	if want := []hotRow{{windowStart: 0, auction: 7, count: 3}}; !slices.Equal(got, want) {
		t.Fatalf("watermark 99 produced %+v, want %+v", got, want)
	}

	got = p.watermark(199)
	want := []hotRow{
		{windowStart: 100, auction: 8, count: 2},
		{windowStart: 100, auction: 9, count: 2},
	}
	if !slices.Equal(got, want) {
		t.Fatalf("watermark 199 produced %+v, want %+v", got, want)
	}
}

// TestQ5PipelineSliding, where a bid belongs to size/slide windows.
func TestQ5PipelineSliding(t *testing.T) {
	p := newQ5Pipeline(t, 100, 50, 0)
	// Hand-computed. Each bid joins the window aligned at or below its time
	// and the one 50 before it.
	p.bid(7, 20) // [-50, 50), [0, 100)
	p.bid(7, 30) // [-50, 50), [0, 100)
	p.bid(8, 40) // [-50, 50), [0, 100)
	p.bid(8, 60) // [0, 100), [50, 150)
	p.bid(9, 70) // [0, 100), [50, 150)
	p.bid(9, 80) // [0, 100), [50, 150)

	// [-50, 50) completes at 49: auction 7 has two, auction 8 has one.
	if got := p.watermark(49); !slices.Equal(got, []hotRow{{windowStart: -50, auction: 7, count: 2}}) {
		t.Fatalf("watermark 49 produced %+v", got)
	}
	// [0, 100) completes at 99: 7 has two, 8 has two, 9 has two. A three-way
	// tie, in ascending auction order.
	got := p.watermark(99)
	want := []hotRow{
		{windowStart: 0, auction: 7, count: 2},
		{windowStart: 0, auction: 8, count: 2},
		{windowStart: 0, auction: 9, count: 2},
	}
	if !slices.Equal(got, want) {
		t.Fatalf("watermark 99 produced %+v, want %+v", got, want)
	}
	// [50, 150) completes at 149: 8 has one, 9 has two.
	if got := p.watermark(149); !slices.Equal(got, []hotRow{{windowStart: 50, auction: 9, count: 2}}) {
		t.Fatalf("watermark 149 produced %+v", got)
	}
}

// TestQ5Stage2ReceivesOnTimeRecords is the assertion the step asks for, and it
// carries its own counterfactual.
//
// A stage 2 that saw its whole input as late would emit nothing, drop
// everything and report no error, which reads as a windowing bug in stage 2.
// So the positive claim is asserted directly -- stage 2 accepted records and
// dropped none -- and the negative one is run beside it: the same pipeline with
// stage 1 stamping a fired window at its START, which is what it did before
// Phase 3c, and which is exactly the bug this arrangement exists to survive.
//
// The watermarks step by half a window, which is what makes the counterfactual
// bite: a record stamped at the start describes a window that ended size-1
// millis earlier, and at this granularity the watermark has already passed its
// purge threshold when it arrives.
func TestQ5Stage2ReceivesOnTimeRecords(t *testing.T) {
	const (
		size  = 1000
		slide = 1000
		step  = size / 2
	)

	feed := func(p *q5Pipeline) {
		for n := int64(0); n < 40; n++ {
			p.bid(uint64(n%4), 100*n)
		}
		for wm := int64(0); wm <= 4500; wm += step {
			p.watermark(wm)
		}
	}

	correct := newQ5Pipeline(t, size, slide, 0)
	feed(correct)
	if got := correct.stage2.OnTime(); got == 0 {
		t.Fatal("stage 2 accepted no records at all: its whole input was late, which produces empty output and no error")
	}
	if got := correct.stage2.Dropped(); got != 0 {
		t.Errorf("stage 2 dropped %d of the %d records it was given; stage 1 emits at end-1 so none of them can be late",
			got, got+correct.stage2.OnTime())
	}
	if len(correct.ctx3.emitted) == 0 {
		t.Error("stage 2 emitted nothing")
	}
	t.Logf("stage 2 accepted %d records, dropped %d, and emitted %d rows",
		correct.stage2.OnTime(), correct.stage2.Dropped(), len(correct.ctx3.emitted))

	// The counterfactual: stage 1 stamping its output at the window start.
	late := newQ5Pipeline(t, size, slide, 0)
	late.stampAtWindowStart = true
	feed(late)
	t.Logf("with stage 1 stamping the window start, stage 2 accepted %d records, dropped %d, and emitted %d rows",
		late.stage2.OnTime(), late.stage2.Dropped(), len(late.ctx3.emitted))
	if late.stage2.Dropped() == 0 {
		t.Error("the counterfactual does not hold: stage 1 stamping the window start dropped nothing, so the assertion above is not about the stamp")
	}
	if late.stage2.OnTime() != 0 {
		t.Errorf("the counterfactual accepted %d records; the point of end-1 is that the alternative is ALL late",
			late.stage2.OnTime())
	}
}

// ---------------------------------------------------------------------------
// The census.
// ---------------------------------------------------------------------------

// censusContext counts the calls an operator makes to CurrentWatermark.
type censusContext struct {
	emitContext
	calls int
}

func (c *censusContext) CurrentWatermark() int64 {
	c.calls++
	return c.emitContext.CurrentWatermark()
}

// TestNoOperatorReadsTheWatermarkFromTheContext is the census, and it is the
// only thing watching this.
//
// core.Context.CurrentWatermark() is NOT restored across a checkpoint: the
// runtime hands a recovered subtask its initial minimum, and an operator that
// read its lateness rule off it would come back believing nothing had been
// purged. It would then accept records it should drop and re-emit windows the
// sink already holds.
//
// Every operator here that has such a rule keeps its watermark in its own
// KeyedState instead, under state.PrefixOperatorState. This drives each of them
// through a context that counts the calls and asserts the count is zero.
//
// It is an operator-level test on purpose. A job-level suite cannot see this:
// a run that never fails never restores, so the field and the state hold the
// same number, and the end-of-input flush fixes the contents up in any case.
// If this test goes, nothing else notices.
func TestNoOperatorReadsTheWatermarkFromTheContext(t *testing.T) {
	// Every operator in this package, so a new one has to be added here to
	// compile the list rather than being quietly omitted.
	operators := []struct {
		name string
		op   core.Operator
		// feed drives the operator through the paths that would be tempted to
		// read a watermark: a record, then a watermark, then another record.
		feed func(t *testing.T, op core.Operator, ctx core.Context)
	}{
		{"Map", NewMap(func(r *core.Record) (*core.Record, error) { return r, nil }), feedPlain},
		{"Filter", NewFilter(func(r *core.Record) bool { return true }), feedPlain},
		{"q0", NewQ0(), feedPlain},
		{"q1", NewQ1(Q1Factor), feedNexmarkBid},
		{"q2", NewQ2(Q2Divisor), feedNexmarkBid},
		{"bidsOnly", NewBidsOnly(), feedNexmarkBid},
		{"q5 re-key", NewQ5Rekey(100), feedRekey},
		{"WindowCount", NewTumblingCount(100, 0), feedPlain},
		{"q7", NewQ7(100, 0), feedNexmarkBid},
		{"q5 stage 2", NewQ5HotItems(100, 0), feedHotItems},
	}

	for _, o := range operators {
		t.Run(o.name, func(t *testing.T) {
			ctx := &censusContext{}
			if err := o.op.Open(ctx); err != nil {
				t.Fatalf("Open: %v", err)
			}
			o.feed(t, o.op, ctx)
			if ctx.calls != 0 {
				t.Errorf("%s called ctx.CurrentWatermark() %d times. It is not restored across a checkpoint: "+
					"an operator with a lateness or purge rule keeps its watermark in its own KeyedState under "+
					"state.PrefixOperatorState, as WindowCount does", o.name, ctx.calls)
			}
		})
	}
}

func feedPlain(t *testing.T, op core.Operator, ctx core.Context) {
	t.Helper()
	drive(t, op, ctx, rec("a", 10), rec("a", 150))
}

func feedNexmarkBid(t *testing.T, op core.Operator, ctx core.Context) {
	t.Helper()
	drive(t, op, ctx, bidRecord(7, 1, 50, 10), bidRecord(7, 2, 60, 150))
}

func feedRekey(t *testing.T, op core.Operator, ctx core.Context) {
	t.Helper()
	drive(t, op, ctx,
		&core.Record{Key: sources.NexmarkKey(7), Value: encodeCount(3), EventTime: 99},
		&core.Record{Key: sources.NexmarkKey(7), Value: encodeCount(4), EventTime: 199})
}

func feedHotItems(t *testing.T, op core.Operator, ctx core.Context) {
	t.Helper()
	drive(t, op, ctx, stage1Record(0, 100, 7, 3), stage1Record(100, 100, 7, 4))
}

// drive runs one record, a watermark that fires it, and a second record, then
// the end-of-stream flush.
func drive(t *testing.T, op core.Operator, ctx core.Context, first, second *core.Record) {
	t.Helper()
	if err := op.ProcessElement(first, ctx); err != nil {
		t.Fatalf("ProcessElement: %v", err)
	}
	if err := op.ProcessWatermark(99, ctx); err != nil {
		t.Fatalf("ProcessWatermark: %v", err)
	}
	if err := op.ProcessElement(second, ctx); err != nil {
		t.Fatalf("ProcessElement: %v", err)
	}
	if err := op.ProcessWatermark(math.MaxInt64, ctx); err != nil {
		t.Fatalf("ProcessWatermark: %v", err)
	}
	if err := op.OnEndOfStream(ctx); err != nil {
		t.Fatalf("OnEndOfStream: %v", err)
	}
}
