package operators

import (
	"bytes"
	"io"
	"strings"
	"testing"

	"github.com/AarinB1/tidemark/pkg/core"
	"github.com/AarinB1/tidemark/pkg/sources"
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
