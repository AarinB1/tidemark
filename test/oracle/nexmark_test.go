package oracle

import (
	"fmt"
	"slices"
	"testing"

	"github.com/AarinB1/tidemark/pkg/core"
	"github.com/AarinB1/tidemark/pkg/sources"
)

// The fixture: twenty events, written out by hand, with every expected answer
// below also written out by hand.
//
// This is the one place in this repository where the expected values are not
// produced by any code in it. Everything else is compared against the oracle,
// so the oracle itself cannot be checked against something that computes the
// same answer -- that would be the oracle agreeing with itself. Nobody can
// compute a splitmix64 stream in their head, so the input is hand-written too
// and the generated source is checked separately, in pkg/sources.
//
// The twenty events are chosen for the cases the queries turn on rather than
// for realism:
//
//   - three persons and two auctions, so the four queries that take bids only
//     have something to drop. A query that passed them through would show up
//     here and nowhere else in this package.
//   - auction 8 in window [0, 100) receives two bids at price 90 from bidders
//     1 and 2: q7's tie on the bidder id.
//   - auction 9 in window [100, 200) receives three bids at price 7 from
//     bidders 1, 2 and 3: the same tie, three ways.
//   - auctions 7 and 9 both take three bids in the tumbling window
//     [100, 200): q5's tie, which is two output rows rather than a choice.
//   - the sliding window starting at 50 gives auctions 7, 8 and 9 two bids
//     each: q5's tie, three ways.
//   - auction ids 7, 8, 9 and 10 straddle both q2 divisors used below, so
//     neither predicate keeps everything or nothing.
type fixtureEvent struct {
	// Exactly one of person, auction or bid is set, which is what kind says.
	kind      sources.NexmarkEventType
	person    sources.Person
	auction   sources.Auction
	bid       sources.Bid
	eventTime int64
}

func bidAt(auction, bidder, price uint64, t int64) fixtureEvent {
	return fixtureEvent{
		kind:      sources.EventBid,
		bid:       sources.Bid{Auction: auction, Bidder: bidder, Price: price, DateTime: t},
		eventTime: t,
	}
}

func personAt(id uint64, t int64) fixtureEvent {
	return fixtureEvent{
		kind:      sources.EventPerson,
		person:    sources.Person{ID: id, DateTime: t},
		eventTime: t,
	}
}

func auctionAt(id, seller uint64, t int64) fixtureEvent {
	return fixtureEvent{
		kind:      sources.EventAuction,
		auction:   sources.Auction{ID: id, Seller: seller, DateTime: t, Expires: t + 1000},
		eventTime: t,
	}
}

// nexmarkFixture is the twenty events, in offset order.
var nexmarkFixture = []fixtureEvent{
	/*  0 */ personAt(1, 10),
	/*  1 */ auctionAt(7, 1, 15),
	/*  2 */ bidAt(7, 1, 50, 20),
	/*  3 */ bidAt(7, 2, 90, 30),
	/*  4 */ bidAt(8, 1, 90, 40),
	/*  5 */ bidAt(7, 3, 10, 60),
	/*  6 */ personAt(2, 70),
	/*  7 */ bidAt(8, 2, 90, 80),
	/*  8 */ bidAt(9, 1, 5, 95),
	/*  9 */ auctionAt(8, 2, 110),
	/* 10 */ bidAt(7, 1, 200, 120),
	/* 11 */ bidAt(8, 4, 200, 130),
	/* 12 */ bidAt(9, 1, 7, 140),
	/* 13 */ bidAt(9, 2, 7, 150),
	/* 14 */ bidAt(9, 3, 7, 160),
	/* 15 */ personAt(3, 170),
	/* 16 */ bidAt(7, 9, 1, 180),
	/* 17 */ bidAt(8, 9, 1, 190),
	/* 18 */ bidAt(10, 1, 3, 195),
	/* 19 */ bidAt(7, 5, 42, 199),
}

// record renders one fixture event the way the source would.
func (e fixtureEvent) record() *core.Record {
	switch e.kind {
	case sources.EventPerson:
		return &core.Record{
			Key: sources.NexmarkKey(e.person.ID), Value: sources.EncodePerson(e.person), EventTime: e.eventTime,
		}
	case sources.EventAuction:
		return &core.Record{
			Key: sources.NexmarkKey(e.auction.ID), Value: sources.EncodeAuction(e.auction), EventTime: e.eventTime,
		}
	default:
		return &core.Record{
			Key: sources.NexmarkKey(e.bid.Auction), Value: sources.EncodeBid(e.bid), EventTime: e.eventTime,
		}
	}
}

func nexmarkFixtureRecords() []*core.Record {
	out := make([]*core.Record, len(nexmarkFixture))
	for i, e := range nexmarkFixture {
		out[i] = e.record()
	}
	return out
}

// bidRow is a q1 or q2 expectation written as the four fields of a bid rather
// than as bytes, so that the table below reads as bids and not as hex.
type bidRow struct {
	auction, bidder, price uint64
	eventTime              int64
}

func (r bidRow) row() NexmarkRow {
	b := sources.Bid{Auction: r.auction, Bidder: r.bidder, Price: r.price, DateTime: r.eventTime}
	return NexmarkRow{
		Key:       string(sources.NexmarkKey(r.auction)),
		Value:     string(sources.EncodeBid(b)),
		EventTime: r.eventTime,
	}
}

func rowsOf(rs []bidRow) []NexmarkRow {
	out := make([]NexmarkRow, len(rs))
	for i, r := range rs {
		out[i] = r.row()
	}
	slices.SortFunc(out, CompareNexmarkRows)
	return out
}

func assertSameRows(t *testing.T, got, want []NexmarkRow, label string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: %d rows, want %d", label, len(got), len(want))
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("%s: row %d is {key %x value %x time %d}, want {key %x value %x time %d}",
				label, i, got[i].Key, got[i].Value, got[i].EventTime,
				want[i].Key, want[i].Value, want[i].EventTime)
		}
	}
}

// TestQ0OverTheFixture.
//
// q0 is the identity, so its oracle cannot be checked against a different
// computation of the same thing: what a fixture can say is that nothing is
// dropped, nothing is duplicated and NOTHING IS FILTERED. The last one is the
// content here -- q0 is the only one of the five that carries persons and
// auctions through -- and it is the difference between q0 and q1.
func TestQ0OverTheFixture(t *testing.T) {
	got := q0Records(nexmarkFixtureRecords())

	byType := map[sources.NexmarkEventType]int64{}
	var total int64
	for row, n := range got {
		typ, err := sources.NexmarkTypeOf([]byte(row.Value))
		if err != nil {
			t.Fatalf("NexmarkTypeOf: %v", err)
		}
		byType[typ] += n
		total += n
	}
	// Hand-counted off the table above: three persons, two auctions, fifteen
	// bids.
	for typ, want := range map[sources.NexmarkEventType]int64{
		sources.EventPerson: 3, sources.EventAuction: 2, sources.EventBid: 15,
	} {
		if byType[typ] != want {
			t.Errorf("q0 passed %d %s events, want %d", byType[typ], typ, want)
		}
	}
	if total != 20 {
		t.Errorf("q0 passed %d events, want 20", total)
	}
	for row, n := range got {
		if n != 1 {
			t.Errorf("q0 passed a row %d times, want once: {key %x time %d}", n, row.Key, row.EventTime)
		}
	}
}

// TestQ1OverTheFixture: every bid's price times three, everything else dropped.
func TestQ1OverTheFixture(t *testing.T) {
	const factor = 3

	// Hand-computed: the fifteen bids of the fixture, prices tripled.
	want := rowsOf([]bidRow{
		{7, 1, 150, 20},
		{7, 2, 270, 30},
		{8, 1, 270, 40},
		{7, 3, 30, 60},
		{8, 2, 270, 80},
		{9, 1, 15, 95},
		{7, 1, 600, 120},
		{8, 4, 600, 130},
		{9, 1, 21, 140},
		{9, 2, 21, 150},
		{9, 3, 21, 160},
		{7, 9, 3, 180},
		{8, 9, 3, 190},
		{10, 1, 9, 195},
		{7, 5, 126, 199},
	})

	got, err := q1Records(nexmarkFixtureRecords(), factor)
	if err != nil {
		t.Fatalf("q1Records: %v", err)
	}
	assertSameRows(t, SortedNexmarkRows(got), want, "q1")
}

// TestQ2OverTheFixture, at two divisors, because one divisor cannot say the
// predicate depends on the auction id at all.
func TestQ2OverTheFixture(t *testing.T) {
	tests := []struct {
		divisor uint64
		want    []bidRow
	}{
		{
			// Auction ids are 7, 8, 9 and 10. A multiple of three is 9 alone.
			divisor: 3,
			want: []bidRow{
				{9, 1, 5, 95},
				{9, 1, 7, 140},
				{9, 2, 7, 150},
				{9, 3, 7, 160},
			},
		},
		{
			// A multiple of two is 8 and 10.
			divisor: 2,
			want: []bidRow{
				{8, 1, 90, 40},
				{8, 2, 90, 80},
				{8, 4, 200, 130},
				{8, 9, 1, 190},
				{10, 1, 3, 195},
			},
		},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("divisor%d", tt.divisor), func(t *testing.T) {
			got, err := q2Records(nexmarkFixtureRecords(), tt.divisor)
			if err != nil {
				t.Fatalf("q2Records: %v", err)
			}
			assertSameRows(t, SortedNexmarkRows(got), rowsOf(tt.want), "q2")
		})
	}
}

func assertSameHotItems(t *testing.T, got, want []HotItem, label string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: %d rows, want %d\n got %+v\nwant %+v", label, len(got), len(want), got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("%s: row %d is %+v, want %+v", label, i, got[i], want[i])
		}
	}
}

// TestQ5OverTheFixture, tumbling and sliding.
//
// Both tables are hand-counted off the fixture. The tumbling case has one tie,
// the sliding case has a three-way one, and in both a tie is several rows
// rather than a choice: q5 emits every auction at the window's maximum, which
// is Nexmark's "num >= ALL" and is what makes the result deterministic without
// inventing a rule to break with.
func TestQ5OverTheFixture(t *testing.T) {
	t.Run("tumbling", func(t *testing.T) {
		// [0, 100): auction 7 takes three bids (t=20, 30, 60), auction 8 two
		// (40, 80), auction 9 one (95). Maximum three, auction 7 alone.
		// [100, 200): auction 7 takes three (120, 180, 199), auction 8 two
		// (130, 190), auction 9 three (140, 150, 160), auction 10 one (195).
		// Maximum three, auctions 7 AND 9.
		want := []HotItem{
			{WindowStart: 0, Auction: 7, Count: 3},
			{WindowStart: 100, Auction: 7, Count: 3},
			{WindowStart: 100, Auction: 9, Count: 3},
		}
		got, err := q5Records(nexmarkFixtureRecords(), Spec{Size: 100, Slide: 100})
		if err != nil {
			t.Fatalf("q5Records: %v", err)
		}
		assertSameHotItems(t, SortedHotItems(got), want, "q5 tumbling")
	})

	t.Run("sliding", func(t *testing.T) {
		// Size 100, slide 50, so each bid falls in two windows.
		//
		// [-50, 50): auction 7 has t=20, 30; auction 8 has t=40. Max two, 7.
		// [0, 100):  7 has 20, 30, 60; 8 has 40, 80; 9 has 95. Max three, 7.
		// [50, 150): 7 has 60, 120; 8 has 80, 130; 9 has 95, 140. Max two, all
		//            three of them.
		// [100, 200): 7 has 120, 180, 199; 8 has 130, 190; 9 has 140, 150,
		//            160; 10 has 195. Max three, 7 and 9.
		// [150, 250): 7 has 180, 199; 8 has 190; 9 has 150, 160; 10 has 195.
		//            Max two, 7 and 9.
		want := []HotItem{
			{WindowStart: -50, Auction: 7, Count: 2},
			{WindowStart: 0, Auction: 7, Count: 3},
			{WindowStart: 50, Auction: 7, Count: 2},
			{WindowStart: 50, Auction: 8, Count: 2},
			{WindowStart: 50, Auction: 9, Count: 2},
			{WindowStart: 100, Auction: 7, Count: 3},
			{WindowStart: 100, Auction: 9, Count: 3},
			{WindowStart: 150, Auction: 7, Count: 2},
			{WindowStart: 150, Auction: 9, Count: 2},
		}
		got, err := q5Records(nexmarkFixtureRecords(), Spec{Size: 100, Slide: 50})
		if err != nil {
			t.Fatalf("q5Records: %v", err)
		}
		assertSameHotItems(t, SortedHotItems(got), want, "q5 sliding")
	})
}

// TestQ7OverTheFixture.
//
// Hand-computed per (auction, window). Two of the seven rows are decided by the
// tie rule rather than by the price: auction 8 in [0, 100) takes 90 from
// bidder 1 and 90 from bidder 2, and auction 9 in [100, 200) takes 7 from
// bidders 1, 2 and 3. Both go to the lowest bidder.
func TestQ7OverTheFixture(t *testing.T) {
	want := []MaxBidRow{
		// [0, 100)
		{WindowStart: 0, Auction: 7, Price: 90, Bidder: 2},
		{WindowStart: 0, Auction: 8, Price: 90, Bidder: 1}, // tie on price, lowest bidder
		{WindowStart: 0, Auction: 9, Price: 5, Bidder: 1},
		// [100, 200)
		{WindowStart: 100, Auction: 7, Price: 200, Bidder: 1},
		{WindowStart: 100, Auction: 8, Price: 200, Bidder: 4},
		{WindowStart: 100, Auction: 9, Price: 7, Bidder: 1}, // three-way tie, lowest bidder
		{WindowStart: 100, Auction: 10, Price: 3, Bidder: 1},
	}

	got, err := q7Records(nexmarkFixtureRecords(), 100)
	if err != nil {
		t.Fatalf("q7Records: %v", err)
	}
	rows := SortedMaxBids(got)
	if len(rows) != len(want) {
		t.Fatalf("q7 produced %d rows, want %d: %+v", len(rows), len(want), rows)
	}
	for i := range rows {
		if rows[i] != want[i] {
			t.Fatalf("q7 row %d is %+v, want %+v", i, rows[i], want[i])
		}
	}
}

// TestBetterBidIsATotalOrder exercises the tier q7 itself cannot reach.
//
// The auction tier is unreachable inside q7, where the auction is the grouping
// key and is therefore constant within a comparison. Testing the rule only
// through the query would leave a third of it unexercised while looking like it
// was covered, so it is tested directly here on inputs the query cannot
// produce.
func TestBetterBidIsATotalOrder(t *testing.T) {
	tests := []struct {
		name                   string
		p1, b1, a1, p2, b2, a2 uint64
		want                   bool
	}{
		{"a higher price wins", 10, 9, 9, 5, 1, 1, true},
		{"a lower price loses", 5, 1, 1, 10, 9, 9, false},
		{"equal price, lower bidder wins", 10, 1, 5, 10, 2, 5, true},
		{"equal price, higher bidder loses", 10, 2, 5, 10, 1, 5, false},
		{"equal price and bidder, lower auction wins", 10, 1, 4, 10, 1, 5, true},
		{"equal price and bidder, higher auction loses", 10, 1, 5, 10, 1, 4, false},
		{"identical is not better", 10, 1, 5, 10, 1, 5, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := BetterBid(tt.p1, tt.b1, tt.a1, tt.p2, tt.b2, tt.a2); got != tt.want {
				t.Errorf("BetterBid = %v, want %v", got, tt.want)
			}
		})
	}
	// Antisymmetry over the whole small cube, which is what "total order"
	// means and what a per-row table cannot say.
	for p1 := uint64(0); p1 < 3; p1++ {
		for b1 := uint64(0); b1 < 3; b1++ {
			for a1 := uint64(0); a1 < 3; a1++ {
				for p2 := uint64(0); p2 < 3; p2++ {
					for b2 := uint64(0); b2 < 3; b2++ {
						for a2 := uint64(0); a2 < 3; a2++ {
							ab := BetterBid(p1, b1, a1, p2, b2, a2)
							ba := BetterBid(p2, b2, a2, p1, b1, a1)
							same := p1 == p2 && b1 == b2 && a1 == a2
							if same && (ab || ba) {
								t.Fatalf("(%d,%d,%d) beats itself", p1, b1, a1)
							}
							if !same && ab == ba {
								t.Fatalf("(%d,%d,%d) and (%d,%d,%d) both %v",
									p1, b1, a1, p2, b2, a2, ab)
							}
						}
					}
				}
			}
		}
	}
}

// TestNexmarkOraclesAreIndependentOfInputOrder.
//
// The engine's subtasks deliver records in whatever order the shuffle and the
// scheduler produced, so an oracle whose answer depended on arrival order
// would disagree with a correct engine at random. That would look like a
// windowing bug and would be the oracle.
//
// The interesting one is q7: its answer is a maximum under a rule, and a rule
// that was not a strict total order would give a different winner for a
// different arrival order without ever failing anything else.
func TestNexmarkOraclesAreIndependentOfInputOrder(t *testing.T) {
	forward := nexmarkFixtureRecords()
	reversed := slices.Clone(forward)
	slices.Reverse(reversed)
	// A third order that is neither, so the assertion is not just "reversal".
	shuffled := slices.Clone(forward)
	for i := range shuffled {
		j := (i*7 + 3) % len(shuffled)
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	}

	for _, order := range []struct {
		name string
		recs []*core.Record
	}{{"reversed", reversed}, {"shuffled", shuffled}} {
		t.Run(order.name, func(t *testing.T) {
			wantQ5, err := q5Records(forward, Spec{Size: 100, Slide: 50})
			if err != nil {
				t.Fatalf("q5Records: %v", err)
			}
			gotQ5, err := q5Records(order.recs, Spec{Size: 100, Slide: 50})
			if err != nil {
				t.Fatalf("q5Records: %v", err)
			}
			assertSameHotItems(t, SortedHotItems(gotQ5), SortedHotItems(wantQ5), "q5")

			wantQ7, err := q7Records(forward, 100)
			if err != nil {
				t.Fatalf("q7Records: %v", err)
			}
			gotQ7, err := q7Records(order.recs, 100)
			if err != nil {
				t.Fatalf("q7Records: %v", err)
			}
			if !slices.Equal(SortedMaxBids(gotQ7), SortedMaxBids(wantQ7)) {
				t.Errorf("q7 differs under a different input order:\n got %+v\nwant %+v",
					SortedMaxBids(gotQ7), SortedMaxBids(wantQ7))
			}

			wantQ1, err := q1Records(forward, 3)
			if err != nil {
				t.Fatalf("q1Records: %v", err)
			}
			gotQ1, err := q1Records(order.recs, 3)
			if err != nil {
				t.Fatalf("q1Records: %v", err)
			}
			assertSameRows(t, SortedNexmarkRows(gotQ1), SortedNexmarkRows(wantQ1), "q1")
		})
	}
}

// nexmarkOracleConfig is the generated input the regenerating oracles are
// checked over.
func nexmarkOracleConfig(seed uint64, count int64) sources.NexmarkConfig {
	return sources.NexmarkConfig{
		Seed:               seed,
		Count:              count,
		AuctionCardinality: 16,
		PersonCardinality:  8,
		PriceRange:         500,
		CategoryCount:      4,
		AuctionDuration:    5000,
		BaseEventTime:      1700000000000,
		EventTimeStep:      10,
		MaxLag:             200,
	}
}

// TestNexmarkOraclesMatchTheirRecordForms.
//
// The exported oracles regenerate their input and the unexported forms take it
// in hand. The fixtures above check the unexported ones, so this is what
// carries that evidence across to the exported ones: it says the two read the
// same events and do the same arithmetic, and it is the reason the fixture is
// worth writing at all.
func TestNexmarkOraclesMatchTheirRecordForms(t *testing.T) {
	const (
		count   = 2000
		factor  = 89
		divisor = 3
		size    = 5000
	)
	spec := Spec{Size: size, Slide: size / 4}

	for seed := uint64(1); seed <= 5; seed++ {
		cfg := nexmarkOracleConfig(seed, count)

		// The same events, materialised once, for the record forms.
		var recs []*core.Record
		if err := readNexmark(cfg, func(rec *core.Record) error {
			recs = append(recs, rec)
			return nil
		}); err != nil {
			t.Fatalf("readNexmark: %v", err)
		}
		if int64(len(recs)) != count {
			t.Fatalf("seed %d: read %d events, want %d", seed, len(recs), count)
		}

		gotQ0, err := NexmarkQ0(cfg)
		if err != nil {
			t.Fatalf("NexmarkQ0: %v", err)
		}
		assertSameRows(t, SortedNexmarkRows(gotQ0), SortedNexmarkRows(q0Records(recs)), "q0")

		gotQ1, err := NexmarkQ1(cfg, factor)
		if err != nil {
			t.Fatalf("NexmarkQ1: %v", err)
		}
		wantQ1, err := q1Records(recs, factor)
		if err != nil {
			t.Fatalf("q1Records: %v", err)
		}
		assertSameRows(t, SortedNexmarkRows(gotQ1), SortedNexmarkRows(wantQ1), "q1")

		gotQ2, err := NexmarkQ2(cfg, divisor)
		if err != nil {
			t.Fatalf("NexmarkQ2: %v", err)
		}
		wantQ2, err := q2Records(recs, divisor)
		if err != nil {
			t.Fatalf("q2Records: %v", err)
		}
		assertSameRows(t, SortedNexmarkRows(gotQ2), SortedNexmarkRows(wantQ2), "q2")

		gotQ5, err := NexmarkQ5(cfg, spec)
		if err != nil {
			t.Fatalf("NexmarkQ5: %v", err)
		}
		wantQ5, err := q5Records(recs, spec)
		if err != nil {
			t.Fatalf("q5Records: %v", err)
		}
		assertSameHotItems(t, SortedHotItems(gotQ5), SortedHotItems(wantQ5), "q5")

		gotQ7, err := NexmarkQ7(cfg, size)
		if err != nil {
			t.Fatalf("NexmarkQ7: %v", err)
		}
		wantQ7, err := q7Records(recs, size)
		if err != nil {
			t.Fatalf("q7Records: %v", err)
		}
		if !slices.Equal(SortedMaxBids(gotQ7), SortedMaxBids(wantQ7)) {
			t.Errorf("seed %d: q7 differs between the regenerating and the record forms", seed)
		}
	}
}

// TestNexmarkOraclesRejectABadSpecification.
func TestNexmarkOraclesRejectABadSpecification(t *testing.T) {
	cfg := nexmarkOracleConfig(1, 100)
	if _, err := NexmarkQ2(cfg, 0); err == nil {
		t.Error("q2 accepted a divisor of zero")
	}
	for _, spec := range []Spec{{Size: 0, Slide: 100}, {Size: 100, Slide: 0}, {Size: 100, Slide: 30}} {
		if _, err := NexmarkQ5(cfg, spec); err == nil {
			t.Errorf("q5 accepted %+v", spec)
		}
	}
	for _, size := range []int64{0, -1} {
		if _, err := NexmarkQ7(cfg, size); err == nil {
			t.Errorf("q7 accepted size %d", size)
		}
	}
}

// TestQ5SelectsOnlyTheMaximum guards the second pass.
//
// A q5Select that returned every (auction, window) rather than the maxima
// would still agree with the engine if the engine had the same bug, so the
// property is asserted directly against the counts it was derived from: every
// row kept is its window's maximum, and every window with any count has at
// least one row.
func TestQ5SelectsOnlyTheMaximum(t *testing.T) {
	cfg := nexmarkOracleConfig(4, 3000)
	spec := Spec{Size: 5000, Slide: 5000}

	counts := make(map[HotItemKey]int64)
	if err := readNexmark(cfg, func(rec *core.Record) error {
		return q5Accumulate(counts, rec, spec)
	}); err != nil {
		t.Fatalf("readNexmark: %v", err)
	}
	if len(counts) == 0 {
		t.Fatal("no bids reached the counts; this test asserts nothing")
	}

	best := map[int64]int64{}
	windows := map[int64]bool{}
	for k, n := range counts {
		windows[k.WindowStart] = true
		if n > best[k.WindowStart] {
			best[k.WindowStart] = n
		}
	}

	selected := q5Select(counts)
	if len(selected) >= len(counts) {
		t.Errorf("q5 kept %d of %d (auction, window) counts; with several auctions per window it must keep fewer",
			len(selected), len(counts))
	}
	seenWindows := map[int64]bool{}
	for k, n := range selected {
		if n != best[k.WindowStart] {
			t.Errorf("q5 kept auction %d in window %d at count %d, but that window's maximum is %d",
				k.Auction, k.WindowStart, n, best[k.WindowStart])
		}
		if counts[k] != n {
			t.Errorf("q5 reported count %d for a pair the input counted at %d", n, counts[k])
		}
		seenWindows[k.WindowStart] = true
	}
	for w := range windows {
		if !seenWindows[w] {
			t.Errorf("window %d received bids and produced no row", w)
		}
	}
}
