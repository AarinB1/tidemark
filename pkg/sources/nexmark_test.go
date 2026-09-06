package sources

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"testing"

	"github.com/AarinB1/tidemark/pkg/core"
)

func nexmarkTestConfig(seed uint64) NexmarkConfig {
	return NexmarkConfig{
		Seed:               seed,
		Count:              5000,
		AuctionCardinality: 64,
		PersonCardinality:  32,
		PriceRange:         1000,
		CategoryCount:      5,
		AuctionDuration:    10000,
		BaseEventTime:      1700000000000,
		EventTimeStep:      10,
		MaxLag:             200,
	}
}

func openNexmark(t *testing.T, cfg NexmarkConfig) *Nexmark {
	t.Helper()
	src := NewNexmark(cfg)
	if err := src.Open(nil); err != nil {
		t.Fatalf("Open: %v", err)
	}
	return src
}

// readNexmark reads n records, failing if the source runs out first.
func readNexmark(t *testing.T, src *Nexmark, n int) []*core.Record {
	t.Helper()
	out := make([]*core.Record, 0, n)
	for range n {
		rec, ok, err := src.Next()
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if !ok {
			t.Fatalf("the source ran out after %d records, want %d", len(out), n)
		}
		out = append(out, rec)
	}
	return out
}

func equalNexmarkRecords(a, b *core.Record) bool {
	return bytes.Equal(a.Key, b.Key) && bytes.Equal(a.Value, b.Value) && a.EventTime == b.EventTime
}

// TestNexmarkSeekToMatchesSequentialRead is invariant 7 for this source.
//
// Four seeds and several offsets each, comparing the FULL record -- key, value
// and event time -- rather than a field of it. A seek that reproduced the
// event times and not the values would pass a comparison on event time alone,
// and a recovered run would then commit different events under the same
// timestamps with nothing to point at.
func TestNexmarkSeekToMatchesSequentialRead(t *testing.T) {
	offsets := []int64{0, 1, 7, 49, 50, 51, 137, 999, 2500}

	for _, seed := range []uint64{1, 2, 3, 4, 0xDEADBEEF} {
		for _, offset := range offsets {
			t.Run(fmt.Sprintf("seed%d/offset%d", seed, offset), func(t *testing.T) {
				cfg := nexmarkTestConfig(seed)

				// Read from zero and discard the first offset records.
				sequential := openNexmark(t, cfg)
				readNexmark(t, sequential, int(offset))
				want := readNexmark(t, sequential, 200)

				// Seek, then read.
				seeked := openNexmark(t, cfg)
				if err := seeked.SeekTo(offset); err != nil {
					t.Fatalf("SeekTo(%d): %v", offset, err)
				}
				if got := seeked.Position(); got != offset {
					t.Fatalf("Position after SeekTo(%d) is %d", offset, got)
				}
				got := readNexmark(t, seeked, 200)

				for i := range want {
					if !equalNexmarkRecords(got[i], want[i]) {
						t.Fatalf("element %d after seeking to %d is {key %x value %x time %d}, want {key %x value %x time %d}",
							i, offset, got[i].Key, got[i].Value, got[i].EventTime,
							want[i].Key, want[i].Value, want[i].EventTime)
					}
				}
			})
		}
	}
}

// TestNexmarkSameSeedSameSequence: element n is a pure function of (Seed, n),
// so two independent sources agree everywhere.
func TestNexmarkSameSeedSameSequence(t *testing.T) {
	for _, seed := range []uint64{1, 2, 3, 4} {
		a := readNexmark(t, openNexmark(t, nexmarkTestConfig(seed)), 500)
		b := readNexmark(t, openNexmark(t, nexmarkTestConfig(seed)), 500)
		for i := range a {
			if !equalNexmarkRecords(a[i], b[i]) {
				t.Fatalf("seed %d: element %d differs between two sources of the same seed", seed, i)
			}
		}
	}
}

// TestNexmarkDifferentSeedsDiffer guards the salting: a seed that did not reach
// every field would leave that field identical across seeds, and the source
// would look deterministic while carrying less variation than it claims.
func TestNexmarkDifferentSeedsDiffer(t *testing.T) {
	a := readNexmark(t, openNexmark(t, nexmarkTestConfig(1)), 500)
	b := readNexmark(t, openNexmark(t, nexmarkTestConfig(2)), 500)

	same := 0
	for i := range a {
		if equalNexmarkRecords(a[i], b[i]) {
			same++
		}
	}
	// A handful of collisions is arithmetic; hundreds is a seed that is not
	// reaching the fields.
	if same > len(a)/10 {
		t.Errorf("%d of %d elements are identical across two seeds", same, len(a))
	}
}

// TestNexmarkEventProportions pins the mix at exactly 1:3:46 per fifty.
//
// Exact, not approximate: the type is a function of the offset, so a run whose
// length is a multiple of fifty holds exactly that ratio and a test can say so
// without a tolerance. A source that drew its type from the seed would need a
// range here, and a range is a test that passes under a mix that has drifted.
func TestNexmarkEventProportions(t *testing.T) {
	const cycles = 100
	cfg := nexmarkTestConfig(9)
	cfg.Count = cycles * nexmarkEventsPerCycle

	counts := map[NexmarkEventType]int{}
	src := openNexmark(t, cfg)
	for {
		rec, ok, err := src.Next()
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if !ok {
			break
		}
		typ, err := NexmarkTypeOf(rec.Value)
		if err != nil {
			t.Fatalf("NexmarkTypeOf: %v", err)
		}
		counts[typ]++
	}

	want := map[NexmarkEventType]int{
		EventPerson:  cycles * nexmarkPersonsPerCycle,
		EventAuction: cycles * nexmarkAuctionsPerCycle,
		EventBid:     cycles * nexmarkBidsPerCycle,
	}
	for typ, n := range want {
		if counts[typ] != n {
			t.Errorf("%s: %d events, want %d", typ, counts[typ], n)
		}
	}
	if got := counts[EventPerson] + counts[EventAuction] + counts[EventBid]; int64(got) != cfg.Count {
		t.Errorf("the three types total %d events, want %d", got, cfg.Count)
	}
}

// TestNexmarkTypeIsAFunctionOfOffsetAlone. The seed must not move the mix, or
// SeekTo would have to reconstruct a cycle position.
func TestNexmarkTypeIsAFunctionOfOffsetAlone(t *testing.T) {
	for i := int64(0); i < 200; i++ {
		want := NewNexmark(nexmarkTestConfig(1)).TypeAt(i)
		for _, seed := range []uint64{2, 3, 99, 12345} {
			if got := NewNexmark(nexmarkTestConfig(seed)).TypeAt(i); got != want {
				t.Fatalf("offset %d is a %s under seed %d and a %s under seed 1", i, got, seed, want)
			}
		}
	}
	// The cycle itself, written out rather than derived.
	for _, tt := range []struct {
		offset int64
		want   NexmarkEventType
	}{
		{0, EventPerson}, {1, EventAuction}, {2, EventAuction}, {3, EventAuction},
		{4, EventBid}, {49, EventBid}, {50, EventPerson}, {51, EventAuction},
		{53, EventAuction}, {54, EventBid}, {99, EventBid}, {100, EventPerson},
	} {
		if got := NewNexmark(nexmarkTestConfig(1)).TypeAt(tt.offset); got != tt.want {
			t.Errorf("offset %d is a %s, want a %s", tt.offset, got, tt.want)
		}
	}
}

// TestNexmarkKeysAreTheIdTheQueryGroupsOn.
//
// A bid keys on its AUCTION and not its bidder. Keying on the bidder would
// scatter one auction's bids across every subtask, so q5's count per auction
// and q7's maximum per auction would each be computed over a share of their
// input, and every count would simply be too small.
func TestNexmarkKeysAreTheIdTheQueryGroupsOn(t *testing.T) {
	src := openNexmark(t, nexmarkTestConfig(3))
	seen := map[NexmarkEventType]int{}
	for _, rec := range readNexmark(t, src, 500) {
		typ, err := NexmarkTypeOf(rec.Value)
		if err != nil {
			t.Fatalf("NexmarkTypeOf: %v", err)
		}
		id, err := NexmarkKeyID(rec.Key)
		if err != nil {
			t.Fatalf("NexmarkKeyID: %v", err)
		}
		seen[typ]++
		switch typ {
		case EventPerson:
			p, err := DecodePerson(rec.Value)
			if err != nil {
				t.Fatalf("DecodePerson: %v", err)
			}
			if id != p.ID {
				t.Fatalf("a person is keyed %d and its id is %d", id, p.ID)
			}
		case EventAuction:
			a, err := DecodeAuction(rec.Value)
			if err != nil {
				t.Fatalf("DecodeAuction: %v", err)
			}
			if id != a.ID {
				t.Fatalf("an auction is keyed %d and its id is %d", id, a.ID)
			}
		case EventBid:
			b, err := DecodeBid(rec.Value)
			if err != nil {
				t.Fatalf("DecodeBid: %v", err)
			}
			if id != b.Auction {
				t.Fatalf("a bid is keyed %d; its auction is %d and its bidder is %d",
					id, b.Auction, b.Bidder)
			}
			if id == b.Bidder && b.Bidder != b.Auction {
				t.Fatalf("a bid is keyed on its bidder")
			}
		}
		if len(rec.Key) != NexmarkKeyBytes {
			t.Fatalf("a %s key is %d bytes, want %d", typ, len(rec.Key), NexmarkKeyBytes)
		}
	}
	for _, typ := range []NexmarkEventType{EventPerson, EventAuction, EventBid} {
		if seen[typ] == 0 {
			t.Errorf("no %s reached this assertion", typ)
		}
	}
}

// TestNexmarkDateTimeIsTheRecordEventTime. The two are one number in two
// places; a source that let them drift would give the oracle, which reads the
// value, a different time from the engine, which windows on the record.
func TestNexmarkDateTimeIsTheRecordEventTime(t *testing.T) {
	src := openNexmark(t, nexmarkTestConfig(5))
	for i, rec := range readNexmark(t, src, 300) {
		typ, err := NexmarkTypeOf(rec.Value)
		if err != nil {
			t.Fatalf("NexmarkTypeOf: %v", err)
		}
		var dateTime int64
		switch typ {
		case EventPerson:
			p, err := DecodePerson(rec.Value)
			if err != nil {
				t.Fatalf("DecodePerson: %v", err)
			}
			dateTime = p.DateTime
		case EventAuction:
			a, err := DecodeAuction(rec.Value)
			if err != nil {
				t.Fatalf("DecodeAuction: %v", err)
			}
			dateTime = a.DateTime
			if a.Expires <= a.DateTime {
				t.Fatalf("element %d expires at %d, which is not after its dateTime %d", i, a.Expires, a.DateTime)
			}
			if a.Reserve < a.InitialBid {
				t.Fatalf("element %d has reserve %d below its initial bid %d", i, a.Reserve, a.InitialBid)
			}
		case EventBid:
			b, err := DecodeBid(rec.Value)
			if err != nil {
				t.Fatalf("DecodeBid: %v", err)
			}
			dateTime = b.DateTime
		}
		if dateTime != rec.EventTime {
			t.Fatalf("element %d carries dateTime %d and EventTime %d", i, dateTime, rec.EventTime)
		}
	}
}

// TestNexmarkEventTimeWithinLagBound is the property the watermark rests on: a
// source vertex whose MaxOutOfOrderness is MaxLag emits a watermark no record
// can be late against, which is what makes a comparison against a batch oracle
// with no lateness model valid rather than approximately valid.
func TestNexmarkEventTimeWithinLagBound(t *testing.T) {
	for _, seed := range []uint64{1, 2, 3, 4} {
		cfg := nexmarkTestConfig(seed)
		src := openNexmark(t, cfg)
		for i, rec := range readNexmark(t, src, int(cfg.Count)) {
			inOrder := cfg.BaseEventTime + int64(i)*cfg.EventTimeStep
			if rec.EventTime > inOrder || rec.EventTime < inOrder-cfg.MaxLag {
				t.Fatalf("seed %d: element %d has event time %d, outside [%d, %d]",
					seed, i, rec.EventTime, inOrder-cfg.MaxLag, inOrder)
			}
		}
	}
}

// TestNexmarkEventTimeLagCoverage. A lag that was always zero would satisfy the
// bound above and leave the out-of-orderness untested; one that was always
// MaxLag would shift every event time by a constant and do the same.
func TestNexmarkEventTimeLagCoverage(t *testing.T) {
	cfg := nexmarkTestConfig(11)
	src := openNexmark(t, cfg)
	lags := map[int64]bool{}
	for i, rec := range readNexmark(t, src, int(cfg.Count)) {
		lags[cfg.BaseEventTime+int64(i)*cfg.EventTimeStep-rec.EventTime] = true
	}
	if len(lags) < int(cfg.MaxLag)/2 {
		t.Errorf("%d distinct lags over %d events with MaxLag %d; the lag is barely varying",
			len(lags), cfg.Count, cfg.MaxLag)
	}
}

// TestNexmarkAuctionIdSpaceIsTheConfiguredDial.
//
// The id space is what Phase 6b turns to reach a state-size target and what
// decides whether a windowed aggregation has anything to aggregate: bids key on
// their auction, so the number of distinct keys a window operator holds is this
// number and not the event count.
func TestNexmarkAuctionIdSpaceIsTheConfiguredDial(t *testing.T) {
	for _, cardinality := range []int64{1, 4, 64, 500} {
		t.Run(fmt.Sprintf("cardinality%d", cardinality), func(t *testing.T) {
			cfg := nexmarkTestConfig(13)
			cfg.AuctionCardinality = cardinality
			// Enough bids that every id in the space is very likely drawn.
			cfg.Count = cardinality * 400
			if cfg.Count < 5000 {
				cfg.Count = 5000
			}

			src := openNexmark(t, cfg)
			bidKeys := map[uint64]bool{}
			bids := 0
			for {
				rec, ok, err := src.Next()
				if err != nil {
					t.Fatalf("Next: %v", err)
				}
				if !ok {
					break
				}
				typ, err := NexmarkTypeOf(rec.Value)
				if err != nil {
					t.Fatalf("NexmarkTypeOf: %v", err)
				}
				if typ != EventBid {
					continue
				}
				bids++
				id, err := NexmarkKeyID(rec.Key)
				if err != nil {
					t.Fatalf("NexmarkKeyID: %v", err)
				}
				if int64(id) >= cardinality {
					t.Fatalf("a bid keys on auction %d, outside the space [0, %d)", id, cardinality)
				}
				bidKeys[id] = true
			}
			if int64(len(bidKeys)) != cardinality {
				t.Errorf("%d distinct auction keys over %d bids, want the full space of %d",
					len(bidKeys), bids, cardinality)
			}
		})
	}
}

// TestNexmarkExhaustsExactlyAtCount.
func TestNexmarkExhaustsExactlyAtCount(t *testing.T) {
	cfg := nexmarkTestConfig(1)
	cfg.Count = 137
	src := openNexmark(t, cfg)
	readNexmark(t, src, int(cfg.Count))

	rec, ok, err := src.Next()
	if err != nil || ok || rec != nil {
		t.Fatalf("Next past the end returned (%v, %v, %v), want (nil, false, nil)", rec, ok, err)
	}
	if got := src.Position(); got != cfg.Count {
		t.Errorf("Position at exhaustion is %d, want %d", got, cfg.Count)
	}
	if got := src.Count(); got != cfg.Count {
		t.Errorf("Count is %d, want %d", got, cfg.Count)
	}
}

// splittable is the shape pkg/runtime dispatches on to divide a source's offset
// space across subtasks. It is restated here rather than imported because
// pkg/runtime imports this package, and the point is exactly that the method
// set is what matters: a source without Count is refused above parallelism 1.
type splittable interface {
	core.Source
	Count() int64
}

// TestNexmarkReportsACountSoItCanBeSplit is the decorator rule from CLAUDE.md
// applied to the source itself.
//
// Without Count the runtime refuses parallelism above 1, so every later test
// that claims to run this source across several subtasks would silently run one
// -- and a suite about watermark minimums over several inputs would be
// asserting nothing at all. The assertion is structural rather than behavioural
// because that is the failure: a missing method, not a wrong number.
func TestNexmarkReportsACountSoItCanBeSplit(t *testing.T) {
	var src any = NewNexmark(nexmarkTestConfig(1))
	if _, ok := src.(splittable); !ok {
		t.Fatal("*Nexmark does not satisfy the splittable shape the runtime divides an offset space with; parallelism above 1 would be refused")
	}
}

// TestNexmarkLayoutIsExactlyTheDocumentedBytes.
//
// The expected bytes are written out by hand, not produced by the encoders.
// An assertion built on Encode-then-Decode passes under any self-consistent
// layout, including one that has silently moved: the batch oracle and the
// queries read these offsets, so the offsets themselves are what has to be
// pinned.
func TestNexmarkLayoutIsExactlyTheDocumentedBytes(t *testing.T) {
	person := Person{ID: 1, NameOffset: 2, CityOffset: 3, StateOffset: 4, DateTime: 5}
	auction := Auction{ID: 1, Seller: 2, Category: 3, InitialBid: 4, Reserve: 5, DateTime: 6, Expires: 7}
	bid := Bid{Auction: 1, Bidder: 2, Price: 3, DateTime: 4}

	be := func(vs ...uint64) []byte {
		out := make([]byte, 0, 8*len(vs))
		for _, v := range vs {
			var buf [8]byte
			binary.BigEndian.PutUint64(buf[:], v)
			out = append(out, buf[:]...)
		}
		return out
	}

	tests := []struct {
		name string
		got  []byte
		want []byte
	}{
		{"person", EncodePerson(person), append([]byte{0x01}, be(1, 2, 3, 4, 5)...)},
		{"auction", EncodeAuction(auction), append([]byte{0x02}, be(1, 2, 3, 4, 5, 6, 7)...)},
		{"bid", EncodeBid(bid), append([]byte{0x03}, be(1, 2, 3, 4)...)},
	}
	for _, tt := range tests {
		if !bytes.Equal(tt.got, tt.want) {
			t.Errorf("%s encodes to %x, want %x", tt.name, tt.got, tt.want)
		}
	}

	for _, tt := range []struct {
		name string
		got  int
		want int
	}{
		{"person", len(EncodePerson(person)), NexmarkPersonBytes},
		{"auction", len(EncodeAuction(auction)), NexmarkAuctionBytes},
		{"bid", len(EncodeBid(bid)), NexmarkBidBytes},
	} {
		if tt.got != tt.want {
			t.Errorf("a %s is %d bytes, want %d", tt.name, tt.got, tt.want)
		}
	}
}

// TestNexmarkRoundTrips, including the extremes of the ranges.
func TestNexmarkRoundTrips(t *testing.T) {
	people := []Person{
		{},
		{ID: math.MaxUint64, NameOffset: math.MaxUint64, CityOffset: 1, StateOffset: 2, DateTime: math.MinInt64},
		{ID: 7, DateTime: math.MaxInt64},
		{ID: 7, DateTime: -1},
	}
	for _, p := range people {
		got, err := DecodePerson(EncodePerson(p))
		if err != nil || got != p {
			t.Errorf("person %+v round-tripped to (%+v, %v)", p, got, err)
		}
	}

	auctions := []Auction{
		{},
		{ID: math.MaxUint64, Seller: 1, Category: 2, InitialBid: 3, Reserve: math.MaxUint64, DateTime: -1, Expires: math.MaxInt64},
		{ID: 9, DateTime: math.MinInt64, Expires: math.MinInt64 + 1},
	}
	for _, a := range auctions {
		got, err := DecodeAuction(EncodeAuction(a))
		if err != nil || got != a {
			t.Errorf("auction %+v round-tripped to (%+v, %v)", a, got, err)
		}
	}

	bids := []Bid{
		{},
		{Auction: math.MaxUint64, Bidder: math.MaxUint64, Price: math.MaxUint64, DateTime: math.MaxInt64},
		{Auction: 1, Bidder: 2, Price: 3, DateTime: -4},
	}
	for _, b := range bids {
		got, err := DecodeBid(EncodeBid(b))
		if err != nil || got != b {
			t.Errorf("bid %+v round-tripped to (%+v, %v)", b, got, err)
		}
	}
}

// TestNexmarkDecodersRefuseTheWrongShape.
//
// The case that matters is a Person decoded as a Bid. Both start with a
// discriminator and four eight-byte fields, so a decoder that checked only the
// LENGTH IT NEEDED would succeed and hand back three plausible numbers. The
// check is on the discriminator and on the exact size, and this is what says so.
func TestNexmarkDecodersRefuseTheWrongShape(t *testing.T) {
	person := EncodePerson(Person{ID: 1, NameOffset: 2, CityOffset: 3, StateOffset: 4, DateTime: 5})
	auction := EncodeAuction(Auction{ID: 1})
	bid := EncodeBid(Bid{Auction: 1, Bidder: 2, Price: 3, DateTime: 4})

	tests := []struct {
		name  string
		value []byte
		call  func([]byte) error
	}{
		{"a person read as a bid", person, func(v []byte) error { _, err := DecodeBid(v); return err }},
		{"a bid read as a person", bid, func(v []byte) error { _, err := DecodePerson(v); return err }},
		{"an auction read as a bid", auction, func(v []byte) error { _, err := DecodeBid(v); return err }},
		{"a bid read as an auction", bid, func(v []byte) error { _, err := DecodeAuction(v); return err }},
		{"an empty value", nil, func(v []byte) error { _, err := DecodeBid(v); return err }},
		{"a truncated bid", bid[:len(bid)-1], func(v []byte) error { _, err := DecodeBid(v); return err }},
		{"a bid with trailing bytes", append(append([]byte{}, bid...), 0),
			func(v []byte) error { _, err := DecodeBid(v); return err }},
		{"an unknown discriminator", []byte{0x7F, 0, 0, 0, 0, 0, 0, 0, 0},
			func(v []byte) error { _, err := NexmarkTypeOf(v); return err }},
	}
	for _, tt := range tests {
		if err := tt.call(tt.value); err == nil {
			t.Errorf("%s was accepted", tt.name)
		}
	}

	// A key is fixed width on both sides.
	if _, err := NexmarkKeyID([]byte{1, 2, 3}); err == nil {
		t.Error("a three-byte key was accepted")
	}
	if got, err := NexmarkKeyID(NexmarkKey(1234)); err != nil || got != 1234 {
		t.Errorf("NexmarkKey/NexmarkKeyID round-tripped 1234 to (%d, %v)", got, err)
	}
}

// TestNexmarkOpenRejectsBadConfig. Every guard, one row each, because a config
// that is accepted and then divided by produces a panic several frames from
// the cause.
func TestNexmarkOpenRejectsBadConfig(t *testing.T) {
	tests := []struct {
		name   string
		break_ func(*NexmarkConfig)
	}{
		{"zero count", func(c *NexmarkConfig) { c.Count = 0 }},
		{"negative count", func(c *NexmarkConfig) { c.Count = -1 }},
		{"zero auction cardinality", func(c *NexmarkConfig) { c.AuctionCardinality = 0 }},
		{"zero person cardinality", func(c *NexmarkConfig) { c.PersonCardinality = 0 }},
		{"zero price range", func(c *NexmarkConfig) { c.PriceRange = 0 }},
		{"price range that lets a reserve wrap", func(c *NexmarkConfig) { c.PriceRange = math.MaxInt64 }},
		{"zero category count", func(c *NexmarkConfig) { c.CategoryCount = 0 }},
		{"zero auction duration", func(c *NexmarkConfig) { c.AuctionDuration = 0 }},
		{"negative lag", func(c *NexmarkConfig) { c.MaxLag = -1 }},
	}
	for _, tt := range tests {
		cfg := nexmarkTestConfig(1)
		tt.break_(&cfg)
		if err := NewNexmark(cfg).Open(nil); err == nil {
			t.Errorf("%s was accepted", tt.name)
		}
	}
	if err := NewNexmark(nexmarkTestConfig(1)).Open(nil); err != nil {
		t.Errorf("a good config was refused: %v", err)
	}
}

// TestNexmarkSeekToRejectsNegativeOffset.
func TestNexmarkSeekToRejectsNegativeOffset(t *testing.T) {
	src := openNexmark(t, nexmarkTestConfig(1))
	if err := src.SeekTo(-1); err == nil {
		t.Error("SeekTo(-1) was accepted")
	}
	if got := src.Position(); got != 0 {
		t.Errorf("a refused seek moved the position to %d", got)
	}
}

// TestNexmarkFieldsStayInTheirConfiguredSpaces.
func TestNexmarkFieldsStayInTheirConfiguredSpaces(t *testing.T) {
	cfg := nexmarkTestConfig(17)
	src := openNexmark(t, cfg)
	for i, rec := range readNexmark(t, src, int(cfg.Count)) {
		typ, err := NexmarkTypeOf(rec.Value)
		if err != nil {
			t.Fatalf("NexmarkTypeOf: %v", err)
		}
		switch typ {
		case EventPerson:
			p, _ := DecodePerson(rec.Value)
			if p.ID >= uint64(cfg.PersonCardinality) {
				t.Fatalf("element %d: person id %d outside [0, %d)", i, p.ID, cfg.PersonCardinality)
			}
		case EventAuction:
			a, _ := DecodeAuction(rec.Value)
			switch {
			case a.ID >= uint64(cfg.AuctionCardinality):
				t.Fatalf("element %d: auction id %d outside [0, %d)", i, a.ID, cfg.AuctionCardinality)
			case a.Seller >= uint64(cfg.PersonCardinality):
				t.Fatalf("element %d: seller %d outside [0, %d)", i, a.Seller, cfg.PersonCardinality)
			case a.Category >= uint64(cfg.CategoryCount):
				t.Fatalf("element %d: category %d outside [0, %d)", i, a.Category, cfg.CategoryCount)
			case a.InitialBid >= uint64(cfg.PriceRange):
				t.Fatalf("element %d: initial bid %d outside [0, %d)", i, a.InitialBid, cfg.PriceRange)
			case a.Expires > a.DateTime+cfg.AuctionDuration:
				t.Fatalf("element %d: expires %d is more than %d after %d", i, a.Expires, cfg.AuctionDuration, a.DateTime)
			}
		case EventBid:
			b, _ := DecodeBid(rec.Value)
			switch {
			case b.Auction >= uint64(cfg.AuctionCardinality):
				t.Fatalf("element %d: bid auction %d outside [0, %d)", i, b.Auction, cfg.AuctionCardinality)
			case b.Bidder >= uint64(cfg.PersonCardinality):
				t.Fatalf("element %d: bidder %d outside [0, %d)", i, b.Bidder, cfg.PersonCardinality)
			case b.Price >= uint64(cfg.PriceRange):
				t.Fatalf("element %d: price %d outside [0, %d)", i, b.Price, cfg.PriceRange)
			}
		}
	}
}

// TestNexmarkAtMatchesNext. At is where the purity lives and Next is a call to
// it; this says the two are the same sequence rather than two derivations.
func TestNexmarkAtMatchesNext(t *testing.T) {
	cfg := nexmarkTestConfig(23)
	src := openNexmark(t, cfg)
	direct := NewNexmark(cfg)
	for i, rec := range readNexmark(t, src, 400) {
		if !equalNexmarkRecords(rec, direct.At(int64(i))) {
			t.Fatalf("At(%d) and the %dth Next disagree", i, i)
		}
	}
	// At does not move the position, so a source that has only been asked
	// about elements still starts at zero.
	if got := direct.Position(); got != 0 {
		t.Errorf("At moved the position to %d", got)
	}
}

// TestNexmarkValueErrorsAreOneKind lets a caller tell a layout disagreement
// from any other failure.
func TestNexmarkValueErrorsAreOneKind(t *testing.T) {
	_, err := DecodeBid(nil)
	var ve *nexmarkValueError
	if !errors.As(err, &ve) {
		t.Errorf("a malformed value reported %v, which is not a value error", err)
	}
}
