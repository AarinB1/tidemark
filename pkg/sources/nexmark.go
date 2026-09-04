package sources

import (
	"encoding/binary"
	"fmt"

	"github.com/AarinB1/tidemark/pkg/core"
)

// The Nexmark event model.
//
// This is a SECOND source beside Generator rather than a mode flag on it.
// Generator produces one shape -- (key, amount, padding) -- and every field of
// it is read by position out of a value whose only variable is its length.
// Nexmark produces three shapes with different field counts, and a record's
// shape is decided by its offset. A flag on GeneratorConfig would make every
// existing decode site branch on a config value it does not hold, and the
// failure of getting that wrong is a number read out of the wrong eight bytes:
// a plausible amount, not a decode error.
//
// # Value layout
//
// One byte of type discriminator, then fixed-width big-endian fields. No
// separators, no lengths: every event type has a fixed size, so the type byte
// decides how many bytes follow and where each field starts.
//
//	Person  (0x01), 41 bytes
//	  0        type, 0x01
//	  1..8     id           uint64
//	  9..16    name offset  uint64
//	  17..24   city offset  uint64
//	  25..32   state offset uint64
//	  33..40   dateTime     int64
//
//	Auction (0x02), 57 bytes
//	  0        type, 0x02
//	  1..8     id           uint64
//	  9..16    seller       uint64
//	  17..24   category     uint64
//	  25..32   initialBid   uint64
//	  33..40   reserve      uint64
//	  41..48   dateTime     int64
//	  49..56   expires      int64
//
//	Bid     (0x03), 33 bytes
//	  0        type, 0x03
//	  1..8     auction      uint64
//	  9..16    bidder       uint64
//	  17..24   price        uint64
//	  25..32   dateTime     int64
//
// The layout is written down ONCE, here, and the encoders and decoders below
// are the only code that knows the offsets. The source builds its values with
// EncodePerson, EncodeAuction and EncodeBid; the queries and the batch oracle
// read them back with DecodePerson, DecodeAuction and DecodeBid. Nothing
// indexes into a value by hand.
//
// That matters more here than it did for Generator. A query and its oracle
// disagreeing about where "price" lives does not fail to decode -- both read
// eight well-formed bytes -- it produces a maximum that is wrong, in a query
// whose whole purpose is to be compared against that oracle. The symptom would
// be a windowing bug that is not one.
//
// dateTime is the event's own timestamp and is EXACTLY core.Record.EventTime.
// It is carried in the value as well because a downstream stage that re-emits
// an event (q1 rewrites a bid's price) must not have to reconstruct it, and
// because the oracle reads events without a core.Record around them.
//
// Big-endian and fixed-width for the same reason Generator's amount is: a
// value that sorts the way it compares stays stable across changes, and
// reflection-based serialisation is out of scope anywhere on the data path.
//
// # Record.Key
//
// Key is what a query partitions on, so it is the id the queries group by:
//
//	Person   the person id
//	Auction  the auction id
//	Bid      the AUCTION id, not the bidder
//
// Bids key on their auction because q5 and q7 both group that way: q5 counts
// bids per auction and q7 takes a maximum per auction. Keying on the bidder
// would put one auction's bids on every subtask and neither query could be
// computed without a second shuffle.
//
// Eight big-endian bytes in every case; see NexmarkKey.

// NexmarkEventType is the one-byte discriminator that leads every value.
type NexmarkEventType uint8

const (
	// EventPerson, EventAuction and EventBid are the three event types. The
	// values are explicit rather than iota-from-zero so that a zero byte --
	// which is what an empty or truncated value decodes to -- is not a valid
	// type.
	EventPerson  NexmarkEventType = 0x01
	EventAuction NexmarkEventType = 0x02
	EventBid     NexmarkEventType = 0x03
)

// String renders the type for error messages and test names.
func (t NexmarkEventType) String() string {
	switch t {
	case EventPerson:
		return "Person"
	case EventAuction:
		return "Auction"
	case EventBid:
		return "Bid"
	default:
		return "Unknown"
	}
}

// Widths of the three encodings and of the pieces they are built from.
const (
	// nexmarkTypeBytes is the discriminator.
	nexmarkTypeBytes = 1
	// NexmarkFieldBytes is the width of every field after the discriminator.
	// Uniform, so a field's offset is its index and nothing has to be counted.
	NexmarkFieldBytes = 8

	// NexmarkPersonBytes, NexmarkAuctionBytes and NexmarkBidBytes are the
	// encoded sizes. Exported because a test that asserts the layout has to be
	// able to name them without recomputing them, which would be the layout
	// agreeing with itself.
	NexmarkPersonBytes  = nexmarkTypeBytes + 5*NexmarkFieldBytes
	NexmarkAuctionBytes = nexmarkTypeBytes + 7*NexmarkFieldBytes
	NexmarkBidBytes     = nexmarkTypeBytes + 4*NexmarkFieldBytes

	// NexmarkKeyBytes is the width of a Record.Key: one big-endian id.
	NexmarkKeyBytes = NexmarkFieldBytes
)

// Person is a Nexmark person event.
//
// The three offsets stand in for the name, city and state strings the Nexmark
// specification carries. They are offsets rather than strings because nothing
// in this phase reads them and a variable-length field would put a length
// prefix into a layout that otherwise needs none; a later phase that wants the
// strings resolves an offset against a table it brings with it.
type Person struct {
	ID          uint64
	NameOffset  uint64
	CityOffset  uint64
	StateOffset uint64
	DateTime    int64
}

// Auction is a Nexmark auction event.
type Auction struct {
	ID         uint64
	Seller     uint64
	Category   uint64
	InitialBid uint64
	Reserve    uint64
	DateTime   int64
	Expires    int64
}

// Bid is a Nexmark bid event.
type Bid struct {
	Auction  uint64
	Bidder   uint64
	Price    uint64
	DateTime int64
}

// errNexmarkValue is what every malformed value reports through, so a caller
// can tell a layout disagreement from any other failure with errors.Is.
type nexmarkValueError struct{ msg string }

func (e *nexmarkValueError) Error() string { return e.msg }

// valueErrorf returns a layout error. Every decoder below reports through it,
// so the message always says what was expected and what was there.
func valueErrorf(format string, args ...any) error {
	return &nexmarkValueError{msg: "sources: nexmark: " + fmt.Sprintf(format, args...)}
}

// NexmarkKey renders an id as the eight big-endian bytes a Record.Key holds.
//
// Fixed width and byte-ordered so that partitioning, state keys and sorted
// iteration all see one encoding. A variable-width id would make two keys of
// different lengths compare in an order that has nothing to do with the numbers.
func NexmarkKey(id uint64) []byte {
	var buf [NexmarkKeyBytes]byte
	binary.BigEndian.PutUint64(buf[:], id)
	return buf[:]
}

// NexmarkKeyID reads an id back out of a Record.Key.
func NexmarkKeyID(key []byte) (uint64, error) {
	if len(key) != NexmarkKeyBytes {
		return 0, valueErrorf("a key is %d bytes, want %d", len(key), NexmarkKeyBytes)
	}
	return binary.BigEndian.Uint64(key), nil
}

// NexmarkTypeOf reads the discriminator off a value.
func NexmarkTypeOf(value []byte) (NexmarkEventType, error) {
	if len(value) < nexmarkTypeBytes {
		return 0, valueErrorf("a value of %d bytes carries no type discriminator", len(value))
	}
	switch t := NexmarkEventType(value[0]); t {
	case EventPerson, EventAuction, EventBid:
		return t, nil
	default:
		return 0, valueErrorf("type discriminator %#x is not one of %#x, %#x, %#x",
			value[0], byte(EventPerson), byte(EventAuction), byte(EventBid))
	}
}

// field reads the i'th fixed-width field of a value, counting from zero after
// the discriminator. Every decoder goes through it, so the offset arithmetic
// exists once.
func field(value []byte, i int) uint64 {
	off := nexmarkTypeBytes + i*NexmarkFieldBytes
	return binary.BigEndian.Uint64(value[off : off+NexmarkFieldBytes])
}

// putField writes the i'th fixed-width field. The inverse of field, directly
// beside it so the two cannot drift.
func putField(value []byte, i int, v uint64) {
	off := nexmarkTypeBytes + i*NexmarkFieldBytes
	binary.BigEndian.PutUint64(value[off:off+NexmarkFieldBytes], v)
}

// checkValue confirms a value is the encoding of want at exactly size bytes.
//
// Exactly, not at least. A Person read as a Bid would otherwise decode: the
// first four fields are in range and the answer is three plausible numbers.
func checkValue(value []byte, want NexmarkEventType, size int) error {
	got, err := NexmarkTypeOf(value)
	if err != nil {
		return err
	}
	if got != want {
		return valueErrorf("value is a %s, want a %s", got, want)
	}
	if len(value) != size {
		return valueErrorf("a %s is %d bytes, want %d", want, len(value), size)
	}
	return nil
}

// EncodePerson renders p in the layout at the top of this file.
func EncodePerson(p Person) []byte {
	v := make([]byte, NexmarkPersonBytes)
	v[0] = byte(EventPerson)
	putField(v, 0, p.ID)
	putField(v, 1, p.NameOffset)
	putField(v, 2, p.CityOffset)
	putField(v, 3, p.StateOffset)
	putField(v, 4, uint64(p.DateTime))
	return v
}

// DecodePerson is the inverse of EncodePerson.
func DecodePerson(value []byte) (Person, error) {
	if err := checkValue(value, EventPerson, NexmarkPersonBytes); err != nil {
		return Person{}, err
	}
	return Person{
		ID:          field(value, 0),
		NameOffset:  field(value, 1),
		CityOffset:  field(value, 2),
		StateOffset: field(value, 3),
		DateTime:    int64(field(value, 4)),
	}, nil
}

// EncodeAuction renders a in the layout at the top of this file.
func EncodeAuction(a Auction) []byte {
	v := make([]byte, NexmarkAuctionBytes)
	v[0] = byte(EventAuction)
	putField(v, 0, a.ID)
	putField(v, 1, a.Seller)
	putField(v, 2, a.Category)
	putField(v, 3, a.InitialBid)
	putField(v, 4, a.Reserve)
	putField(v, 5, uint64(a.DateTime))
	putField(v, 6, uint64(a.Expires))
	return v
}

// DecodeAuction is the inverse of EncodeAuction.
func DecodeAuction(value []byte) (Auction, error) {
	if err := checkValue(value, EventAuction, NexmarkAuctionBytes); err != nil {
		return Auction{}, err
	}
	return Auction{
		ID:         field(value, 0),
		Seller:     field(value, 1),
		Category:   field(value, 2),
		InitialBid: field(value, 3),
		Reserve:    field(value, 4),
		DateTime:   int64(field(value, 5)),
		Expires:    int64(field(value, 6)),
	}, nil
}

// EncodeBid renders b in the layout at the top of this file.
func EncodeBid(b Bid) []byte {
	v := make([]byte, NexmarkBidBytes)
	v[0] = byte(EventBid)
	putField(v, 0, b.Auction)
	putField(v, 1, b.Bidder)
	putField(v, 2, b.Price)
	putField(v, 3, uint64(b.DateTime))
	return v
}

// DecodeBid is the inverse of EncodeBid.
func DecodeBid(value []byte) (Bid, error) {
	if err := checkValue(value, EventBid, NexmarkBidBytes); err != nil {
		return Bid{}, err
	}
	return Bid{
		Auction:  field(value, 0),
		Bidder:   field(value, 1),
		Price:    field(value, 2),
		DateTime: int64(field(value, 3)),
	}, nil
}

// The event mix, as the standard Nexmark proportions.
//
// One person to three auctions to forty-six bids, in a cycle of fifty. The
// ratio is EXACT rather than probabilistic: element n is a person when
// n mod 50 is 0, an auction when it is 1, 2 or 3, and a bid otherwise. Drawing
// the type from the seed instead would make the mix a sample, so a run of ten
// thousand elements would hold "about" three hundred auctions and a test that
// asserted the count would have to assert a range.
//
// It also keeps the type a function of the OFFSET alone, which is what SeekTo
// needs: seeking to k and reading gives the same types as reading from zero,
// with no cycle position to reconstruct.
const (
	nexmarkPersonsPerCycle  = 1
	nexmarkAuctionsPerCycle = 3
	nexmarkBidsPerCycle     = 46
	nexmarkEventsPerCycle   = nexmarkPersonsPerCycle + nexmarkAuctionsPerCycle + nexmarkBidsPerCycle
)

// The spaces the person's three string offsets are drawn from.
//
// Constants rather than configuration because no query in this phase reads
// them, and a knob nothing turns is a knob that is wrong the first time
// somebody does turn it. NexmarkConfig carries the dials the queries and Phase
// 6b's state-size target actually depend on and no others.
const (
	nexmarkNameSpace  = 1000
	nexmarkCitySpace  = 100
	nexmarkStateSpace = 50
)

// Salts separating the derived streams, disjoint from Generator's.
//
// One salt per FIELD, so no field's value can move when another one's
// derivation changes, and none of them depends on how many draws preceded it.
// Same reasoning as Generator's four: a counter advanced per draw makes element
// n a function of everything before it, which is exactly what SeekTo must not
// have to reconstruct.
const (
	nexmarkTimeSalt       = 0x10
	nexmarkPersonIDSalt   = 0x11
	nexmarkNameSalt       = 0x12
	nexmarkCitySalt       = 0x13
	nexmarkStateSalt      = 0x14
	nexmarkAuctionIDSalt  = 0x15
	nexmarkSellerSalt     = 0x16
	nexmarkCategorySalt   = 0x17
	nexmarkInitialBidSalt = 0x18
	nexmarkReserveSalt    = 0x19
	nexmarkExpiresSalt    = 0x1A
	nexmarkBidAuctionSalt = 0x1B
	nexmarkBidderSalt     = 0x1C
	nexmarkPriceSalt      = 0x1D
)

// maxNexmarkPriceRange bounds PriceRange so that an auction's reserve, which is
// its initial bid plus a second draw, cannot wrap. A wrapped reserve is a
// small number where a large one belongs and nothing reports it.
const maxNexmarkPriceRange = int64(1) << 62

// NexmarkConfig parameterises a Nexmark source. The same config and the same
// seed always describe the same finite sequence of events.
//
// The field layout the records carry is documented at the top of this file, on
// the package, because the source writes it and the batch oracle reads it and
// they are in different packages.
type NexmarkConfig struct {
	Seed  uint64
	Count int64 // number of events; must be > 0

	// AuctionCardinality is the auction id space. It is the dial that decides
	// how many distinct keys a windowed query holds state for, so it is what
	// Phase 6b turns to reach a state-size target -- the same role
	// GeneratorConfig.KeyCardinality plays for the other source.
	//
	// It also decides whether windowed aggregation is interesting at all: bids
	// key on their auction, so an id space larger than the bid count gives
	// every window one bid per auction and a maximum with nothing to compare
	// against.
	AuctionCardinality int64
	// PersonCardinality is the person id space, which bidders and sellers are
	// drawn from. q7 breaks ties on the bidder, so a space of one would make
	// every tie fall through to the auction id and leave that rule untested.
	PersonCardinality int64
	// PriceRange bounds prices, initial bids and reserves: each is drawn from
	// [0, PriceRange). Must be >= 1, and small enough that a reserve cannot
	// wrap; see maxNexmarkPriceRange.
	PriceRange int64
	// CategoryCount is the auction category space; must be >= 1.
	CategoryCount int64
	// AuctionDuration bounds how long after its dateTime an auction expires:
	// the gap is drawn from [1, AuctionDuration]. Must be >= 1.
	AuctionDuration int64

	BaseEventTime int64 // millis since the Unix epoch
	EventTimeStep int64 // millis of event time per offset
	MaxLag        int64 // millis of bounded out-of-orderness
}

// Nexmark is a seekable, deterministic Nexmark event source.
//
// Element n is a pure function of (Seed, n), exactly as Generator's is. It
// holds a position and nothing else, so SeekTo is an assignment and a run
// resumed from a checkpointed offset replays the records the failed run would
// have produced. Every contract Generator holds is held here and none of them
// is optional; see the tests, which assert each one against this type rather
// than inheriting it from the other source.
type Nexmark struct {
	cfg NexmarkConfig
	pos int64
}

var _ core.Source = (*Nexmark)(nil)

// NewNexmark returns a source positioned at offset 0. The config is checked by
// Open.
func NewNexmark(cfg NexmarkConfig) *Nexmark { return &Nexmark{cfg: cfg} }

// Open validates the config. It does not touch the position, so a SeekTo before
// Open survives.
func (n *Nexmark) Open(ctx core.Context) error {
	switch {
	case n.cfg.Count <= 0:
		return fmt.Errorf("nexmark: Count is %d, must be > 0", n.cfg.Count)
	case n.cfg.AuctionCardinality <= 0:
		return fmt.Errorf("nexmark: AuctionCardinality is %d, must be > 0", n.cfg.AuctionCardinality)
	case n.cfg.PersonCardinality <= 0:
		return fmt.Errorf("nexmark: PersonCardinality is %d, must be > 0", n.cfg.PersonCardinality)
	case n.cfg.PriceRange < 1:
		return fmt.Errorf("nexmark: PriceRange is %d, must be >= 1", n.cfg.PriceRange)
	case n.cfg.PriceRange > maxNexmarkPriceRange:
		return fmt.Errorf("nexmark: PriceRange is %d, must be <= %d so that a reserve cannot wrap",
			n.cfg.PriceRange, maxNexmarkPriceRange)
	case n.cfg.CategoryCount < 1:
		return fmt.Errorf("nexmark: CategoryCount is %d, must be >= 1", n.cfg.CategoryCount)
	case n.cfg.AuctionDuration < 1:
		return fmt.Errorf("nexmark: AuctionDuration is %d, must be >= 1", n.cfg.AuctionDuration)
	case n.cfg.MaxLag < 0:
		return fmt.Errorf("nexmark: MaxLag is %d, must be >= 0", n.cfg.MaxLag)
	}
	return nil
}

// Next returns the event at the current offset and advances.
func (n *Nexmark) Next() (*core.Record, bool, error) {
	if n.pos >= n.cfg.Count {
		return nil, false, nil
	}
	rec := n.At(n.pos)
	n.pos++
	return rec, true, nil
}

// SeekTo positions the source at offset. There is no accumulated state to
// discard: the offset is the whole of the source's state.
func (n *Nexmark) SeekTo(offset int64) error {
	if offset < 0 {
		return fmt.Errorf("nexmark: seek to negative offset %d", offset)
	}
	n.pos = offset
	return nil
}

// Position returns the offset of the element the next Next will return.
func (n *Nexmark) Position() int64 { return n.pos }

// Count returns the number of elements produced from offset 0.
//
// It is not optional and it is not an optimisation. The runtime divides a
// source vertex's offset space into one contiguous range per subtask and
// REFUSES parallelism above 1 for a source that does not report a Count, so a
// source without it silently runs single-input and every test built on it that
// claims to exercise several inputs is claiming something else. That is the
// decorator rule in CLAUDE.md, and it applies to the source itself first.
func (n *Nexmark) Count() int64 { return n.cfg.Count }

// Close releases nothing; the source holds no resources.
func (n *Nexmark) Close() error { return nil }

// TypeAt returns the type of element i.
//
// A function of the OFFSET alone, with no dependence on the seed: see the
// proportions above. Exported so a test can say what it expects at an offset
// without re-deriving the cycle.
func (n *Nexmark) TypeAt(i int64) NexmarkEventType {
	switch phase := floorMod64(i, nexmarkEventsPerCycle); {
	case phase < nexmarkPersonsPerCycle:
		return EventPerson
	case phase < nexmarkPersonsPerCycle+nexmarkAuctionsPerCycle:
		return EventAuction
	default:
		return EventBid
	}
}

// At returns element i without moving the position.
//
// This is where "element n is a pure function of (Seed, n)" is written down.
// Next is a call to it and an increment, so there is no second derivation that
// could disagree with this one, and SeekTo cannot skip a side effect because
// there is none to skip.
func (n *Nexmark) At(i int64) *core.Record {
	eventTime := n.timeOf(i)
	switch n.TypeAt(i) {
	case EventPerson:
		p := Person{
			ID:          n.draw(nexmarkPersonIDSalt, i, n.cfg.PersonCardinality),
			NameOffset:  n.draw(nexmarkNameSalt, i, nexmarkNameSpace),
			CityOffset:  n.draw(nexmarkCitySalt, i, nexmarkCitySpace),
			StateOffset: n.draw(nexmarkStateSalt, i, nexmarkStateSpace),
			DateTime:    eventTime,
		}
		return &core.Record{Key: NexmarkKey(p.ID), Value: EncodePerson(p), EventTime: eventTime}
	case EventAuction:
		initial := n.draw(nexmarkInitialBidSalt, i, n.cfg.PriceRange)
		a := Auction{
			ID:         n.draw(nexmarkAuctionIDSalt, i, n.cfg.AuctionCardinality),
			Seller:     n.draw(nexmarkSellerSalt, i, n.cfg.PersonCardinality),
			Category:   n.draw(nexmarkCategorySalt, i, n.cfg.CategoryCount),
			InitialBid: initial,
			// The reserve is at or above the initial bid, which is what a
			// reserve means. Two draws added rather than one drawn from a range
			// that depends on the first, so neither field moves when the other
			// one's derivation changes. PriceRange is capped so the sum cannot
			// wrap; see Open.
			Reserve:  initial + n.draw(nexmarkReserveSalt, i, n.cfg.PriceRange),
			DateTime: eventTime,
			// Expires is at least one millisecond after the event, saturating.
			// An auction near MaxInt64 that expired before it opened would be
			// a plausible-looking number rather than an error.
			Expires: addCeil64(eventTime, 1+int64(n.draw(nexmarkExpiresSalt, i, n.cfg.AuctionDuration))),
		}
		return &core.Record{Key: NexmarkKey(a.ID), Value: EncodeAuction(a), EventTime: eventTime}
	default:
		b := Bid{
			Auction:  n.draw(nexmarkBidAuctionSalt, i, n.cfg.AuctionCardinality),
			Bidder:   n.draw(nexmarkBidderSalt, i, n.cfg.PersonCardinality),
			Price:    n.draw(nexmarkPriceSalt, i, n.cfg.PriceRange),
			DateTime: eventTime,
		}
		// Keyed on the AUCTION, not the bidder. See the note on Record.Key at
		// the top of this file.
		return &core.Record{Key: NexmarkKey(b.Auction), Value: EncodeBid(b), EventTime: eventTime}
	}
}

// draw returns one field of element i: splitmix64 over the salted seed, reduced
// into [0, space). space is positive at every call site, checked by Open or
// fixed as a constant above.
func (n *Nexmark) draw(salt uint64, i int64, space int64) uint64 {
	return mix(n.cfg.Seed^salt, uint64(i)) % uint64(space)
}

// timeOf returns the event time of element i: the in-order time for that offset
// pulled back by a lag in [0, MaxLag].
//
// Identical in shape to Generator.timeOf and for the same reason. The
// out-of-orderness is bounded by exactly MaxLag, so a source vertex configured
// with MaxOutOfOrderness equal to it emits a watermark no record can be late
// against, and every equivalence comparison against a batch oracle with no
// lateness model is valid rather than approximately valid.
func (n *Nexmark) timeOf(i int64) int64 {
	lag := int64(mix(n.cfg.Seed^nexmarkTimeSalt, uint64(i)) % uint64(n.cfg.MaxLag+1))
	return n.cfg.BaseEventTime + i*n.cfg.EventTimeStep - lag
}

// floorMod64 returns a mod b for b > 0, always in [0, b).
//
// Offsets are non-negative, so this is the identity in every reachable call --
// but TypeAt is exported and a caller handing it a negative offset would get a
// negative phase and fall into the bid branch, which is a plausible answer
// rather than an obviously wrong one. Go's % takes the sign of the dividend;
// this does not.
func floorMod64(a, b int64) int64 {
	m := a % b
	if m < 0 {
		m += b
	}
	return m
}

// addCeil64 returns a + b clamped to MaxInt64 rather than wrapping, for
// non-negative b. An auction's expiry is the only caller.
func addCeil64(a, b int64) int64 {
	s := a + b
	if b > 0 && s < a {
		return 1<<63 - 1
	}
	return s
}
