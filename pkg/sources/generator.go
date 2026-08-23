// Package sources holds Source implementations.
package sources

import (
	"encoding/binary"
	"fmt"

	"github.com/AarinB1/tidemark/pkg/core"
)

// GeneratorConfig parameterises a Generator. The same config and the same seed
// always describe the same finite sequence of records.
//
// # Value layout
//
// Value is a fixed binary record, not opaque bytes:
//
//	bytes 0..7    amount, big-endian uint64, drawn from [0, AmountRange)
//	bytes 8..end  padding derived from (Seed, n), out to ValueSize
//
// The layout is documented here, on the config, because more than one place
// decodes it and they are in different packages: an aggregating operator reads
// the amount out of a record, and the batch oracle reads it out again
// independently to say what that operator should have produced. If the two ever
// disagree about where the amount lives, the symptom is not a decode error. It
// is a sum that is wrong, which is the hardest kind of bug this engine has.
//
// The number lives in Value rather than in a new field on core.Record. Nexmark
// records carry several numbers at once (auction id, bidder, price), so one
// typed field would not be enough and a second would not settle it either; and
// Phase 7 has to serialise a Record across a process boundary regardless, at
// which point a documented fixed binary layout is exactly the thing that has to
// exist. Settling it now rather than in Phase 6 keeps it out of the snapshot
// format Phase 3b is about to write and out of the restore path that would
// follow it.
type GeneratorConfig struct {
	Seed           uint64
	Count          int64 // number of records; must be > 0 in Phase 0
	KeyCardinality int64 // distinct keys; drives Phase 6 state size
	BaseEventTime  int64 // millis since the Unix epoch
	EventTimeStep  int64 // millis of event time per offset
	MaxLag         int64 // millis of bounded out-of-orderness
	ValueSize      int   // bytes; must be >= 8, which the amount occupies
	AmountRange    int64 // amounts are drawn from [0, AmountRange); must be >= 1
}

// Salts separating the three derived streams. Independent streams come from
// salting the seed rather than from advancing a counter, because advancing a
// counter makes element n depend on how many elements were drawn before it,
// which is exactly what SeekTo must not have to reconstruct.
const (
	keySalt    = 0x1
	valueSalt  = 0x2
	timeSalt   = 0x3
	amountSalt = 0x4
)

// Generator is a seekable, deterministic source.
//
// Element n is a pure function of (Seed, n). It holds no generator state, only
// a position, so SeekTo is an assignment and recovery from a checkpointed offset
// replays exactly the records the failed run would have produced. A source
// built around a held *rand.Rand cannot offer that: its output depends on how
// many values have been drawn, not on which element is being asked for.
type Generator struct {
	cfg GeneratorConfig
	pos int64
}

var _ core.Source = (*Generator)(nil)

// NewGenerator returns a generator positioned at offset 0. The config is
// checked by Open.
func NewGenerator(cfg GeneratorConfig) *Generator {
	return &Generator{cfg: cfg}
}

// Open validates the config. It does not touch the position, so a SeekTo before
// Open survives.
func (g *Generator) Open(ctx core.Context) error {
	switch {
	case g.cfg.Count <= 0:
		return fmt.Errorf("generator: Count is %d, must be > 0 in Phase 0", g.cfg.Count)
	case g.cfg.KeyCardinality <= 0:
		return fmt.Errorf("generator: KeyCardinality is %d, must be > 0", g.cfg.KeyCardinality)
	case g.cfg.MaxLag < 0:
		return fmt.Errorf("generator: MaxLag is %d, must be >= 0", g.cfg.MaxLag)
	case g.cfg.ValueSize < 8:
		return fmt.Errorf("generator: ValueSize is %d, must be >= 8: the amount occupies the first 8 bytes of Value", g.cfg.ValueSize)
	case g.cfg.AmountRange < 1:
		return fmt.Errorf("generator: AmountRange is %d, must be >= 1", g.cfg.AmountRange)
	}
	return nil
}

// Next returns the record at the current offset and advances.
func (g *Generator) Next() (*core.Record, bool, error) {
	if g.pos >= g.cfg.Count {
		return nil, false, nil
	}
	n := g.pos
	g.pos++
	return &core.Record{
		Key:       g.keyOf(n),
		Value:     g.valueOf(n),
		EventTime: g.timeOf(n),
	}, true, nil
}

// SeekTo positions the generator at offset. There is no accumulated state to
// discard: the offset is the whole of the source's state.
func (g *Generator) SeekTo(offset int64) error {
	if offset < 0 {
		return fmt.Errorf("generator: seek to negative offset %d", offset)
	}
	g.pos = offset
	return nil
}

// Position returns the offset of the element the next Next will return.
func (g *Generator) Position() int64 { return g.pos }

// Count returns the number of elements the generator produces from offset 0.
// It is what lets a parallel source vertex divide the offset space into one
// contiguous range per subtask.
func (g *Generator) Count() int64 { return g.cfg.Count }

// Close releases nothing; the generator holds no resources.
func (g *Generator) Close() error { return nil }

// keyOf returns the key of element n as a fixed-width big-endian integer.
// Fixed width and byte-ordered so that Phase 1's partition hash and Phase 3's
// state keys stay stable: a variable-width or little-endian encoding would make
// the same logical key sort and hash differently across changes.
func (g *Generator) keyOf(n int64) []byte {
	k := mix(g.cfg.Seed^keySalt, uint64(n)) % uint64(g.cfg.KeyCardinality)
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], k)
	return buf[:]
}

// valueOf returns ValueSize bytes in the layout documented on GeneratorConfig:
// the amount in the first eight, then padding out to ValueSize.
//
// The padding starts at byte 8 rather than being written from byte 0 and partly
// overwritten, and it is derived from its own salt. That is what makes the
// amount independent of ValueSize: element n of a given seed decodes to the
// same amount whether the record carries 8 bytes or 64. A layout where the
// padding decided the amount would make the aggregate a function of a size
// knob, and every comparison against the oracle would then depend on the two
// agreeing about that knob as well as about the arithmetic.
func (g *Generator) valueOf(n int64) []byte {
	v := make([]byte, g.cfg.ValueSize)
	binary.BigEndian.PutUint64(v[:8], g.amountOf(n))
	base := mix(g.cfg.Seed^valueSalt, uint64(n))
	var buf [8]byte
	for i := 8; i < len(v); i += 8 {
		binary.BigEndian.PutUint64(buf[:], mix(base, uint64(i/8)))
		copy(v[i:], buf[:])
	}
	return v
}

// amountOf returns the amount of element n: a pure function of (Seed, n) in
// [0, AmountRange), derived from its own salt like every other field.
//
// It is a separate stream rather than a slice of the value stream so that
// nothing about the padding can move it. Aggregation over these amounts is what
// Nexmark q5 and q7 need, and what the window operator's count is a sum of ones
// standing in for until Phase 6.
func (g *Generator) amountOf(n int64) uint64 {
	return mix(g.cfg.Seed^amountSalt, uint64(n)) % uint64(g.cfg.AmountRange)
}

// timeOf returns the event time of element n: the in-order time for that offset
// pulled back by a lag in [0, MaxLag].
//
// Out-of-orderness is bounded by exactly MaxLag, which is the property Phase 2's
// watermark generator is tested against. Unbounded lateness would leave that
// contract with nothing to assert.
func (g *Generator) timeOf(n int64) int64 {
	lag := int64(mix(g.cfg.Seed^timeSalt, uint64(n)) % uint64(g.cfg.MaxLag+1))
	return g.cfg.BaseEventTime + n*g.cfg.EventTimeStep - lag
}

// mix is splitmix64 applied to (seed, n). It is written out here rather than
// taken from math/rand because a held *rand.Rand cannot be indexed, and rather
// than hash/maphash because maphash randomises its seed per process, which
// destroys reproducibility across runs without failing any obvious test.
func mix(seed, n uint64) uint64 {
	z := seed + (n+1)*0x9E3779B97F4A7C15
	z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
	z = (z ^ (z >> 27)) * 0x94D049BB133111EB
	return z ^ (z >> 31)
}
