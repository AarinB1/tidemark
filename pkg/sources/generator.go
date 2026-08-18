// Package sources holds Source implementations.
package sources

import (
	"encoding/binary"
	"fmt"

	"github.com/AarinB1/tidemark/pkg/core"
)

// GeneratorConfig parameterises a Generator. The same config and the same seed
// always describe the same finite sequence of records.
type GeneratorConfig struct {
	Seed           uint64
	Count          int64 // number of records; must be > 0 in Phase 0
	KeyCardinality int64 // distinct keys; drives Phase 6 state size
	BaseEventTime  int64 // millis since the Unix epoch
	EventTimeStep  int64 // millis of event time per offset
	MaxLag         int64 // millis of bounded out-of-orderness
	ValueSize      int   // bytes
}

// Salts separating the three derived streams. Independent streams come from
// salting the seed rather than from advancing a counter, because advancing a
// counter makes element n depend on how many elements were drawn before it,
// which is exactly what Seek must not have to reconstruct.
const (
	keySalt   = 0x1
	valueSalt = 0x2
	timeSalt  = 0x3
)

// Generator is a seekable, deterministic source.
//
// Element n is a pure function of (Seed, n). It holds no generator state, only
// a position, so Seek is an assignment and recovery from a checkpointed offset
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

// Open validates the config. It does not touch the position, so a Seek before
// Open survives.
func (g *Generator) Open(ctx core.Context) error {
	switch {
	case g.cfg.Count <= 0:
		return fmt.Errorf("generator: Count is %d, must be > 0 in Phase 0", g.cfg.Count)
	case g.cfg.KeyCardinality <= 0:
		return fmt.Errorf("generator: KeyCardinality is %d, must be > 0", g.cfg.KeyCardinality)
	case g.cfg.MaxLag < 0:
		return fmt.Errorf("generator: MaxLag is %d, must be >= 0", g.cfg.MaxLag)
	case g.cfg.ValueSize < 0:
		return fmt.Errorf("generator: ValueSize is %d, must be >= 0", g.cfg.ValueSize)
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

// Seek positions the generator at offset. There is no accumulated state to
// discard: the offset is the whole of the source's state.
func (g *Generator) Seek(offset int64) error {
	if offset < 0 {
		return fmt.Errorf("generator: seek to negative offset %d", offset)
	}
	g.pos = offset
	return nil
}

// Position returns the offset of the element the next Next will return.
func (g *Generator) Position() int64 { return g.pos }

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

// valueOf returns ValueSize bytes derived from (Seed, n). The bytes are filled
// from successive mixes of one per-element base so the whole value, not just
// its first 8 bytes, is a function of the element index.
func (g *Generator) valueOf(n int64) []byte {
	v := make([]byte, g.cfg.ValueSize)
	base := mix(g.cfg.Seed^valueSalt, uint64(n))
	var buf [8]byte
	for i := 0; i < len(v); i += 8 {
		binary.BigEndian.PutUint64(buf[:], mix(base, uint64(i/8)))
		copy(v[i:], buf[:])
	}
	return v
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
