package sources

import (
	"bytes"
	"testing"

	"github.com/AarinB1/tidemark/pkg/core"
)

// seeds exercises every property below against more than one stream, so that a
// property that happens to hold for one seed is not mistaken for an invariant.
var seeds = []uint64{0, 1, 0xDEADBEEF, 0x9E3779B97F4A7C15}

func testConfig(seed uint64) GeneratorConfig {
	return GeneratorConfig{
		Seed:           seed,
		Count:          10000,
		KeyCardinality: 64,
		BaseEventTime:  1700000000000,
		EventTimeStep:  10,
		MaxLag:         250,
		ValueSize:      20,
	}
}

func open(t *testing.T, cfg GeneratorConfig) *Generator {
	t.Helper()
	g := NewGenerator(cfg)
	if err := g.Open(nil); err != nil {
		t.Fatalf("Open: %v", err)
	}
	return g
}

func readN(t *testing.T, g *Generator, n int) []*core.Record {
	t.Helper()
	out := make([]*core.Record, 0, n)
	for range n {
		rec, ok, err := g.Next()
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if !ok {
			t.Fatalf("Next reported exhaustion after %d of %d records", len(out), n)
		}
		out = append(out, rec)
	}
	return out
}

func equalRecords(a, b *core.Record) bool {
	return bytes.Equal(a.Key, b.Key) &&
		bytes.Equal(a.Value, b.Value) &&
		a.EventTime == b.EventTime
}

// TestSeekToMatchesSequentialRead is invariant 7: SeekTo(n) then read is identical
// to reading from 0 and discarding n. A generator that advanced a held random
// source would fail here, and only here.
func TestSeekToMatchesSequentialRead(t *testing.T) {
	const (
		skip = 500
		take = 100
	)
	for _, seed := range seeds {
		t.Run(name(seed), func(t *testing.T) {
			cfg := testConfig(seed)

			seeked := open(t, cfg)
			if err := seeked.SeekTo(skip); err != nil {
				t.Fatalf("SeekTo: %v", err)
			}
			if got := seeked.Position(); got != skip {
				t.Fatalf("Position after SeekTo(%d) = %d", skip, got)
			}
			got := readN(t, seeked, take)

			sequential := open(t, cfg)
			all := readN(t, sequential, skip+take)
			want := all[skip:]

			for i := range want {
				if !equalRecords(got[i], want[i]) {
					t.Fatalf("offset %d: got %+v, want %+v", skip+i, got[i], want[i])
				}
			}
			if got, want := seeked.Position(), int64(skip+take); got != want {
				t.Errorf("Position = %d, want %d", got, want)
			}
		})
	}
}

// TestSameSeedSameSequence pins reproducibility across construction, which is
// what makes a recovered run comparable to a clean one.
func TestSameSeedSameSequence(t *testing.T) {
	for _, seed := range seeds {
		t.Run(name(seed), func(t *testing.T) {
			cfg := testConfig(seed)
			a := readN(t, open(t, cfg), 1000)
			b := readN(t, open(t, cfg), 1000)
			for i := range a {
				if !equalRecords(a[i], b[i]) {
					t.Fatalf("offset %d: %+v != %+v", i, a[i], b[i])
				}
			}
		})
	}
}

// TestDifferentSeedsDiffer guards against a mix that ignores its seed, which
// would make every property above hold vacuously.
func TestDifferentSeedsDiffer(t *testing.T) {
	cfgA, cfgB := testConfig(seeds[0]), testConfig(seeds[1])
	a := readN(t, open(t, cfgA), 1000)
	b := readN(t, open(t, cfgB), 1000)
	same := 0
	for i := range a {
		if equalRecords(a[i], b[i]) {
			same++
		}
	}
	if same == len(a) {
		t.Fatal("two seeds produced identical sequences; the seed is not reaching mix")
	}
}

// TestEventTimeLagCoverage pins that the generator spreads event times across
// the whole lag range rather than clustering in part of it. MaxLag is small
// enough that a fixed number of draws covers every residue in [0, MaxLag]
// outright: a generator that always returned zero lag, or that reached only
// some of the range, leaves a residue unseen.
func TestEventTimeLagCoverage(t *testing.T) {
	const maxLag = 7
	const draws = 1000
	for _, seed := range seeds {
		t.Run(name(seed), func(t *testing.T) {
			cfg := testConfig(seed)
			cfg.MaxLag = maxLag
			cfg.Count = draws
			g := open(t, cfg)

			var seen [maxLag + 1]bool
			for n := range int64(draws) {
				rec, ok, err := g.Next()
				if err != nil || !ok {
					t.Fatalf("Next at %d: ok=%v err=%v", n, ok, err)
				}
				lag := cfg.BaseEventTime + n*cfg.EventTimeStep - rec.EventTime
				if lag < 0 || lag > maxLag {
					t.Fatalf("offset %d: lag %d is outside [0, %d]", n, lag, maxLag)
				}
				seen[lag] = true
			}
			for lag, hit := range seen {
				if !hit {
					t.Errorf("lag %d never occurred in %d draws", lag, draws)
				}
			}
		})
	}
}

// TestEventTimeWithinLagBound is the property Phase 2's watermark generator is
// tested against: out-of-orderness is bounded by exactly MaxLag. Only the bound
// is asserted here, at a lag wide enough that its endpoints need not occur;
// TestEventTimeLagCoverage is what pins the spread.
func TestEventTimeWithinLagBound(t *testing.T) {
	const maxLag = 5000
	const draws = 10000
	for _, seed := range seeds {
		t.Run(name(seed), func(t *testing.T) {
			cfg := testConfig(seed)
			cfg.MaxLag = maxLag
			cfg.Count = draws
			g := open(t, cfg)

			for n := range int64(draws) {
				rec, ok, err := g.Next()
				if err != nil || !ok {
					t.Fatalf("Next at %d: ok=%v err=%v", n, ok, err)
				}
				inOrder := cfg.BaseEventTime + n*cfg.EventTimeStep
				if rec.EventTime > inOrder {
					t.Fatalf("offset %d: event time %d is later than %d", n, rec.EventTime, inOrder)
				}
				if rec.EventTime < inOrder-maxLag {
					t.Fatalf("offset %d: event time %d is earlier than %d", n, rec.EventTime, inOrder-maxLag)
				}
			}
		})
	}
}

func TestDistinctKeyCount(t *testing.T) {
	cardinalities := []int64{1, 7, 64}
	for _, seed := range seeds {
		for _, cardinality := range cardinalities {
			t.Run(name(seed), func(t *testing.T) {
				cfg := testConfig(seed)
				cfg.Count = 100000
				cfg.KeyCardinality = cardinality
				g := open(t, cfg)

				seen := make(map[string]bool, cardinality)
				for {
					rec, ok, err := g.Next()
					if err != nil {
						t.Fatalf("Next: %v", err)
					}
					if !ok {
						break
					}
					if len(rec.Key) != 8 {
						t.Fatalf("key is %d bytes, want a fixed width of 8", len(rec.Key))
					}
					seen[string(rec.Key)] = true
				}
				if got := int64(len(seen)); got != cardinality {
					t.Errorf("saw %d distinct keys over %d records, want %d", got, cfg.Count, cardinality)
				}
			})
		}
	}
}

func TestExhaustsExactlyAtCount(t *testing.T) {
	counts := []int64{1, 2, 1000}
	for _, seed := range seeds {
		for _, count := range counts {
			t.Run(name(seed), func(t *testing.T) {
				cfg := testConfig(seed)
				cfg.Count = count
				g := open(t, cfg)

				readN(t, g, int(count))
				if got := g.Position(); got != count {
					t.Errorf("Position = %d, want %d", got, count)
				}
				rec, ok, err := g.Next()
				if ok || rec != nil || err != nil {
					t.Fatalf("Next past Count = (%+v, %v, %v), want (nil, false, nil)", rec, ok, err)
				}
				// Exhaustion is stable: asking again does not resume.
				if _, ok, _ := g.Next(); ok {
					t.Error("Next resumed after reporting exhaustion")
				}
			})
		}
	}
}

func TestValueSize(t *testing.T) {
	sizes := []int{0, 1, 8, 20, 64}
	for _, size := range sizes {
		t.Run(name(uint64(size)), func(t *testing.T) {
			cfg := testConfig(seeds[2])
			cfg.ValueSize = size
			g := open(t, cfg)
			for _, rec := range readN(t, g, 100) {
				if len(rec.Value) != size {
					t.Fatalf("value is %d bytes, want %d", len(rec.Value), size)
				}
			}
		})
	}
}

func TestOpenRejectsBadConfig(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*GeneratorConfig)
	}{
		{name: "zero count", mutate: func(c *GeneratorConfig) { c.Count = 0 }},
		{name: "negative count", mutate: func(c *GeneratorConfig) { c.Count = -1 }},
		{name: "zero cardinality", mutate: func(c *GeneratorConfig) { c.KeyCardinality = 0 }},
		{name: "negative lag", mutate: func(c *GeneratorConfig) { c.MaxLag = -1 }},
		{name: "negative value size", mutate: func(c *GeneratorConfig) { c.ValueSize = -1 }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := testConfig(seeds[0])
			tt.mutate(&cfg)
			if err := NewGenerator(cfg).Open(nil); err == nil {
				t.Fatal("Open accepted an invalid config")
			}
		})
	}
}

func TestSeekToRejectsNegativeOffset(t *testing.T) {
	g := open(t, testConfig(seeds[0]))
	if err := g.SeekTo(-1); err == nil {
		t.Fatal("SeekTo accepted a negative offset")
	}
	if got := g.Position(); got != 0 {
		t.Errorf("a rejected SeekTo moved the position to %d", got)
	}
}

func name(seed uint64) string {
	return "seed_" + itoa(seed)
}

func itoa(v uint64) string {
	if v == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	return string(buf[i:])
}
