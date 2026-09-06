package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/AarinB1/tidemark/pkg/checkpoint"
	"github.com/AarinB1/tidemark/pkg/graph"
	tmruntime "github.com/AarinB1/tidemark/pkg/runtime"
	"github.com/AarinB1/tidemark/pkg/state"
)

// The state-size harness: how large a checkpoint a workload configuration
// actually produces.
//
// This is the memory-bound half of Phase 6b, and the half a machine with four
// cores and rotational disks can answer honestly. Nothing here is a timing:
// entry counts and byte counts are functions of the workload and the encoding,
// so the same configuration on any machine produces the same numbers.
//
// # Why parallelism is an axis and not a constant
//
// Source subtasks split the offset space into CONTIGUOUS ranges, and the
// Nexmark source's event time increases with offset. Subtask 0 therefore covers
// the earliest event times, and the input gate's watermark is the minimum
// across its inputs (invariant 1), so no window can fire while subtask 0 is
// still emitting. The later subtasks' windows accumulate and cannot be purged.
//
// At parallelism 1 there is one range, event time advances monotonically, and a
// window fires and purges as soon as the watermark passes it: state stays at
// roughly one window. At parallelism above 1 nearly the whole dataset's windows
// are live at once, because every subtask finishes at about the same moment.
//
// So checkpoint size here is a function of parallelism in a way it would not be
// in a system whose partitions overlap in time. Phase 2 recorded that as a
// comment; this is where it becomes a number.

// StateResult is one configuration's state measurement.
type StateResult struct {
	Config
	// PeakEntries is the greatest number of KeyedState entries the operator
	// subtasks of this run held BETWEEN THEM at one instant. It is measured on
	// the record path rather than sampled, so it is the real peak and not the
	// largest of some samples.
	PeakEntries int64 `json:"peak_state_entries"`
	// CheckpointID is which checkpoint the byte counts below describe: the
	// largest complete one the run wrote.
	CheckpointID int64 `json:"checkpoint_id"`
	// Checkpoints is how many complete checkpoints the run wrote, so a reader
	// can tell "the largest of eight" from "the only one".
	Checkpoints int `json:"checkpoints"`
	// StateBytes is the serialised keyed state of every operator subtask in
	// that checkpoint, summed. DiskBytes is what the checkpoint directory
	// occupies: the same state plus per-file framing, the source offsets, the
	// sink payloads and the metadata.
	StateBytes int64 `json:"state_bytes"`
	DiskBytes  int64 `json:"disk_bytes"`
	// Entries is how many KeyedState entries that checkpoint holds, decoded
	// from the payloads rather than inferred from PeakEntries: the two are
	// measured at different instants and a bytes-per-entry built from both
	// would be a ratio of two different states.
	Entries       int64          `json:"checkpoint_entries"`
	BytesPerEntry float64        `json:"bytes_per_entry"`
	PerSubtask    []SubtaskState `json:"per_subtask"`
	// ProjectedPeakBytes is PeakEntries at this configuration's bytes per
	// entry: what a checkpoint taken at the peak would have held. DERIVED, and
	// named as such, because no checkpoint was taken at that instant.
	ProjectedPeakBytes int64 `json:"projected_peak_bytes"`
	Oversubscribed     bool  `json:"oversubscribed"`
}

// SubtaskState is one operator subtask's share of a checkpoint.
//
// Per subtask because a subtask is the unit of state: a skewed key space shows
// up here as one subtask holding most of the bytes, and a total alone would
// hide it.
type SubtaskState struct {
	VertexID string `json:"vertex_id"`
	Index    int    `json:"index"`
	Bytes    int64  `json:"bytes"`
	Entries  int64  `json:"entries"`
}

// StateReport is what the state sweep writes.
type StateReport struct {
	Fingerprint Fingerprint `json:"fingerprint"`
	// Provisional marks output that is not a published result. It is false for
	// this report: state size is memory-bound and reproducible, so these
	// numbers stand on any machine. The recovery report sets it true.
	Provisional bool          `json:"provisional"`
	Results     []StateResult `json:"results"`
}

// measureState runs one configuration with checkpointing on and reports what it
// held and what it wrote.
//
// root is a directory this function owns: it writes the run's checkpoints there
// and removes them before returning, because a sweep across a dozen
// configurations would otherwise leave every checkpoint of every one of them on
// disk at once.
func measureState(cfg Config, checkpointsPerSubtask int, root string) (StateResult, error) {
	if checkpointsPerSubtask < 1 {
		return StateResult{}, fmt.Errorf("checkpoints per subtask is %d, must be >= 1", checkpointsPerSubtask)
	}
	interval := barrierIntervalFor(cfg, checkpointsPerSubtask)
	g, err := buildGraph(cfg, interval)
	if err != nil {
		return StateResult{}, err
	}
	operators, err := operatorSubtasks(g)
	if err != nil {
		return StateResult{}, err
	}

	meter := &stateMeter{}
	if err := tmruntime.RunWithOptions(context.Background(), g, tmruntime.Options{
		CheckpointRoot: root,
		Seed:           cfg.Seed,
		NewState:       meter.newState,
	}); err != nil {
		return StateResult{}, fmt.Errorf("%s: %w", cfg.label(), err)
	}

	res := StateResult{
		Config:         cfg,
		PeakEntries:    meter.peak.Load(),
		Oversubscribed: cfg.oversubscribed(),
	}
	sizes, err := scanCheckpoints(root, operators)
	if err != nil {
		return StateResult{}, fmt.Errorf("%s: %w", cfg.label(), err)
	}
	res.Checkpoints = len(sizes)
	if len(sizes) > 0 {
		// The LARGEST, not the last. They are almost always the same
		// checkpoint, because state grows until the sources exhaust; taking the
		// largest rather than assuming that makes the number true even when a
		// window purges between the last two barriers.
		largest := sizes[0]
		for _, s := range sizes[1:] {
			if s.stateBytes > largest.stateBytes {
				largest = s
			}
		}
		res.CheckpointID = largest.id
		res.StateBytes = largest.stateBytes
		res.DiskBytes = largest.diskBytes
		res.Entries = largest.entries
		res.PerSubtask = largest.perSubtask
		if largest.entries > 0 {
			res.BytesPerEntry = float64(largest.stateBytes) / float64(largest.entries)
			res.ProjectedPeakBytes = int64(res.BytesPerEntry * float64(res.PeakEntries))
		}
	}
	return res, nil
}

// barrierIntervalFor spaces barriers so that every source subtask injects
// roughly checkpointsPerSubtask of them.
//
// Per SUBTASK rather than per job, because a subtask counts within its own
// contiguous range: at parallelism 4 a fixed interval would give each subtask a
// quarter as many barriers and the last checkpoint would land a quarter of the
// way in.
func barrierIntervalFor(cfg Config, checkpointsPerSubtask int) int64 {
	perSubtask := cfg.Records / int64(cfg.Parallelism)
	interval := perSubtask / int64(checkpointsPerSubtask)
	if interval < 1 {
		return 1
	}
	return interval
}

// operatorSubtasks names every operator subtask of g.
//
// Only operator subtasks are read as keyed state. A source subtask's payload is
// a resume offset and a sink's is a staged transaction, so a scan that decoded
// every file in a checkpoint directory would be decoding three formats as one.
func operatorSubtasks(g *graph.Graph) (map[checkpoint.SubtaskKey]bool, error) {
	order, err := g.TopoOrder()
	if err != nil {
		return nil, err
	}
	keys := make(map[checkpoint.SubtaskKey]bool)
	for _, v := range order {
		if v.Kind != graph.VertexOperator {
			continue
		}
		for i := range v.Parallelism {
			keys[checkpoint.SubtaskKey{VertexID: v.ID, Index: i}] = true
		}
	}
	return keys, nil
}

// checkpointSize is one complete checkpoint, measured.
type checkpointSize struct {
	id         int64
	stateBytes int64
	diskBytes  int64
	entries    int64
	perSubtask []SubtaskState
}

// scanCheckpoints measures every complete checkpoint under root.
//
// Two byte counts, because they answer different questions. stateBytes is what
// the operator subtasks' keyed state serialised to, which is the quantity that
// scales with the workload. diskBytes is what the directory occupies, which is
// what a machine has to write and read back and includes the framing, the
// metadata and the source offsets.
//
// Entries are decoded rather than taken from the header bytes directly:
// state.ReadFrom is the public way to read a payload back, and a benchmark that
// reached into the wire format would keep reporting a number after the format
// moved underneath it.
func scanCheckpoints(root string, operators map[checkpoint.SubtaskKey]bool) ([]checkpointSize, error) {
	entries, err := os.ReadDir(root)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("checkpoint root %s: %w", root, err)
	}
	storage := checkpoint.NewStorage(root)

	var sizes []checkpointSize
	for _, e := range entries {
		if !e.IsDir() || !strings.HasPrefix(e.Name(), "chk-") {
			continue
		}
		id, convErr := strconv.ParseInt(strings.TrimPrefix(e.Name(), "chk-"), 10, 64)
		if convErr != nil {
			continue
		}
		_, payloads, err := storage.Load(id)
		if err != nil {
			// An incomplete checkpoint is ordinary: the run ends between two
			// barriers, and the last one may never have been acknowledged by
			// every subtask. It is not a measurement, so it is skipped rather
			// than reported as a checkpoint of zero bytes.
			continue
		}
		size := checkpointSize{id: id}
		size.diskBytes, err = dirBytes(filepath.Join(root, e.Name()))
		if err != nil {
			return nil, err
		}
		for key, payload := range payloads {
			if !operators[key] {
				continue
			}
			n, err := entriesIn(payload)
			if err != nil {
				return nil, fmt.Errorf("checkpoint %d, subtask %s: %w", id, key, err)
			}
			size.stateBytes += int64(len(payload))
			size.entries += n
			size.perSubtask = append(size.perSubtask, SubtaskState{
				VertexID: key.VertexID, Index: key.Index, Bytes: int64(len(payload)), Entries: n,
			})
		}
		// Load returns a map, so the order is Go's. Sorted here because the
		// rows go into a JSON document that gets diffed between machines.
		sort.Slice(size.perSubtask, func(i, j int) bool {
			if size.perSubtask[i].VertexID != size.perSubtask[j].VertexID {
				return size.perSubtask[i].VertexID < size.perSubtask[j].VertexID
			}
			return size.perSubtask[i].Index < size.perSubtask[j].Index
		})
		sizes = append(sizes, size)
	}
	sort.Slice(sizes, func(i, j int) bool { return sizes[i].id < sizes[j].id })
	return sizes, nil
}

// entriesIn counts the KeyedState entries a serialised payload holds.
func entriesIn(payload []byte) (int64, error) {
	dst := state.NewMemory()
	if err := state.ReadFrom(dst, bytes.NewReader(payload)); err != nil {
		return 0, err
	}
	return int64(dst.Len()), nil
}

// dirBytes is the size of every regular file under dir.
func dirBytes(dir string) (int64, error) {
	var total int64
	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		total += info.Size()
		return nil
	})
	return total, err
}

// stateMeter tracks the entries the operator subtasks of one run hold between
// them, and the greatest number they held at once.
//
// Shared across the subtasks and read afterwards, so it is atomic rather than
// locked: the increment is on the record path, once per new (key, window) and
// once per timer, and a mutex there would be paying for contention on every
// window a record opens.
//
// The same shape as the chaos suite's meter, and deliberately a second copy:
// test/chaos is a test package that imports testing, and a command cannot
// import it. The alternative is a shared package holding forty lines, which is
// the abstraction layer the scope rule refuses.
type stateMeter struct {
	current atomic.Int64
	peak    atomic.Int64
}

func (m *stateMeter) inc() {
	n := m.current.Add(1)
	for {
		p := m.peak.Load()
		if n <= p || m.peak.CompareAndSwap(p, n) {
			return
		}
	}
}

func (m *stateMeter) dec() { m.current.Add(-1) }

// newState is what runtime.Options.NewState is set to.
func (m *stateMeter) newState() (state.KeyedState, error) {
	return &countingState{inner: state.NewMemory(), meter: m}, nil
}

// countingState is a KeyedState that keeps its entry count.
//
// Put and Delete each read the key first to tell an insert from an update and a
// removal from a no-op. That doubles the reads on the record path, which is why
// no throughput run uses this: KeyedState deliberately has no Len, and the
// count is not obtainable any other way.
//
// The inner state is Memory, which has no Close, so this decorator has none
// either. Wrapping a backend that does own resources would need one forwarded
// explicitly: the runtime asserts for it, and an embedded interface would
// satisfy the assertion while closing nothing.
type countingState struct {
	inner state.KeyedState
	meter *stateMeter
}

var _ state.KeyedState = (*countingState)(nil)

func (c *countingState) Get(key []byte) ([]byte, bool) { return c.inner.Get(key) }

func (c *countingState) Put(key, value []byte) {
	_, existed := c.inner.Get(key)
	c.inner.Put(key, value)
	if !existed {
		c.meter.inc()
	}
}

func (c *countingState) Delete(key []byte) {
	_, existed := c.inner.Get(key)
	c.inner.Delete(key)
	if existed {
		c.meter.dec()
	}
}

func (c *countingState) Iterate(fn func(key, value []byte) bool) { c.inner.Iterate(fn) }
func (c *countingState) Err() error                              { return c.inner.Err() }

// runStateSweep measures every configuration and prints the table.
func runStateSweep(opts sweepOptions) (StateReport, error) {
	configs, err := sweep(opts)
	if err != nil {
		return StateReport{}, err
	}
	dir, err := os.Getwd()
	if err != nil {
		return StateReport{}, fmt.Errorf("working directory: %w", err)
	}
	report := StateReport{Fingerprint: machineFingerprint(dir)}

	for _, cfg := range configs {
		if len(timerVertices(cfg.Query)) == 0 {
			// A query with no windows holds no keyed state, so its checkpoint
			// is a set of source offsets whatever the auction space is. Running
			// it would put a row of zeroes in a table about state size.
			fmt.Printf("%s: skipped, this query holds no keyed state\n", cfg.label())
			continue
		}
		root, err := os.MkdirTemp(opts.stateDir, "tidemark-state-")
		if err != nil {
			return StateReport{}, fmt.Errorf("checkpoint directory: %w", err)
		}
		res, err := measureState(cfg, opts.checkpoints, root)
		// Removed whether or not the run succeeded: a sweep of a dozen
		// configurations would otherwise hold every checkpoint of every one of
		// them at once, and the largest configurations are the point.
		if rmErr := os.RemoveAll(root); rmErr != nil && err == nil {
			err = rmErr
		}
		if err != nil {
			return StateReport{}, err
		}
		report.Results = append(report.Results, res)
		fmt.Printf("%s: peak=%d entries, checkpoint %d = %s over %d entries (%.1f B/entry), disk %s\n",
			res.label(), res.PeakEntries, res.CheckpointID, humanBytes(res.StateBytes),
			res.Entries, res.BytesPerEntry, humanBytes(res.DiskBytes))
	}
	return report, nil
}

// printStateTable is the table the phase report quotes.
func printStateTable(r StateReport) {
	if len(r.Results) == 0 {
		return
	}
	fmt.Printf("\nstate size (%s)\n", r.Fingerprint)
	fmt.Printf("%-6s %5s %10s %12s %14s %12s %10s %12s\n",
		"query", "p", "auctions", "peak entries", "chk entries", "chk state", "B/entry", "peak proj.")
	for _, res := range r.Results {
		fmt.Printf("%-6s %5d %10d %12d %14d %12s %10.1f %12s\n",
			res.Query, res.Parallelism, res.AuctionCardinality, res.PeakEntries, res.Entries,
			humanBytes(res.StateBytes), res.BytesPerEntry, humanBytes(res.ProjectedPeakBytes))
	}
}

// humanBytes renders a byte count in the unit a reader compares in.
func humanBytes(n int64) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%.2f GiB", float64(n)/(1<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%.2f MiB", float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.2f KiB", float64(n)/(1<<10))
	default:
		return fmt.Sprintf("%d B", n)
	}
}
