// Package runtime executes a job graph.
package runtime

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"

	"github.com/AarinB1/tidemark/pkg/checkpoint"
	"github.com/AarinB1/tidemark/pkg/graph"
	"github.com/AarinB1/tidemark/pkg/state"
	"github.com/AarinB1/tidemark/pkg/transport"
)

// errNoRestorePoint is returned when a run was told to resume from a checkpoint
// root that holds no complete checkpoint.
var errNoRestorePoint = errors.New("no complete checkpoint to restore from")

// Options configures one run of a job.
//
// The zero value is the Phase 3a behaviour: barriers flow through the stream
// and drive alignment, and nothing records a snapshot on them. That is what
// every test written before this step runs on, and it is why Run keeps its
// signature.
type Options struct {
	// CheckpointRoot, when non-empty, is the directory checkpoints are written
	// under. Empty means the job takes none.
	CheckpointRoot string
	// RestoreFrom, when non-empty, is a checkpoint root to resume from. The
	// highest checkpoint there with a _COMPLETE marker is the one used, and a
	// root with none is an error rather than a fresh start: a caller that asked
	// to resume and got a run from offset zero would get correct-looking output
	// from a job that silently did not recover.
	//
	// It may be the same directory as CheckpointRoot. Checkpoint IDs continue
	// from the one restored, so a resumed run does not write over the
	// checkpoint it came from.
	RestoreFrom string
	// Seed is recorded in the checkpoint metadata. It is not validated on
	// restore; see checkpoint.Metadata for why the graph cannot report one.
	Seed uint64
	// NewState makes the keyed state for one operator subtask. nil is the
	// in-memory backend, which is the default because it is the one a job that
	// fits in RAM should use and the one every test that is not about the
	// backend should run on.
	//
	// It is called once per operator subtask, so a backend that owns files
	// gives each subtask its own: a subtask is the unit of state, and two
	// sharing one store would put two key spaces in one file with nothing
	// between them. The runtime closes what it makes, if it has a Close.
	NewState func() (state.KeyedState, error)
	// FaultInjector, when non-nil, is consulted at the three logical positions
	// a subtask can be aborted at: before a record, just after a barrier is
	// forwarded, and inside an alignment window. nil means no faults, which is
	// every job that is not a chaos run.
	//
	// It is the only way to abort a subtask at a chosen logical position, and
	// it exists so that a chaos run goes through the same loops a real job
	// does rather than through a copy of them. See FaultInjector.
	FaultInjector FaultInjector
}

// Run executes g to completion with no checkpointing and no restore.
func Run(ctx context.Context, g *graph.Graph) error {
	return RunWithOptions(ctx, g, Options{})
}

// Run executes g to completion and returns the first error any subtask
// reported.
//
// One goroutine per subtask, identified by (vertexID, index). Fan-out and
// multi-input topologies execute; the chain-only refusal Phase 0 carried is
// gone, replaced by the record writer that partitions and the input gate that
// merges.
//
// Deadlock freedom rests on two properties, and on nothing else:
//
//  1. The graph is a DAG, so there is no cycle of subtasks each waiting on the
//     next. TopoOrder establishes this before anything is wired.
//  2. Every subtask always drains its inputs. A subtask never stops receiving
//     while holding a send, so a full channel always has a consumer that will
//     eventually empty it, and backpressure propagates rather than latching.
//
// Phase 3 weakens the second one: barrier alignment stops processing elements
// from an input that has delivered its barrier. That is why the gate must
// buffer those elements per input and keep consuming, rather than pausing a
// forwarder. Whoever reads this then needs to know property 2 was load-bearing
// here, not incidental.
func RunWithOptions(ctx context.Context, g *graph.Graph, opts Options) error {
	order, err := g.TopoOrder()
	if err != nil {
		return err
	}

	meta := buildMetadata(order, opts.Seed)

	// Restore first, and before a single goroutine is started. A validation
	// failure here has to stop the job rather than fail it part way through:
	// half a job that ran on a checkpoint it should have rejected has already
	// written to the sink.
	restored, restoredID, err := loadRestorePoint(opts.RestoreFrom, meta)
	if err != nil {
		return err
	}

	var coordinator *checkpoint.Coordinator
	if opts.CheckpointRoot != "" {
		coordinator = checkpoint.NewCoordinator(checkpoint.NewStorage(opts.CheckpointRoot), meta)
	}

	inputs, outputs := wire(g, order)

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Gates are built here rather than inside a subtask because Run is what
	// waits for their forwarders. A subtask cannot: see Gate.Wait.
	gates := make(map[subtaskID]*Gate, len(inputs))
	for id, ins := range inputs {
		gates[id] = NewGate(runCtx, ins, faults{injector: opts.FaultInjector, id: id})
	}

	subtasks := 0
	for _, v := range order {
		subtasks += v.Parallelism
	}

	// Buffered to the subtask count so no failing goroutine ever blocks on
	// reporting, which would leave Run waiting for it in wg.Wait.
	errs := make(chan error, subtasks)
	var wg sync.WaitGroup
	for _, v := range order {
		for index := range v.Parallelism {
			id := subtaskID{vertexID: v.ID, index: index}
			wg.Add(1)
			go func() {
				defer wg.Done()
				cfg := subtaskConfig{
					coordinator:        coordinator,
					newState:           opts.NewState,
					restoredCheckpoint: restoredID,
					injector:           opts.FaultInjector,
				}
				if payload, ok := restored[id.checkpointKey()]; ok {
					cfg.restore, cfg.restored = payload, true
				}
				if err := runSubtask(runCtx, v, index, gates[id], outputs[id], cfg); err != nil {
					errs <- err
					cancel()
				}
			}()
		}
	}
	wg.Wait()
	close(errs)

	// Every subtask has unwound, so every channel has been closed by its one
	// producer, so every forwarder can reach its end. This is the only point at
	// which waiting for them is guaranteed to return, and it is what makes "no
	// goroutine outlives Run" true.
	for _, gate := range gates {
		gate.Wait()
	}

	// The first error into the channel is the one that cancelled the context;
	// anything after it is a consequence.
	for err := range errs {
		return err
	}
	return nil
}

// wire creates the channels between subtasks and returns each subtask's inputs
// and outputs.
//
// For an edge from vertex A at parallelism P to vertex B at parallelism Q there
// are P*Q channels: A_i writes to {(i,0)..(i,Q-1)} and B_j reads
// {(0,j)..(P-1,j)}. Every channel therefore has exactly one producer and
// exactly one consumer, which is the contract *transport.Channel is built on
// and the reason a subtask may close its own outputs and nothing else.
//
// A subtask's outputs are grouped by downstream vertex: one group per outgoing
// edge, holding that edge's channel to each subtask on the far end. The Writer
// partitions within a group and broadcasts across groups, so the grouping is
// what makes a vertex with two downstream vertices send the whole stream to
// each rather than splitting it between them. A flat list cannot express that
// distinction, which is how the split survived Phase 1.
//
// Order matters and is fixed here. The groups are ordered by downstream vertex
// (lexicographic, from Downstream) and each group's outputs by downstream
// index. Within a group the order is load-bearing: the writer picks among a
// group by hash modulo its length, so a different order would send the same key
// to a different subtask between runs and quietly falsify the reproducibility
// claim. Across groups it is not — every group receives every record — but it
// still fixes which send is attempted first, and therefore which failure
// surfaces when a cancelled job comes apart. That should not vary run to run
// either. A subtask's inputs are ordered by upstream vertex and then by
// upstream index for the same reason in reverse — Phase 2 indexes per-input
// watermarks by position, and Phase 3 indexes barrier alignment the same way.
func wire(g *graph.Graph, order []graph.Vertex) (map[subtaskID][]transport.Input, map[subtaskID][][]transport.Output) {
	byID := make(map[string]graph.Vertex, len(order))
	for _, v := range order {
		byID[v.ID] = v
	}

	inputs := make(map[subtaskID][]transport.Input)
	outputs := make(map[subtaskID][][]transport.Output)

	// order is topological with a lexicographic tie-break and Downstream is
	// already sorted lexicographically, so this traversal, and therefore both
	// the group order and the channel order within a group, is a function of
	// the graph alone.
	for _, from := range order {
		for _, toID := range g.Downstream(from.ID) {
			to := byID[toID]
			for i := range from.Parallelism {
				producer := subtaskID{vertexID: from.ID, index: i}
				// One group per edge. to.Parallelism >= 1 is guaranteed by
				// graph validation, so this group is never empty and NewWriter
				// never rejects it.
				group := make([]transport.Output, 0, to.Parallelism)
				for j := range to.Parallelism {
					ch := transport.NewChannel(transport.DefaultCapacity)
					consumer := subtaskID{vertexID: toID, index: j}
					group = append(group, ch)
					inputs[consumer] = append(inputs[consumer], ch)
				}
				outputs[producer] = append(outputs[producer], group)
			}
		}
	}
	return inputs, outputs
}

// buildMetadata describes the job in order for the checkpoint format.
//
// Vertices are sorted by ID rather than left in topological order. Both are
// functions of the graph alone, so either would compare stably, but ID order is
// the one a reader can check against a graph definition without running Kahn's
// algorithm in their head -- and this slice is what a mismatch error is
// reported against.
//
// Count comes from the source itself, through the same splittableSource
// assertion the source runner makes, so the number recorded is the number the
// range arithmetic will use. A vertex that is not a countable source records
// zero.
func buildMetadata(order []graph.Vertex, seed uint64) checkpoint.Metadata {
	meta := checkpoint.Metadata{Seed: seed}
	for _, v := range order {
		vm := checkpoint.VertexMeta{ID: v.ID, Parallelism: v.Parallelism}
		if v.Kind == graph.VertexSource && v.NewSource != nil {
			// One instance, made and discarded, purely to ask its length. It is
			// never opened, so it holds nothing to release.
			if s, ok := v.NewSource().(splittableSource); ok {
				vm.Count = s.Count()
			}
		}
		meta.Vertices = append(meta.Vertices, vm)
	}
	slices.SortFunc(meta.Vertices, func(a, b checkpoint.VertexMeta) int {
		return strings.Compare(a.ID, b.ID)
	})
	return meta
}

// loadRestorePoint reads the checkpoint a run is resuming from, if it is
// resuming from one.
//
// It validates the recorded shape of the job against the shape about to run,
// and refuses a mismatch. That refusal is the Phase 1 constraint coming due: a
// source subtask's range is derived from (Count, parallelism, index) and only
// its resume OFFSET is checkpointed, so at a different shape subtask 1 resumes
// from an offset inside somebody else's range. The job reads a perfectly valid
// stream that is not the one it was checkpointed on, and produces a wrong
// answer with nothing to point at. It is never adapted to.
func loadRestorePoint(root string, meta checkpoint.Metadata) (map[checkpoint.SubtaskKey][]byte, int64, error) {
	if root == "" {
		return nil, 0, nil
	}
	storage := checkpoint.NewStorage(root)
	id, ok, err := storage.Latest()
	if err != nil {
		return nil, 0, err
	}
	if !ok {
		return nil, 0, fmt.Errorf("%w under %s", errNoRestorePoint, root)
	}
	recorded, payloads, err := storage.Load(id)
	if err != nil {
		return nil, 0, err
	}
	if err := recorded.CheckAgainst(meta); err != nil {
		return nil, 0, fmt.Errorf("restoring checkpoint %d from %s: %w", id, root, err)
	}
	return payloads, id, nil
}
