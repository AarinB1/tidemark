// Package runtime executes a job graph.
package runtime

import (
	"context"
	"sync"

	"github.com/AarinB1/tidemark/pkg/graph"
	"github.com/AarinB1/tidemark/pkg/transport"
)

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
func Run(ctx context.Context, g *graph.Graph) error {
	order, err := g.TopoOrder()
	if err != nil {
		return err
	}

	inputs, outputs := wire(g, order)

	// Nothing is recording checkpoints. Barriers still flow: they are part of
	// the element stream whether or not anybody snapshots on them, and the
	// alignment they drive is what makes a snapshot a consistent cut when
	// something does. Turning checkpointing on is the next step's job.
	cfg := subtaskConfig{}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Gates are built here rather than inside a subtask because Run is what
	// waits for their forwarders. A subtask cannot: see Gate.Wait.
	gates := make(map[subtaskID]*Gate, len(inputs))
	for id, ins := range inputs {
		gates[id] = NewGate(runCtx, ins)
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
