// Package runtime executes a job graph.
package runtime

import (
	"context"
	"fmt"
	"math"
	"sync"

	"github.com/AarinB1/tidemark/pkg/core"
	"github.com/AarinB1/tidemark/pkg/graph"
	"github.com/AarinB1/tidemark/pkg/transport"
)

// Run executes g to completion and returns the first error any vertex reported.
//
// Every vertex gets its own goroutine even though parallelism is 1. A pull loop
// driving the whole chain from the sink would be shorter today and thrown away
// entirely in Phase 1: "single parallelism" is a statement about how many
// subtasks a vertex has, not about how many goroutines the job runs in.
func Run(ctx context.Context, g *graph.Graph) error {
	order, err := g.TopoOrder()
	if err != nil {
		return err
	}

	inputs, outputs, err := wire(g, order)
	if err != nil {
		return err
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Buffered to the vertex count so no failing goroutine ever blocks on
	// reporting, which would leave Run waiting for it in wg.Wait.
	errs := make(chan error, len(order))
	var wg sync.WaitGroup
	for _, v := range order {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := runVertex(runCtx, v, inputs[v.ID], outputs[v.ID]); err != nil {
				errs <- err
				cancel()
			}
		}()
	}
	wg.Wait()
	close(errs)

	// The first error into the channel is the one that cancelled the context;
	// anything after it is a consequence.
	for err := range errs {
		return err
	}
	return nil
}

// wire creates one channel per edge and returns each vertex's input and
// outputs.
//
// Phase 0 supports a chain: at most one input and at most one output per
// vertex. Merging several inputs needs the input gate that takes
// min(perChannelWatermark), and splitting one output over several downstream
// channels needs a partitioner. Both arrive in Phase 1, and both are rejected
// here rather than approximated, because a wrong merge produces slightly wrong
// results instead of a failure.
func wire(g *graph.Graph, order []graph.Vertex) (map[string]*transport.Channel, map[string][]*transport.Channel, error) {
	inputs := make(map[string]*transport.Channel, len(order))
	outputs := make(map[string][]*transport.Channel, len(order))
	for _, v := range order {
		for _, to := range g.Downstream(v.ID) {
			if _, ok := inputs[to]; ok {
				return nil, nil, fmt.Errorf("vertex %q has more than one input: merging inputs arrives in Phase 1", to)
			}
			ch := transport.NewChannel(transport.DefaultCapacity)
			outputs[v.ID] = append(outputs[v.ID], ch)
			inputs[to] = ch
		}
		if len(outputs[v.ID]) > 1 {
			return nil, nil, fmt.Errorf("vertex %q has more than one output: partitioning arrives in Phase 1", v.ID)
		}
	}
	return inputs, outputs, nil
}

// runVertex runs one vertex to completion. It closes the vertex's outputs on
// every exit path, including a failure, so a downstream goroutine blocked in
// Recv always unblocks: Recv has no context to watch, and closure is the only
// signal it has.
func runVertex(ctx context.Context, v graph.Vertex, in *transport.Channel, outs []*transport.Channel) error {
	defer func() {
		for _, out := range outs {
			out.Close()
		}
	}()

	switch v.Kind {
	case graph.VertexSource:
		return runSource(ctx, v, outs)
	case graph.VertexOperator:
		return runOperator(ctx, v, in, outs)
	case graph.VertexSink:
		return runSink(ctx, v, in)
	default:
		return fmt.Errorf("vertex %q: unknown kind %d", v.ID, uint8(v.Kind))
	}
}

func runSource(ctx context.Context, v graph.Vertex, outs []*transport.Channel) (err error) {
	src := v.NewSource()
	oc := newOpContext(ctx, outs)
	if err := src.Open(oc); err != nil {
		return fmt.Errorf("source %q: open: %w", v.ID, err)
	}
	defer func() {
		if cerr := src.Close(); cerr != nil && err == nil {
			err = fmt.Errorf("source %q: close: %w", v.ID, cerr)
		}
	}()

	for {
		// Checked every element so that a source whose consumer is keeping up,
		// and which therefore never blocks in Send, still notices a cancelled
		// job.
		if err := ctx.Err(); err != nil {
			return err
		}
		rec, ok, err := src.Next()
		if err != nil {
			return fmt.Errorf("source %q: next: %w", v.ID, err)
		}
		if !ok {
			break
		}
		oc.Emit(rec)
		if err := oc.takeErr(); err != nil {
			return err
		}
	}

	return broadcast(ctx, outs, core.NewEndOfStreamElement())
}

func runOperator(ctx context.Context, v graph.Vertex, in *transport.Channel, outs []*transport.Channel) (err error) {
	op := v.NewOperator()
	oc := newOpContext(ctx, outs)
	if err := op.Open(oc); err != nil {
		return fmt.Errorf("operator %q: open: %w", v.ID, err)
	}
	defer func() {
		if cerr := op.Close(); cerr != nil && err == nil {
			err = fmt.Errorf("operator %q: close: %w", v.ID, cerr)
		}
	}()

	for {
		e, ok := in.Recv()
		if !ok {
			// The upstream vertex closed without sending end-of-stream: it
			// failed, or the job was cancelled. That goroutine reports the
			// cause, so this one exits quietly.
			return ctx.Err()
		}
		switch e.Kind {
		case core.KindRecord:
			if err := op.ProcessElement(e.Record, oc); err != nil {
				return fmt.Errorf("operator %q: process: %w", v.ID, err)
			}
			if err := oc.takeErr(); err != nil {
				return err
			}
		case core.KindWatermark:
			oc.watermark = e.Watermark
			if err := op.ProcessWatermark(e.Watermark, oc); err != nil {
				return fmt.Errorf("operator %q: watermark: %w", v.ID, err)
			}
			if err := oc.takeErr(); err != nil {
				return err
			}
			// Forwarded by the runtime, and broadcast rather than partitioned:
			// a watermark that reached only one downstream channel would let
			// the others fall behind without any signal.
			if err := broadcast(ctx, outs, e); err != nil {
				return err
			}
		case core.KindEndOfStream:
			if err := op.OnEndOfStream(oc); err != nil {
				return fmt.Errorf("operator %q: end of stream: %w", v.ID, err)
			}
			if err := oc.takeErr(); err != nil {
				return err
			}
			return broadcast(ctx, outs, e)
		default:
			return fmt.Errorf("operator %q: unexpected %s element: barriers arrive in Phase 3", v.ID, e.Kind)
		}
	}
}

func runSink(ctx context.Context, v graph.Vertex, in *transport.Channel) (err error) {
	snk := v.NewSink()
	oc := newOpContext(ctx, nil)
	if err := snk.Open(oc); err != nil {
		return fmt.Errorf("sink %q: open: %w", v.ID, err)
	}
	defer func() {
		if cerr := snk.Close(); cerr != nil && err == nil {
			err = fmt.Errorf("sink %q: close: %w", v.ID, cerr)
		}
	}()

	for {
		e, ok := in.Recv()
		if !ok {
			return ctx.Err()
		}
		switch e.Kind {
		case core.KindRecord:
			if err := snk.Write(e.Record); err != nil {
				return fmt.Errorf("sink %q: write: %w", v.ID, err)
			}
		case core.KindWatermark:
			oc.watermark = e.Watermark
		case core.KindEndOfStream:
			return nil
		default:
			return fmt.Errorf("sink %q: unexpected %s element: barriers arrive in Phase 3", v.ID, e.Kind)
		}
	}
}

// broadcast sends e to every downstream channel. Watermarks and end-of-stream
// go to all of them; only records pick one.
func broadcast(ctx context.Context, outs []*transport.Channel, e core.StreamElement) error {
	for _, out := range outs {
		if err := out.Send(ctx, e); err != nil {
			return err
		}
	}
	return nil
}

// opContext is the runtime's implementation of core.Context.
//
// Emit cannot return an error, so a failed Send is held here and collected by
// the caller after each operator call. Dropping it instead would turn a
// cancelled job into a silently truncated output.
type opContext struct {
	ctx       context.Context
	outputs   []*transport.Channel
	watermark int64
	err       error
}

var _ core.Context = (*opContext)(nil)

func newOpContext(ctx context.Context, outputs []*transport.Channel) *opContext {
	return &opContext{
		ctx:     ctx,
		outputs: outputs,
		// No watermark has been delivered, so nothing is complete yet. Starting
		// at zero would claim that every event before 1970 had arrived.
		watermark: math.MinInt64,
	}
}

// Emit sends rec downstream. A record goes to exactly one channel; Run rejects
// a graph with more than one output per vertex, so there is exactly one to
// choose from until Phase 1 adds the partitioner.
func (c *opContext) Emit(rec *core.Record) {
	if c.err != nil || len(c.outputs) == 0 {
		return
	}
	c.err = c.outputs[0].Send(c.ctx, core.NewRecordElement(rec))
}

// CurrentWatermark returns the last watermark delivered to this subtask.
func (c *opContext) CurrentWatermark() int64 { return c.watermark }

// takeErr returns and clears any error stashed by Emit.
func (c *opContext) takeErr() error {
	err := c.err
	c.err = nil
	return err
}
