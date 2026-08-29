package runtime

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	"github.com/AarinB1/tidemark/pkg/checkpoint"
	"github.com/AarinB1/tidemark/pkg/core"
	"github.com/AarinB1/tidemark/pkg/graph"
	"github.com/AarinB1/tidemark/pkg/state"
	"github.com/AarinB1/tidemark/pkg/transport"
)

// subtaskID identifies one parallel instance of a vertex. It is the unit of
// scheduling, state, and failure.
type subtaskID struct {
	vertexID string
	index    int
}

func (id subtaskID) String() string { return fmt.Sprintf("%s[%d]", id.vertexID, id.index) }

// checkpointKey is this subtask's identity on disk. Two types for one identity
// because pkg/checkpoint must not import the runtime: the on-disk half of a
// subtask's name lives with the format that writes it.
func (id subtaskID) checkpointKey() checkpoint.SubtaskKey {
	return checkpoint.SubtaskKey{VertexID: id.vertexID, Index: id.index}
}

// subtaskConfig is what a subtask needs from the job beyond its own vertex and
// its channels.
//
// A struct rather than another parameter on runSubtask because the restore
// half arrives in the next step and a function with eight positional arguments
// is one where a caller eventually passes them in the wrong order.
type subtaskConfig struct {
	// coordinator is nil when the job is not checkpointing. Barriers still flow
	// in that case -- they are part of the element stream whether or not
	// anybody is recording snapshots -- and every subtask simply has nothing to
	// acknowledge.
	coordinator *checkpoint.Coordinator
	// restore is the payload this subtask snapshotted at the checkpoint being
	// resumed from, and restored says whether there is one. They are separate
	// because a sink's payload is legitimately empty, so a nil slice cannot
	// stand for "not restoring".
	restore  []byte
	restored bool
	// newState makes the keyed state for an operator subtask. nil means the
	// in-memory backend.
	//
	// Only OPERATOR subtasks take their state from here. Sources and sinks get
	// a Memory they never touch: a source's recovery state is its offset and a
	// sink stages nothing until Phase 5, so opening a database per source
	// subtask would be a file handle and a directory bought for nothing. If
	// either ever grows real state, this is the line that has to change with
	// it.
	newState func() (state.KeyedState, error)
	// restoredCheckpoint is the ID of the checkpoint being resumed from, and
	// zero when the job is starting fresh.
	//
	// A source subtask needs it as well as its offset. Checkpoint IDs are
	// contiguous from 1 within a subtask and a subtask injects barrier k as its
	// k-th barrier, so restoring from checkpoint k means exactly k barriers
	// have been injected. Restarting the count at zero would make the resumed
	// run emit a second barrier 1 at a different logical position, and a
	// coordinator counting acknowledgements would be told about two different
	// cuts under one name.
	restoredCheckpoint int64
	// injector is consulted at the three logical positions a subtask can be
	// aborted at, and is nil for every job that is not a chaos run. See
	// FaultInjector.
	injector FaultInjector
}

// makeState returns the keyed state for one operator subtask.
func (c subtaskConfig) makeState() (state.KeyedState, error) {
	if c.newState == nil {
		return state.NewMemory(), nil
	}
	return c.newState()
}

// closableState is a state backend holding resources the runtime must release.
//
// An optional interface, asserted for, rather than a Close on KeyedState: a map
// has nothing to close, and putting the method on the interface would make
// every fake in every test implement a no-op to satisfy it. This is the same
// shape as splittableSource, which the source runner asserts for the same
// reason.
type closableState interface {
	Close() error
}

// checkpointer is a subtask's half of the checkpoint protocol.
//
// It exists so that "the job is not checkpointing" is handled in ONE place. The
// alternative is a nil check at each of the four sites that snapshot, and the
// site that forgot one would panic only in a job with checkpointing switched
// off, which is most of the existing test suite.
type checkpointer struct {
	co  *checkpoint.Coordinator
	key checkpoint.SubtaskKey
}

// enabled reports whether anything is recording checkpoints.
func (c checkpointer) enabled() bool { return c.co != nil }

// snapshot records payload as this subtask's state for checkpoint id.
func (c checkpointer) snapshot(id int64, payload []byte) error {
	if c.co == nil {
		return nil
	}
	return c.co.Acknowledge(id, c.key, payload)
}

// fail abandons checkpoint id because this subtask could not snapshot.
//
// Called when the snapshot itself fails, before anything was handed to the
// coordinator. A failure INSIDE Acknowledge abandons the checkpoint on its own,
// because the coordinator is the one that knows the write did not land.
func (c checkpointer) fail(id int64, cause error) {
	if c.co == nil {
		return
	}
	c.co.Fail(id, c.key, cause)
}

// finished tells the coordinator this subtask will not acknowledge again.
func (c checkpointer) finished(payload []byte) error {
	if c.co == nil {
		return nil
	}
	return c.co.Finished(c.key, payload)
}

// ErrFaultInjected is what a subtask returns when a FaultInjector aborts it.
//
// A sentinel rather than a type, matched with errors.Is, so that a harness can
// tell an abort it asked for apart from a run that failed for some other
// reason. A chaos suite that treated any error as its own fault would report a
// real bug as a successful injection.
var ErrFaultInjected = errors.New("fault injected")

// FaultInjector decides, at three logical positions, whether a subtask should
// abort.
//
// # Why this is an interface
//
// It is the sixth exported interface in this project and CLAUDE.md requires a
// justification for each. It exists so that fault injection does not need a
// second code path through the runtime: without it, "run with faults" would be
// a parallel set of loops in the chaos harness, and the thing under test would
// be that copy rather than the runtime a real job runs on. There are exactly
// two implementations and there will not be a third: nil, which is every job
// that is not a chaos run, and the schedule-driven one in test/chaos.
//
// # Logical position, never wall clock
//
// Every call site is keyed to a count the subtask keeps: records processed,
// barriers forwarded, inputs that have delivered a barrier. None of them
// consults a clock, and none of them can be reached by a path that a timer
// decides. That is invariant 6: Go's scheduler is not deterministic, so a
// fault schedule placed on a clock lands somewhere different on every run and
// a seed stops naming a reproducible failure.
//
// An implementation is called from EVERY subtask goroutine of a job and must
// be safe for concurrent use.
type FaultInjector interface {
	// BeforeElement is consulted before the subtask processes a record, with n
	// the number of records it has already processed in this run. It is
	// therefore called with n = 0 before the first record. Watermarks,
	// barriers and end-of-stream are not records and are not counted.
	//
	// The count restarts at zero in a recovered run, because it is a position
	// within a run rather than within the job: a resumed source begins again
	// from its checkpointed offset, and a resumed operator has processed
	// nothing.
	BeforeElement(vertexID string, subtask int, n int64) bool
	// AfterBarrierForwarded is consulted immediately after the subtask has put
	// barrier checkpointID on its outputs -- and, for a SINK, immediately
	// after it has acknowledged the barrier, which is a sink's whole part in
	// the protocol since it forwards nothing.
	AfterBarrierForwarded(vertexID string, subtask int, checkpointID int64) bool
	// DuringAlignment is consulted when barrier checkpointID has arrived on
	// one of the subtask's inputs, delivered of its live inputs have now
	// delivered it, and at least one live input has not.
	//
	// It is NOT consulted for the barrier that completes alignment. There is
	// no gap left to land in at that point, so offering the decision there
	// would let a schedule aimed at an alignment window record a hit for a
	// fault that fired somewhere else.
	DuringAlignment(vertexID string, subtask int, checkpointID int64, delivered int) bool
}

// faults is a subtask's half of fault injection: the injector, plus the
// identity of the subtask consulting it.
//
// It exists so that "this job injects no faults" is handled in ONE place,
// which is the same reason checkpointer exists and the same shape. The zero
// value injects nothing, so every existing test and every real job runs
// through the same code with a nil injector rather than around it.
//
// It holds no counters. The element count belongs to the loop that does the
// counting, so that the Gate can hold one of these without owning a number it
// never touches.
type faults struct {
	injector FaultInjector
	id       subtaskID
}

// beforeElement consults the injector for the record about to be processed,
// with n the number this subtask has already processed.
func (f faults) beforeElement(n int64) error {
	if f.injector == nil {
		return nil
	}
	if !f.injector.BeforeElement(f.id.vertexID, f.id.index, n) {
		return nil
	}
	return fmt.Errorf("subtask %s: %w after %d elements", f.id, ErrFaultInjected, n)
}

// afterBarrierForwarded consults the injector for the barrier just forwarded.
func (f faults) afterBarrierForwarded(checkpointID int64) error {
	if f.injector == nil {
		return nil
	}
	if !f.injector.AfterBarrierForwarded(f.id.vertexID, f.id.index, checkpointID) {
		return nil
	}
	return fmt.Errorf("subtask %s: %w just after forwarding barrier %d", f.id, ErrFaultInjected, checkpointID)
}

// duringAlignment consults the injector inside an alignment window.
func (f faults) duringAlignment(checkpointID int64, delivered int) error {
	if f.injector == nil {
		return nil
	}
	if !f.injector.DuringAlignment(f.id.vertexID, f.id.index, checkpointID, delivered) {
		return nil
	}
	return fmt.Errorf("subtask %s: %w aligning barrier %d with %d inputs delivered",
		f.id, ErrFaultInjected, checkpointID, delivered)
}

// positionBytes is the width of a source subtask's snapshot payload.
const positionBytes = 8

// encodePosition renders a source subtask's resume offset.
//
// The whole of a source subtask's checkpoint is one integer, and that is what
// contiguous ranges buy: a strided split would have to record a stride and a
// phase beside it. Big-endian, like every other encoding here.
func encodePosition(offset int64) []byte {
	var buf [positionBytes]byte
	binary.BigEndian.PutUint64(buf[:], uint64(offset))
	return buf[:]
}

// decodePosition reads a resume offset written by encodePosition.
func decodePosition(payload []byte) (int64, error) {
	if len(payload) != positionBytes {
		return 0, fmt.Errorf("source position payload is %d bytes, want %d", len(payload), positionBytes)
	}
	return int64(binary.BigEndian.Uint64(payload)), nil
}

// runSubtask runs one parallel instance of v to completion.
//
// It closes the subtask's outputs on every exit path, including a failure, so a
// downstream Recv always unblocks: Recv has no context to watch, and closure is
// the only signal it has. A subtask closes its own outputs and nothing else,
// which is what keeps one producer per channel true at P*Q channels.
//
// gate is nil for a source, which has no inputs, and groups is empty for a
// sink, which has no outputs. groups holds one group of outputs per downstream
// vertex; see transport.NewWriter for what the grouping buys.
func runSubtask(ctx context.Context, v graph.Vertex, index int, gate *Gate, groups [][]transport.Output, cfg subtaskConfig) error {
	id := subtaskID{vertexID: v.ID, index: index}
	w := transport.NewWriter(groups)
	defer w.CloseAll()

	cp := checkpointer{co: cfg.coordinator, key: id.checkpointKey()}
	fs := faults{injector: cfg.injector, id: id}
	switch v.Kind {
	case graph.VertexSource:
		return runSourceSubtask(ctx, v, id, w, cp, fs, cfg)
	case graph.VertexOperator:
		return runOperatorSubtask(ctx, v, id, gate, w, cp, fs, cfg)
	case graph.VertexSink:
		// A sink restores nothing. Its payload is empty by construction, and it
		// will be until the transactional sink in Phase 5 has a staging area to
		// record. Ignored rather than checked, because a sink that found
		// something there could not do anything with it either.
		return runSinkSubtask(ctx, v, id, gate, w, cp, fs)
	default:
		return fmt.Errorf("subtask %s: unknown kind %d", id, uint8(v.Kind))
	}
}

// runSourceSubtask reads this subtask's slice of the offset space and emits it.
func runSourceSubtask(ctx context.Context, v graph.Vertex, id subtaskID, w *transport.Writer, cp checkpointer, fs faults, cfg subtaskConfig) (err error) {
	src := v.NewSource()
	oc := newOpContext(ctx, w)
	if err := src.Open(oc); err != nil {
		return fmt.Errorf("source %s: open: %w", id, err)
	}
	defer func() {
		if cerr := src.Close(); cerr != nil && err == nil {
			err = fmt.Errorf("source %s: close: %w", id, cerr)
		}
	}()

	// elements is this subtask's logical position: the records it has emitted
	// in this run. It is what a fault scheduled at an element count is keyed
	// to, and it is a count rather than a clock for invariant 6's reason.
	var elements int64
	emitRecord := func(rec *core.Record) error {
		if err := fs.beforeElement(elements); err != nil {
			return err
		}
		elements++
		return w.EmitRecord(ctx, rec)
	}
	// Watermarks broadcast; they never partition. A watermark that reached one
	// subtask of a downstream vertex would leave the others with no event-time
	// signal at all, so their windows would sit open until end of input and the
	// gate's minimum would be pinned by the silent ones (invariants 1 and 2).
	emitWatermark := func(t int64) error {
		return w.Broadcast(ctx, core.NewWatermarkElement(t))
	}
	// Barriers broadcast for the same reason watermarks do, and the consequence
	// of getting it wrong is worse. A barrier that reached only some of a
	// downstream vertex's subtasks leaves the others aligning on a checkpoint
	// they will never see completed, and alignment has no timeout: the job stops
	// producing output with no error anywhere (invariants 2 and 3).
	//
	// The SNAPSHOT COMES FIRST. A source is the initiator of a checkpoint, and
	// what it records is the offset it will resume from, which is the position
	// immediately after the last element belonging to this checkpoint. That
	// position is handed in by the loop, captured at the injection point rather
	// than read here: reading it after the broadcast would be reading it after
	// nothing has moved, which is right today and would stop being right the
	// moment anything else touches the source between the two.
	emitBarrier := func(b *core.Barrier, position int64) error {
		if cp.enabled() {
			if err := cp.snapshot(b.CheckpointID, encodePosition(position)); err != nil {
				return fmt.Errorf("checkpoint %d: %w", b.CheckpointID, err)
			}
		}
		if err := w.Broadcast(ctx, core.NewBarrierElement(b)); err != nil {
			return err
		}
		// After the broadcast, so a fault here leaves the barrier downstream and
		// the snapshot already acknowledged. That is the interesting cut: the
		// checkpoint can still complete without this subtask, and the run that
		// recovers from it resumes at the offset this barrier recorded.
		return fs.afterBarrierForwarded(b.CheckpointID)
	}
	wm := newWatermarkGenerator(v.WatermarkIntervalElements, v.MaxOutOfOrderness)

	// A restored source resumes at the offset it recorded and continues its
	// barrier numbering. The watermark generator is NOT restored and does not
	// need to be: it derives its value from the records it sees, and the
	// records it sees from here are the ones it would have seen anyway.
	var resume *sourceResume
	if cfg.restored {
		position, err := decodePosition(cfg.restore)
		if err != nil {
			return fmt.Errorf("source %s: %w", id, err)
		}
		resume = &sourceResume{position: position, barriersInjected: cfg.restoredCheckpoint}
	}

	if err := sourceLoop(ctx, src, v.Parallelism, id.index, wm, v.BarrierIntervalElements, resume, emitRecord, emitWatermark, emitBarrier); err != nil {
		return fmt.Errorf("source %s: %w", id, err)
	}

	// The source is done and will not inject another barrier. Tell the
	// coordinator before broadcasting end-of-stream, so a later barrier from a
	// longer source can complete without waiting for an acknowledgement that
	// will never come. The position is the end of the range: elements past the
	// last barrier already went downstream and belong to whatever checkpoint
	// the remaining sources close next.
	if cp.enabled() {
		if err := cp.finished(encodePosition(src.Position())); err != nil {
			return fmt.Errorf("source %s: %w", id, err)
		}
	}

	// End-of-stream broadcasts. Each downstream subtask needs to know this
	// producer is finished; a partitioned end-of-stream would leave the others
	// waiting on an input that will never speak again.
	return w.Broadcast(ctx, core.NewEndOfStreamElement())
}

func runOperatorSubtask(ctx context.Context, v graph.Vertex, id subtaskID, gate *Gate, w *transport.Writer, cp checkpointer, fs faults, cfg subtaskConfig) (err error) {
	st, err := cfg.makeState()
	if err != nil {
		return fmt.Errorf("operator %s: state: %w", id, err)
	}
	if closable, ok := st.(closableState); ok {
		defer func() {
			if cerr := closable.Close(); cerr != nil && err == nil {
				err = fmt.Errorf("operator %s: state: close: %w", id, cerr)
			}
		}()
	}

	op := v.NewOperator()
	oc := newOpContextWithState(ctx, w, st)
	if err := op.Open(oc); err != nil {
		return fmt.Errorf("operator %s: open: %w", id, err)
	}
	defer func() {
		if cerr := op.Close(); cerr != nil && err == nil {
			err = fmt.Errorf("operator %s: close: %w", id, cerr)
		}
	}()

	// AFTER Open and BEFORE any element, which is the window
	// core.Operator.Restore already documents. Open is where an operator takes
	// the state handle, so restoring earlier would fill a state the operator
	// has not asked for yet; restoring later would fold a checkpointed
	// aggregate into one that has already started accumulating, and
	// state.ReadFrom refuses exactly that.
	if cfg.restored {
		if err := state.ReadFrom(oc.state, bytes.NewReader(cfg.restore)); err != nil {
			return fmt.Errorf("operator %s: restore: %w", id, err)
		}
	}

	return runOperatorLoop(ctx, op, oc, id, gate, w, cp, fs)
}

// runOperatorLoop is the element loop of an operator subtask, split from the
// open and close around it so that a test can drive it with a context whose
// state backend is one that fails on demand. Nothing else calls it.
func runOperatorLoop(ctx context.Context, op core.Operator, oc *opContext, id subtaskID, gate *Gate, w *transport.Writer, cp checkpointer, fs faults) error {
	var elements int64
	for {
		e, ok := gate.Recv()
		if !ok {
			// A fault fired inside the gate's alignment. It is this subtask's
			// failure and it is reported from here, because the gate has no
			// error to return from Recv; everything downstream of it then takes
			// the ordinary path an upstream failure already takes.
			if err := gate.Err(); err != nil {
				return err
			}
			// Every input closed without the gate delivering an end-of-stream:
			// an upstream subtask failed, or the job was cancelled. That
			// goroutine reports the cause, so this one exits quietly.
			return ctx.Err()
		}
		switch e.Kind {
		case core.KindRecord:
			if err := fs.beforeElement(elements); err != nil {
				return err
			}
			elements++
			if err := op.ProcessElement(e.Record, oc); err != nil {
				return fmt.Errorf("operator %s: process: %w", id, err)
			}
			if err := oc.takeErr(); err != nil {
				return err
			}
		case core.KindWatermark:
			oc.watermark = e.Watermark
			if err := op.ProcessWatermark(e.Watermark, oc); err != nil {
				return fmt.Errorf("operator %s: watermark: %w", id, err)
			}
			if err := oc.takeErr(); err != nil {
				return err
			}
			// Forwarded by the runtime, and broadcast rather than partitioned:
			// a watermark that reached only one downstream channel would let
			// the others fall behind without any signal.
			if err := w.Broadcast(ctx, e); err != nil {
				return err
			}
		case core.KindEndOfStream:
			// The gate delivers exactly one of these, after every input has
			// sent one, so OnEndOfStream runs once per subtask rather than once
			// per input.
			if err := op.OnEndOfStream(oc); err != nil {
				return fmt.Errorf("operator %s: end of stream: %w", id, err)
			}
			if err := oc.takeErr(); err != nil {
				return err
			}
			return w.Broadcast(ctx, e)
		case core.KindBarrier:
			// The gate has completed alignment: every live input has delivered
			// this barrier, and everything they sent afterwards is held in the
			// gate's buffers. So the operator's state right now is exactly the
			// records BELOW the barrier on every input, which is the cut this
			// checkpoint records.
			//
			// Snapshot, then acknowledge, THEN forward. That order is
			// Chandy-Lamport and it is not interchangeable. Forwarding first
			// would let a downstream operator record a state that includes
			// effects of elements this operator has not recorded yet: this
			// subtask could process more elements and emit from them while the
			// barrier was already ahead of them downstream, so the downstream
			// snapshot would contain records that the upstream snapshot's
			// resume point will replay. Those records arrive twice on recovery.
			//
			// Draining the gate's buffers stays after all of it, and it does so
			// without this loop arranging anything: the gate queues the barrier
			// for delivery before the elements it releases, so the next Recv is
			// what starts epoch k+1.
			//
			// The state is the RUNTIME's, read straight out of the subtask's
			// context, and core.Operator.Snapshot is not called. An operator's
			// state IS its KeyedState; an operator that kept a second copy
			// somewhere else would checkpoint half of itself.
			if err := snapshotOperatorState(cp, e.Barrier.CheckpointID, oc.state); err != nil {
				return fmt.Errorf("operator %s: %w", id, err)
			}
			if err := w.Broadcast(ctx, e); err != nil {
				return err
			}
			if err := fs.afterBarrierForwarded(e.Barrier.CheckpointID); err != nil {
				return err
			}
		default:
			return fmt.Errorf("operator %s: unexpected %s element", id, e.Kind)
		}
	}
}

func runSinkSubtask(ctx context.Context, v graph.Vertex, id subtaskID, gate *Gate, w *transport.Writer, cp checkpointer, fs faults) (err error) {
	snk := v.NewSink()
	oc := newOpContext(ctx, w)
	if err := snk.Open(oc); err != nil {
		return fmt.Errorf("sink %s: open: %w", id, err)
	}
	defer func() {
		if cerr := snk.Close(); cerr != nil && err == nil {
			err = fmt.Errorf("sink %s: close: %w", id, cerr)
		}
	}()

	var elements int64
	for {
		e, ok := gate.Recv()
		if !ok {
			if err := gate.Err(); err != nil {
				return err
			}
			return ctx.Err()
		}
		switch e.Kind {
		case core.KindRecord:
			if err := fs.beforeElement(elements); err != nil {
				return err
			}
			elements++
			if err := snk.Write(e.Record); err != nil {
				return fmt.Errorf("sink %s: write: %w", id, err)
			}
		case core.KindWatermark:
			oc.watermark = e.Watermark
		case core.KindBarrier:
			// Acknowledged with an EMPTY payload, and nothing is committed. A
			// sink commits on NotifyCheckpointComplete and never at snapshot
			// time, because data committed at snapshot time belongs to a
			// checkpoint that may never complete and comes back as duplicates
			// on recovery (invariant 4). Neither the notification nor the
			// transactional sink exists yet, so there is nothing to record.
			//
			// It still acknowledges. A sink that stayed out of the count would
			// let a checkpoint complete while the sink was arbitrarily far
			// behind the cut, and Phase 5 would have to add a participant to
			// the protocol rather than content to a payload.
			if err := cp.snapshot(e.Barrier.CheckpointID, nil); err != nil {
				return fmt.Errorf("sink %s: checkpoint %d: %w", id, e.Barrier.CheckpointID, err)
			}
			// A sink forwards nothing, so its acknowledgement is the whole of
			// its part in the barrier protocol and is the position this trigger
			// names here. See FaultInjector.AfterBarrierForwarded.
			if err := fs.afterBarrierForwarded(e.Barrier.CheckpointID); err != nil {
				return err
			}
		case core.KindEndOfStream:
			return nil
		default:
			return fmt.Errorf("sink %s: unexpected %s element", id, e.Kind)
		}
	}
}

// snapshotOperatorState serialises st and hands it to the coordinator as this
// subtask's state for checkpoint id.
//
// A snapshot that cannot be taken abandons the checkpoint before the error goes
// back to the subtask, because the subtask is about to stop and will never
// acknowledge: a checkpoint still waiting for it would sit outstanding forever
// while later ones completed on top of it. A failure inside Acknowledge
// abandons it too, from the coordinator, which is the side that knows the write
// did not land.
func snapshotOperatorState(cp checkpointer, id int64, st state.KeyedState) error {
	if !cp.enabled() {
		return nil
	}
	// Buffered rather than streamed to the file, because the coordinator is
	// what owns the on-disk layout and a subtask handing it a reader would be a
	// subtask that decides when the write happens. The cost is one copy of a
	// subtask's state per checkpoint; see the note on state.WriteTo.
	var buf bytes.Buffer
	if err := state.WriteTo(st, &buf); err != nil {
		cp.fail(id, err)
		return fmt.Errorf("checkpoint %d: snapshot: %w", id, err)
	}
	if err := cp.snapshot(id, buf.Bytes()); err != nil {
		return fmt.Errorf("checkpoint %d: %w", id, err)
	}
	return nil
}

// opContext is the runtime's implementation of core.Context.
//
// Emit cannot return an error, so a failed send is held here and collected by
// the caller after each operator call. Dropping it instead would turn a
// cancelled job into a silently truncated output.
type opContext struct {
	ctx       context.Context
	writer    *transport.Writer
	watermark int64
	err       error
	// state is this subtask's keyed state. One per subtask, made here rather
	// than by the operator, because a subtask is the unit of state and the
	// runtime is what decides which backend a job runs on: Memory now, Pebble
	// in Phase 3b. Sources and sinks get one too, and neither uses it; giving
	// them a different Context to keep it away would be a second Context
	// implementation to keep in step for no gain.
	state state.KeyedState
}

var _ core.Context = (*opContext)(nil)

func newOpContext(ctx context.Context, w *transport.Writer) *opContext {
	return newOpContextWithState(ctx, w, state.NewMemory())
}

// newOpContextWithState is newOpContext over a chosen backend. Operator
// subtasks use it; sources and sinks take the memory one above.
func newOpContextWithState(ctx context.Context, w *transport.Writer, st state.KeyedState) *opContext {
	return &opContext{
		ctx:    ctx,
		writer: w,
		state:  st,
		// No watermark has been delivered, so nothing is complete yet. Starting
		// at zero would claim that every event before 1970 had arrived.
		watermark: math.MinInt64,
	}
}

// Emit sends rec downstream, to exactly one of this subtask's outputs, chosen
// by the hash of the record's key.
//
// Once a send has failed every remaining Emit in the same operator call is a
// no-op. The runtime only collects the stash between calls, so an operator
// emitting in a loop would otherwise grind through a send per record that
// cannot succeed, and a later send that did succeed would clear the error
// explaining why the job is stopping.
func (c *opContext) Emit(rec *core.Record) {
	if c.err != nil {
		return
	}
	c.err = c.writer.EmitRecord(c.ctx, rec)
}

// CurrentWatermark returns the last watermark delivered to this subtask.
func (c *opContext) CurrentWatermark() int64 { return c.watermark }

// State returns this subtask's keyed state.
func (c *opContext) State() state.KeyedState { return c.state }

// takeErr returns the first error stashed since the last call: one from a
// failed Emit, or one the state backend recorded.
//
// The two are collected together because they are the same kind of thing. Emit
// has no error return and KeyedState's four operations have none either, so
// both stash and both need somebody to look. The runtime looks after every call
// it makes into the operator, which is the granularity at which a failure is
// still attributable to a record.
//
// The Emit stash is CLEARED and the state's is not. Emit's is per call by
// construction: a send that failed because a downstream channel was momentarily
// gone is the operator call's error and nothing later. A state error is sticky
// on the backend, because every value read after a backend fails is suspect;
// the subtask returns on the first one it sees, so it is read once either way.
//
// Emit's error wins when both are set. It is the one with a cause the reader
// can act on; a state error surfacing in the same call is more likely a
// consequence of the same cancellation.
func (c *opContext) takeErr() error {
	err := c.err
	c.err = nil
	if err != nil {
		return err
	}
	return c.state.Err()
}
