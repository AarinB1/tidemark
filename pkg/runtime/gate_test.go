package runtime

import (
	"context"
	"testing"
	"testing/synctest"

	"github.com/AarinB1/tidemark/pkg/core"
	"github.com/AarinB1/tidemark/pkg/transport"
)

// newGateOver builds n channels, hands them to a gate, and returns both.
func newGateOver(ctx context.Context, n, capacity int) (*Gate, []*transport.Channel) {
	chans := make([]*transport.Channel, n)
	inputs := make([]transport.Input, n)
	for i := range chans {
		chans[i] = transport.NewChannel(capacity)
		inputs[i] = chans[i]
	}
	return NewGate(ctx, inputs), chans
}

// drain reads the gate until it reports closure and returns what it delivered.
func drain(g *Gate) []core.StreamElement {
	var out []core.StreamElement
	for {
		e, ok := g.Recv()
		if !ok {
			return out
		}
		out = append(out, e)
	}
}

func recordFor(i int) core.StreamElement {
	return core.NewRecordElement(&core.Record{Key: []byte{byte(i)}, EventTime: int64(i)})
}

// TestGateEmitsEndOfStreamOnlyAfterEveryInput is the assertion the whole gate
// exists for. A subtask with N inputs must call OnEndOfStream once, after the
// last of them is done, and must forward one end-of-stream downstream, not N.
// Reporting it after the first would finish the job on partial data, and
// nothing would fail.
func TestGateEmitsEndOfStreamOnlyAfterEveryInput(t *testing.T) {
	for _, inputs := range []int{1, 2, 4, 8} {
		t.Run(itoa(int64(inputs)), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				g, chans := newGateOver(ctx, inputs, 4)

				delivered := make(chan core.StreamElement, inputs+1)
				go func() {
					for {
						e, ok := g.Recv()
						if !ok {
							close(delivered)
							return
						}
						delivered <- e
					}
				}()

				// Every input but the last delivers end-of-stream. The gate
				// must stay silent: one input still has data to come.
				for i := range inputs - 1 {
					if err := chans[i].Send(ctx, core.NewEndOfStreamElement()); err != nil {
						t.Fatalf("Send: %v", err)
					}
				}
				synctest.Wait()
				select {
				case e, ok := <-delivered:
					t.Fatalf("the gate delivered %v (open=%t) with one input still live", e.Kind, ok)
				default:
				}

				if err := chans[inputs-1].Send(ctx, core.NewEndOfStreamElement()); err != nil {
					t.Fatalf("Send: %v", err)
				}
				synctest.Wait()

				e, ok := <-delivered
				if !ok {
					t.Fatal("the gate closed without delivering end-of-stream")
				}
				if e.Kind != core.KindEndOfStream {
					t.Fatalf("the gate delivered %s, want EndOfStream", e.Kind)
				}

				// Exactly one, not one per input.
				for _, ch := range chans {
					ch.Close()
				}
				synctest.Wait()
				for extra := range delivered {
					t.Errorf("the gate delivered a second %s element", extra.Kind)
				}
			})
		})
	}
}

// TestGateIgnoresARepeatedEndOfStream checks the gate counts inputs rather than
// elements. If it counted, one chatty input could satisfy the gate on behalf of
// a quiet one and the job would finish early on partial data with no error.
func TestGateIgnoresARepeatedEndOfStream(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		g, chans := newGateOver(ctx, 3, 8)

		// Input 0 sends three end-of-stream elements; inputs 1 and 2 send none
		// yet.
		for range 3 {
			if err := chans[0].Send(ctx, core.NewEndOfStreamElement()); err != nil {
				t.Fatalf("Send: %v", err)
			}
		}

		delivered := make(chan core.StreamElement, 4)
		go func() {
			for {
				e, ok := g.Recv()
				if !ok {
					close(delivered)
					return
				}
				delivered <- e
			}
		}()

		synctest.Wait()
		select {
		case e := <-delivered:
			t.Fatalf("three end-of-stream elements on one input produced %s downstream", e.Kind)
		default:
		}

		for _, i := range []int{1, 2} {
			if err := chans[i].Send(ctx, core.NewEndOfStreamElement()); err != nil {
				t.Fatalf("Send: %v", err)
			}
		}
		synctest.Wait()

		e, ok := <-delivered
		if !ok || e.Kind != core.KindEndOfStream {
			t.Fatalf("the gate delivered %v (open=%t), want one EndOfStream", e.Kind, ok)
		}

		// Every producer closes its channel on the way out, so the forwarders
		// have somewhere to finish. Leaving them blocked in Recv would be the
		// test stranding them, not the gate.
		for _, ch := range chans {
			ch.Close()
		}
		for extra := range delivered {
			t.Errorf("the gate delivered a second %s element", extra.Kind)
		}
		g.Wait()
	})
}

// TestGateDeliversRecordsFromEveryInput checks nothing is dropped or duplicated
// in the merge. Arrival order across inputs is not asserted: it is a race
// between forwarders by design, and a test that pinned it would be a broken
// test.
func TestGateDeliversRecordsFromEveryInput(t *testing.T) {
	const (
		inputs         = 4
		perInputRecord = 50
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	g, chans := newGateOver(ctx, inputs, 8)

	for i := range inputs {
		go func() {
			for j := range perInputRecord {
				if err := chans[i].Send(ctx, recordFor(i*perInputRecord+j)); err != nil {
					return
				}
			}
			if err := chans[i].Send(ctx, core.NewEndOfStreamElement()); err != nil {
				return
			}
			chans[i].Close()
		}()
	}

	got := drain(g)
	g.Wait()

	seen := make(map[int64]int)
	ends := 0
	for _, e := range got {
		switch e.Kind {
		case core.KindRecord:
			seen[e.Record.EventTime]++
		case core.KindEndOfStream:
			ends++
		default:
			t.Fatalf("the gate delivered an unexpected %s element", e.Kind)
		}
	}
	if ends != 1 {
		t.Errorf("the gate delivered %d end-of-stream elements, want exactly 1", ends)
	}
	if len(seen) != inputs*perInputRecord {
		t.Errorf("%d distinct records arrived, want %d", len(seen), inputs*perInputRecord)
	}
	for t2, n := range seen {
		if n != 1 {
			t.Errorf("record %d arrived %d times", t2, n)
		}
	}
	// End-of-stream is delivered last: it can only be produced once every
	// input has sent one, and each input sends its records first.
	if len(got) == 0 || got[len(got)-1].Kind != core.KindEndOfStream {
		t.Error("end-of-stream was not the last element the gate delivered")
	}
}

// TestGateCancellationReleasesAForwarderHoldingAnElement covers the failure
// path that the ctx.Done arm of the merged send exists for. When a subtask
// abandons its gate, a forwarder that has already taken an element off its
// input is blocked trying to hand it over; without that arm it stays blocked
// forever, and the leak is invisible outside a test that looks for it.
//
// One input and a merged channel it cannot fit into, so the forwarder is
// certain to be blocked in the send rather than in Recv. The input is never
// closed and still holds an element, so cancellation is the only thing that can
// release it. synctest.Test fails on a deadlock rather than hanging, so Wait
// returning is the assertion.
func TestGateCancellationReleasesAForwarderHoldingAnElement(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		g, chans := newGateOver(ctx, 1, 4)

		for i := range 3 {
			if err := chans[0].Send(ctx, recordFor(i)); err != nil {
				t.Fatalf("Send: %v", err)
			}
		}
		synctest.Wait()

		cancel()
		synctest.Wait()
		g.Wait()

		// The forwarder abandoned its input mid-stream: it stopped where it was
		// rather than draining what remained. The forwarder has exited, so the
		// test is now the only party touching this channel and may close it.
		chans[0].Close()
		left := 0
		for {
			if _, ok := chans[0].Recv(); !ok {
				break
			}
			left++
		}
		if left == 0 {
			t.Error("the forwarder drained its input after cancellation instead of stopping at its blocked send")
		}
	})
}

// TestGateLeavesNoForwarderBehind pins the ordering the executor relies on. A
// forwarder blocked in Recv cannot be released by cancellation, because Recv
// has no context to watch; only its producer closing the input releases it.
// That is why the executor cancels, lets every producing subtask unwind and
// close its outputs, and only then waits.
//
// Cancelling with elements still in flight and only then closing the inputs is
// exactly that sequence, and Wait returning is the assertion that no forwarder
// outlives it.
func TestGateLeavesNoForwarderBehind(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const inputs = 4
		ctx, cancel := context.WithCancel(context.Background())
		g, chans := newGateOver(ctx, inputs, 4)

		// More elements than the merged channel can hold, so some forwarders
		// end up blocked in their send and the rest in Recv. Both cases have to
		// unwind.
		for i := range inputs {
			for j := range 4 {
				if err := chans[i].Send(ctx, recordFor(i*4+j)); err != nil {
					t.Fatalf("Send: %v", err)
				}
			}
		}
		synctest.Wait()

		cancel()
		synctest.Wait()
		for _, ch := range chans {
			ch.Close()
		}
		synctest.Wait()

		g.Wait()

		// The closer goroutine closes merged only once every forwarder has
		// exited, so a drained, closed merged means none is left.
		for {
			if _, ok := <-g.merged; !ok {
				break
			}
		}
	})
}

// TestGateReportsClosureWhenInputsCloseWithoutEndOfStream is the quiet exit: an
// upstream subtask failed, so its channel closed with no end-of-stream on it.
// The gate reports closure rather than synthesising an end-of-stream, because
// the failing subtask is the one that reports and a synthesised end-of-stream
// would tell the sink the job finished successfully.
func TestGateReportsClosureWhenInputsCloseWithoutEndOfStream(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	g, chans := newGateOver(ctx, 3, 4)

	// One input finishes properly, the other two just close.
	if err := chans[0].Send(ctx, core.NewEndOfStreamElement()); err != nil {
		t.Fatalf("Send: %v", err)
	}
	for _, ch := range chans {
		ch.Close()
	}

	for _, e := range drain(g) {
		t.Errorf("the gate delivered a %s element after an upstream failure", e.Kind)
	}
	g.Wait()
}
