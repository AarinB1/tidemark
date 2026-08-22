package runtime

import (
	"context"
	"math"
	"slices"
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

				delivered := make(chan core.StreamElement, inputs+2)
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

				// The flush comes first: MaxInt64 is what fires the windows
				// still open when the input runs out, and it has to precede the
				// end-of-stream so a window operator has somewhere to put them
				// other than OnEndOfStream.
				e, ok := <-delivered
				if !ok {
					t.Fatal("the gate closed without delivering the end-of-input flush")
				}
				if e.Kind != core.KindWatermark || e.Watermark != math.MaxInt64 {
					t.Fatalf("the gate delivered %s (%d), want a MaxInt64 watermark", e.Kind, e.Watermark)
				}

				e, ok = <-delivered
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

		delivered := make(chan core.StreamElement, 8)
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
		if !ok || e.Kind != core.KindWatermark || e.Watermark != math.MaxInt64 {
			t.Fatalf("the gate delivered %v (open=%t), want the MaxInt64 end-of-input flush", e.Kind, ok)
		}
		e, ok = <-delivered
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
	ends, flushes := 0, 0
	for _, e := range got {
		switch e.Kind {
		case core.KindRecord:
			seen[e.Record.EventTime]++
		case core.KindEndOfStream:
			ends++
		case core.KindWatermark:
			// No input sent a watermark, so the only one that can appear here
			// is the end-of-input flush.
			if e.Watermark != math.MaxInt64 {
				t.Errorf("the gate delivered watermark %d, but no input sent one", e.Watermark)
			}
			flushes++
		default:
			t.Fatalf("the gate delivered an unexpected %s element", e.Kind)
		}
	}
	if ends != 1 {
		t.Errorf("the gate delivered %d end-of-stream elements, want exactly 1", ends)
	}
	if flushes != 1 {
		t.Errorf("the gate delivered %d end-of-input flushes, want exactly 1", flushes)
	}
	if len(seen) != inputs*perInputRecord {
		t.Errorf("%d distinct records arrived, want %d", len(seen), inputs*perInputRecord)
	}
	for t2, n := range seen {
		if n != 1 {
			t.Errorf("record %d arrived %d times", t2, n)
		}
	}
	// The flush and then end-of-stream are the last two, in that order. Both
	// can only be produced once every input has sent an end-of-stream, and each
	// input sends its records first.
	if len(got) < 2 || got[len(got)-1].Kind != core.KindEndOfStream {
		t.Fatal("end-of-stream was not the last element the gate delivered")
	}
	if got[len(got)-2].Kind != core.KindWatermark {
		t.Errorf("the element before end-of-stream is a %s, want the MaxInt64 flush", got[len(got)-2].Kind)
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

// gateHarness feeds a gate one element at a time and collects what it delivers.
//
// One element in flight at a time, with a synctest.Wait after each, is what
// makes the assertions below able to name exact values. The merge between
// forwarders is a genuine race by design; feeding it serially does not test any
// less, because the watermark minimum is a function of the per-input state and
// not of arrival order, and it is the only way the expected output of a
// three-input sequence is a single fixed list.
//
// It must run inside synctest.Test.
type gateHarness struct {
	t         *testing.T
	ctx       context.Context
	g         *Gate
	chans     []*transport.Channel
	delivered chan core.StreamElement
}

func newGateHarness(t *testing.T, ctx context.Context, inputs int) *gateHarness {
	t.Helper()
	g, chans := newGateOver(ctx, inputs, 4)
	h := &gateHarness{t: t, ctx: ctx, g: g, chans: chans, delivered: make(chan core.StreamElement, 64)}
	go func() {
		for {
			e, ok := g.Recv()
			if !ok {
				close(h.delivered)
				return
			}
			h.delivered <- e
		}
	}()
	return h
}

// send puts one element on input i and returns everything the gate produced in
// response, which is usually nothing.
func (h *gateHarness) send(input int, e core.StreamElement) []core.StreamElement {
	h.t.Helper()
	if err := h.chans[input].Send(h.ctx, e); err != nil {
		h.t.Fatalf("Send on input %d: %v", input, err)
	}
	synctest.Wait()
	return h.take()
}

func (h *gateHarness) sendWatermark(input int, wm int64) []core.StreamElement {
	h.t.Helper()
	return h.send(input, core.NewWatermarkElement(wm))
}

func (h *gateHarness) sendEndOfStream(input int) []core.StreamElement {
	h.t.Helper()
	return h.send(input, core.NewEndOfStreamElement())
}

// take drains whatever the gate has already delivered without blocking.
func (h *gateHarness) take() []core.StreamElement {
	var out []core.StreamElement
	for {
		select {
		case e, ok := <-h.delivered:
			if !ok {
				return out
			}
			out = append(out, e)
		default:
			return out
		}
	}
}

// finish closes every input, which is what lets the forwarders exit, and
// returns anything the gate delivered on the way out.
func (h *gateHarness) finish() []core.StreamElement {
	h.t.Helper()
	for _, ch := range h.chans {
		ch.Close()
	}
	synctest.Wait()
	h.g.Wait()
	var out []core.StreamElement
	for e := range h.delivered {
		out = append(out, e)
	}
	return out
}

// watermarkValues reduces delivered elements to their watermark values, failing
// on anything that is not a watermark. Assertions on exact values need the
// values; assertions that "a watermark arrived" pass under a maximum.
func watermarkValues(t *testing.T, elems []core.StreamElement) []int64 {
	t.Helper()
	out := make([]int64, 0, len(elems))
	for _, e := range elems {
		if e.Kind != core.KindWatermark {
			t.Fatalf("the gate delivered a %s element where a watermark was expected", e.Kind)
		}
		out = append(out, e.Watermark)
	}
	return out
}

// TestGateWatermarkIsTheMinimumNotTheMaximum is invariant 1, on the only kind
// of topology where the two can differ.
//
// Three inputs reaching 100, 200 and 300 must move the output to 100. A gate
// taking the maximum emits 100, then 200, then 300 over the same sequence, so
// the assertion is the exact list of what came out and not that "a watermark
// arrived": every weaker form of this test passes under the bug. A single-input
// chain cannot distinguish the two at all, because with one input the minimum
// and the maximum are the same number, which is why every row here has at least
// two inputs.
//
// Firing on the maximum declares event time complete as soon as the fastest
// input says so, so windows fire on one input's data and the counts come out
// low. Nothing crashes and no test that only counts elements notices.
func TestGateWatermarkIsTheMinimumNotTheMaximum(t *testing.T) {
	type arrival struct {
		input int
		wm    int64
	}
	tests := []struct {
		name     string
		inputs   int
		arrivals []arrival
		// want is the complete sequence of watermarks the gate must forward.
		want []int64
		// wantUnderMax is what a gate taking the maximum would forward. It is
		// asserted to differ from want, so a row that could not tell the two
		// apart fails as a broken test rather than passing as evidence.
		wantUnderMax []int64
	}{
		{
			name:         "the minimum of three, not the maximum",
			inputs:       3,
			arrivals:     []arrival{{0, 100}, {1, 200}, {2, 300}},
			want:         []int64{100},
			wantUnderMax: []int64{100, 200, 300},
		},
		{
			// Nothing is complete until every input has said something. An
			// input that has not spoken sits at MinInt64 and pins the minimum.
			name:         "one silent input holds the output back",
			inputs:       3,
			arrivals:     []arrival{{0, 500}, {1, 600}},
			want:         nil,
			wantUnderMax: []int64{500, 600},
		},
		{
			name:         "the laggard advancing moves the output to the next minimum",
			inputs:       3,
			arrivals:     []arrival{{0, 100}, {1, 200}, {2, 300}, {0, 250}},
			want:         []int64{100, 200},
			wantUnderMax: []int64{100, 200, 300},
		},
		{
			// The output tracks the minimum, so advancing an input that is not
			// the minimum changes nothing at all.
			name:         "advancing an input that is not the minimum emits nothing",
			inputs:       3,
			arrivals:     []arrival{{0, 100}, {1, 200}, {2, 300}, {2, 9000}},
			want:         []int64{100},
			wantUnderMax: []int64{100, 200, 300, 9000},
		},
		{
			name:         "two inputs",
			inputs:       2,
			arrivals:     []arrival{{0, 1000}, {1, 10}, {1, 20}, {1, 30}},
			want:         []int64{10, 20, 30},
			wantUnderMax: []int64{1000},
		},
		{
			// A decreasing arrival is a contract violation upstream. The gate
			// records it, so the minimum genuinely drops, and the monotonicity
			// guard is what stops the drop from reaching the operator, which
			// core.Operator.ProcessWatermark promises never sees one.
			name:         "a decreasing arrival does not decrease the output",
			inputs:       2,
			arrivals:     []arrival{{0, 100}, {1, 200}, {0, 60}, {1, 210}},
			want:         []int64{100},
			wantUnderMax: []int64{100, 200, 210},
		},
		{
			// A repeated value is not an advance either. The repeats of 100 and
			// 200 here produce nothing; only the arrival that actually moves
			// the minimum does.
			name:         "a repeated watermark emits nothing",
			inputs:       2,
			arrivals:     []arrival{{0, 100}, {1, 200}, {0, 100}, {1, 200}, {0, 300}},
			want:         []int64{100, 200},
			wantUnderMax: []int64{100, 200, 300},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if slices.Equal(tt.want, tt.wantUnderMax) {
				t.Fatalf("this row expects %v under both a minimum and a maximum and is not evidence for either", tt.want)
			}
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				h := newGateHarness(t, ctx, tt.inputs)

				var got []int64
				for _, a := range tt.arrivals {
					got = append(got, watermarkValues(t, h.sendWatermark(a.input, a.wm))...)
				}
				if !slices.Equal(got, tt.want) {
					t.Errorf("the gate forwarded %v, want %v (a maximum would give %v)", got, tt.want, tt.wantUnderMax)
				}

				// Everything left on the way out is the end-of-input flush,
				// which the next test covers. Draining it is what lets the
				// forwarders exit.
				h.finish()
			})
		})
	}
}

// TestGateExcludesAnExhaustedInputFromTheMinimum is the other half of invariant
// 1 and the one whose failure is hardest to read.
//
// An input that has delivered end-of-stream is out of the minimum, not frozen
// at its last value. Frozen, an input that stopped at 50 pins the output there
// for the rest of the run: every window past 50 stays open, the tail of the
// output is simply missing from the sink, and the symptom reads like a window
// assignment bug.
//
// Two inputs at least, and the exhausted one must be the current minimum.
// Retiring an input that was not the minimum changes nothing, so a test built
// that way passes whether the input is excluded or frozen.
func TestGateExcludesAnExhaustedInputFromTheMinimum(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		h := newGateHarness(t, ctx, 3)

		if got := watermarkValues(t, h.sendWatermark(0, 50)); len(got) != 0 {
			t.Fatalf("the gate forwarded %v with two inputs still silent", got)
		}
		if got := watermarkValues(t, h.sendWatermark(1, 200)); len(got) != 0 {
			t.Fatalf("the gate forwarded %v with one input still silent", got)
		}
		if got := watermarkValues(t, h.sendWatermark(2, 300)); !slices.Equal(got, []int64{50}) {
			t.Fatalf("the gate forwarded %v, want [50]", got)
		}

		// Input 0, the minimum, finishes. The output must jump to the minimum
		// of what is left. Frozen at 50 it would emit nothing here.
		if got := watermarkValues(t, h.sendEndOfStream(0)); !slices.Equal(got, []int64{200}) {
			t.Fatalf("after the input at 50 finished the gate forwarded %v, want [200]", got)
		}

		// And again, so this is the rule rather than one lucky recomputation.
		if got := watermarkValues(t, h.sendEndOfStream(1)); !slices.Equal(got, []int64{300}) {
			t.Fatalf("after the input at 200 finished the gate forwarded %v, want [300]", got)
		}

		// The last input finishing takes the output to MaxInt64 and closes the
		// gate, which is the next test's subject.
		h.sendEndOfStream(2)
		h.finish()
	})
}

// TestGateFlushesWithMaxInt64ThenOneEndOfStream pins the end-of-input sequence.
//
// The MaxInt64 is the only thing in the engine that flushes windows still open
// when the input runs out: no source emits one, deliberately, so that there is
// exactly one mechanism and it is exercised by every run. It has to arrive
// BEFORE the end-of-stream, because a window operator flushing on
// OnEndOfStream instead would be a second mechanism for the same job.
//
// Exactly one end-of-stream, however many inputs: a subtask calls OnEndOfStream
// once, not once per input.
func TestGateFlushesWithMaxInt64ThenOneEndOfStream(t *testing.T) {
	tests := []struct {
		name string
		// prior is a watermark sent on every input before the end-of-stream
		// round, or nil for none.
		prior *int64
		want  []core.ElementKind
	}{
		{
			name: "no watermarks at all",
			want: []core.ElementKind{core.KindWatermark, core.KindEndOfStream},
		},
		{
			name:  "after real watermarks",
			prior: ptr(int64(1000)),
			want:  []core.ElementKind{core.KindWatermark, core.KindEndOfStream},
		},
		{
			// Already at MaxInt64, so the flush would not advance anything and
			// is not sent. The monotonicity rule is uniform rather than having
			// an exception carved out for the last watermark.
			name:  "already flushed by the inputs themselves",
			prior: ptr(int64(math.MaxInt64)),
			want:  []core.ElementKind{core.KindEndOfStream},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, inputs := range []int{2, 4} {
				t.Run(itoa(int64(inputs)), func(t *testing.T) {
					synctest.Test(t, func(t *testing.T) {
						ctx, cancel := context.WithCancel(context.Background())
						defer cancel()
						h := newGateHarness(t, ctx, inputs)

						if tt.prior != nil {
							for i := range inputs {
								h.sendWatermark(i, *tt.prior)
							}
						}

						var got []core.StreamElement
						for i := range inputs {
							out := h.sendEndOfStream(i)
							if i < inputs-1 && len(out) != 0 {
								t.Fatalf("the gate delivered %d elements with %d inputs still live",
									len(out), inputs-1-i)
							}
							got = append(got, out...)
						}
						got = append(got, h.finish()...)

						var kinds []core.ElementKind
						for _, e := range got {
							kinds = append(kinds, e.Kind)
						}
						if !slices.Equal(kinds, tt.want) {
							t.Fatalf("the gate delivered %v, want %v", kinds, tt.want)
						}
						if kinds[0] == core.KindWatermark && got[0].Watermark != math.MaxInt64 {
							t.Errorf("the end-of-input watermark is %d, want math.MaxInt64", got[0].Watermark)
						}
					})
				})
			}
		})
	}
}

// TestGateKeepsRecordsInBandWithWatermarks checks that adding the watermark
// path did not turn records into something the gate absorbs, and that the two
// stay in the order they arrived in on a single input.
//
// Invariant 5 is why they share one channel. Order across inputs is a race by
// design and is not asserted; order within one input is the guarantee.
func TestGateKeepsRecordsInBandWithWatermarks(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		h := newGateHarness(t, ctx, 2)

		// Input 1 is held at a high watermark throughout so that input 0's
		// arrivals are always the minimum and always forwarded.
		h.sendWatermark(1, 1_000_000)

		var got []core.StreamElement
		got = append(got, h.send(0, recordFor(1))...)
		got = append(got, h.sendWatermark(0, 10)...)
		got = append(got, h.send(0, recordFor(2))...)
		got = append(got, h.send(0, recordFor(3))...)
		got = append(got, h.sendWatermark(0, 20)...)

		want := []struct {
			kind  core.ElementKind
			value int64
		}{
			{core.KindRecord, 1},
			{core.KindWatermark, 10},
			{core.KindRecord, 2},
			{core.KindRecord, 3},
			{core.KindWatermark, 20},
		}
		if len(got) != len(want) {
			t.Fatalf("the gate delivered %d elements, want %d", len(got), len(want))
		}
		for i, w := range want {
			if got[i].Kind != w.kind {
				t.Fatalf("element %d is a %s, want %s", i, got[i].Kind, w.kind)
			}
			value := got[i].Watermark
			if w.kind == core.KindRecord {
				value = got[i].Record.EventTime
			}
			if value != w.value {
				t.Errorf("element %d carries %d, want %d", i, value, w.value)
			}
		}

		h.finish()
	})
}

func ptr[T any](v T) *T { return &v }
