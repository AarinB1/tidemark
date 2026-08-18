package transport

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/AarinB1/tidemark/pkg/core"
)

func TestRoundTripPreservesOrderAndKind(t *testing.T) {
	rec := &core.Record{Key: []byte("k"), Value: []byte("v"), EventTime: 7}
	bar := &core.Barrier{CheckpointID: 3, Timestamp: 9}

	tests := []struct {
		name string
		send []core.StreamElement
	}{
		{
			name: "records only",
			send: []core.StreamElement{
				core.NewRecordElement(rec),
				core.NewRecordElement(rec),
			},
		},
		{
			name: "mixed control and data",
			send: []core.StreamElement{
				core.NewRecordElement(rec),
				core.NewWatermarkElement(11),
				core.NewBarrierElement(bar),
				core.NewRecordElement(rec),
				core.NewEndOfStreamElement(),
			},
		},
		{
			name: "empty",
			send: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := NewChannel(DefaultCapacity)
			for _, e := range tt.send {
				if err := c.Send(context.Background(), e); err != nil {
					t.Fatalf("Send: %v", err)
				}
			}
			c.Close()

			var got []core.StreamElement
			for {
				e, ok := c.Recv()
				if !ok {
					break
				}
				got = append(got, e)
			}
			if len(got) != len(tt.send) {
				t.Fatalf("received %d elements, want %d", len(got), len(tt.send))
			}
			for i := range got {
				if got[i] != tt.send[i] {
					t.Errorf("element %d: got %+v, want %+v", i, got[i], tt.send[i])
				}
			}
			// A second Recv on a drained, closed channel still reports
			// exhaustion rather than blocking.
			if _, ok := c.Recv(); ok {
				t.Error("Recv after close and drain returned ok=true")
			}
		})
	}
}

func TestNewChannelCapacity(t *testing.T) {
	tests := []struct {
		name     string
		capacity int
		want     int
	}{
		{name: "explicit", capacity: 4, want: 4},
		{name: "default", capacity: DefaultCapacity, want: DefaultCapacity},
		{name: "zero falls back to default", capacity: 0, want: DefaultCapacity},
		{name: "negative falls back to default", capacity: -1, want: DefaultCapacity},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := NewChannel(tt.capacity)
			if got := cap(c.elements); got != tt.want {
				t.Errorf("cap = %d, want %d", got, tt.want)
			}
		})
	}
}

// TestSendBlocksWhenFull asserts backpressure. synctest.Wait returns only when
// every other goroutine in the bubble is durably blocked, so "the sender has not
// progressed" is an assertion here rather than a guess after a sleep.
func TestSendBlocksWhenFull(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := NewChannel(2)
		for i := range 2 {
			if err := c.Send(context.Background(), core.NewWatermarkElement(int64(i))); err != nil {
				t.Fatalf("Send %d: %v", i, err)
			}
		}

		var sent atomic.Bool
		go func() {
			if err := c.Send(context.Background(), core.NewWatermarkElement(2)); err != nil {
				t.Errorf("blocked Send: %v", err)
			}
			sent.Store(true)
		}()

		synctest.Wait()
		if sent.Load() {
			t.Fatal("Send returned on a full channel; there is no backpressure")
		}

		if e, ok := c.Recv(); !ok || e.Watermark != 0 {
			t.Fatalf("Recv = (%+v, %v), want the first watermark", e, ok)
		}
		synctest.Wait()
		if !sent.Load() {
			t.Fatal("Send did not complete after the buffer drained")
		}

		// The blocked element is behind the ones already buffered.
		for _, want := range []int64{1, 2} {
			e, ok := c.Recv()
			if !ok || e.Watermark != want {
				t.Fatalf("Recv = (%+v, %v), want watermark %d", e, ok, want)
			}
		}
	})
}

// TestSendReturnsOnContextCancel asserts that a producer blocked in Send unwinds
// when the job is cancelled. Without this the executor could not shut down: a
// source blocked on a full channel to a failed operator would never exit.
func TestSendReturnsOnContextCancel(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := NewChannel(1)
		if err := c.Send(context.Background(), core.NewWatermarkElement(0)); err != nil {
			t.Fatalf("Send: %v", err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		done := make(chan error, 1)
		go func() {
			done <- c.Send(ctx, core.NewWatermarkElement(1))
		}()

		synctest.Wait()
		select {
		case err := <-done:
			t.Fatalf("Send returned %v before the context was cancelled", err)
		default:
		}

		cancel()
		synctest.Wait()

		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("Send returned %v, want context.Canceled", err)
			}
		default:
			t.Fatal("Send is still blocked after cancellation")
		}
	})
}

// TestRecvBlocksUntilSend is the mirror of the backpressure case: a consumer
// waits rather than seeing a zero element.
func TestRecvBlocksUntilSend(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := NewChannel(1)

		var received atomic.Int64
		received.Store(-1)
		done := make(chan struct{})
		go func() {
			defer close(done)
			e, ok := c.Recv()
			if !ok {
				t.Error("Recv on an open channel reported exhaustion")
				return
			}
			received.Store(e.Watermark)
		}()

		synctest.Wait()
		if got := received.Load(); got != -1 {
			t.Fatalf("Recv returned %d before anything was sent", got)
		}

		if err := c.Send(context.Background(), core.NewWatermarkElement(42)); err != nil {
			t.Fatalf("Send: %v", err)
		}
		<-done
		if got := received.Load(); got != 42 {
			t.Fatalf("Recv delivered %d, want 42", got)
		}
	})
}
