package operators

import "container/heap"

// timerKey identifies a registered timer. Deduplication is on (key,
// windowStart) and deliberately not on the fire time: for a given window
// specification the fire time is a function of the window start, so a second
// registration for the same pair carries the same fire time and is the same
// timer.
type timerKey struct {
	key         string
	windowStart int64
}

// eventTimer is one pending firing.
type eventTimer struct {
	fireTime    int64
	key         string
	windowStart int64
}

// timerQueue is the container/heap half of timerService.
//
// Less is a TOTAL order, not just an order on fire time. container/heap is not
// stable, so two timers that compare equal come out in whatever order the sift
// happened to leave them in, which varies with the insertion sequence and
// therefore with how the shuffle interleaved records upstream. Downstream that
// shows up as a test that passes on most runs, and the reason looks like a
// concurrency bug rather than a comparison that was one field short. This is
// the same trap graph.kahn's lexicographic tie-break avoids.
//
// Since (key, windowStart) is unique in the queue, comparing fire time, then
// key, then window start leaves no pair equal at all.
type timerQueue []eventTimer

func (q timerQueue) Len() int { return len(q) }

func (q timerQueue) Less(i, j int) bool {
	if q[i].fireTime != q[j].fireTime {
		return q[i].fireTime < q[j].fireTime
	}
	// Go compares strings bytewise, which is the documented tie-break: keys are
	// byte slices and their order must not depend on any text encoding.
	if q[i].key != q[j].key {
		return q[i].key < q[j].key
	}
	return q[i].windowStart < q[j].windowStart
}

func (q timerQueue) Swap(i, j int) { q[i], q[j] = q[j], q[i] }

func (q *timerQueue) Push(x any) { *q = append(*q, x.(eventTimer)) }

func (q *timerQueue) Pop() any {
	old := *q
	n := len(old)
	t := old[n-1]
	*q = old[:n-1]
	return t
}

// timerService holds the event-time timers of one operator subtask.
//
// A heap plus a map: the heap answers "what fires next" in log n, and the map
// answers "is this pair already waiting" in constant time. Without the map a
// window with a thousand records would hold a thousand identical timers and
// emit its result a thousand times; without the heap, finding the due ones
// would be a scan of every open window on every watermark.
//
// It lives in pkg/operators rather than pkg/runtime, where the plan put it,
// because pkg/operators cannot import pkg/runtime: three of pkg/runtime's test
// files are in package runtime and import pkg/operators, so the reverse edge is
// an import cycle in the test build. The window operator fires its own windows
// inside ProcessWatermark, so it needs the type where it can reach it. The
// alternative, moving timers into the runtime and firing them into the operator
// through the Context, needs a new method on core.Context and another on
// core.Operator, and grows two of the four interfaces CLAUDE.md counts.
//
// One subtask owns one service and calls it from one goroutine. No locking.
type timerService struct {
	queue      timerQueue
	registered map[timerKey]struct{}
}

func newTimerService() *timerService {
	return &timerService{registered: make(map[timerKey]struct{})}
}

// Register schedules a firing for (key, windowStart) at fireTime. A pair
// already waiting is left alone: the first registration wins, and since the
// fire time is derived from the window start, the second would be identical
// anyway.
func (s *timerService) Register(fireTime int64, key string, windowStart int64) {
	k := timerKey{key: key, windowStart: windowStart}
	if _, ok := s.registered[k]; ok {
		return
	}
	s.registered[k] = struct{}{}
	heap.Push(&s.queue, eventTimer{fireTime: fireTime, key: key, windowStart: windowStart})
}

// Advance fires every timer whose fire time is at or before w, in
// (fireTime, key, windowStart) order, and stops at the first error from fire.
//
// A fired timer is deregistered before fire runs, so fire may register the same
// pair again. That is the late-record path: a record arriving for a window that
// has already fired re-registers it, and the re-fire happens on the NEXT
// watermark rather than inside this loop. fire must not register a timer at or
// before w, which would make this loop fire it again immediately and, for an
// operator that registered on every firing, forever.
func (s *timerService) Advance(w int64, fire func(key string, windowStart int64) error) error {
	for len(s.queue) > 0 && s.queue[0].fireTime <= w {
		t := heap.Pop(&s.queue).(eventTimer)
		delete(s.registered, timerKey{key: t.key, windowStart: t.windowStart})
		if err := fire(t.key, t.windowStart); err != nil {
			return err
		}
	}
	return nil
}

// Len returns the number of timers still waiting.
func (s *timerService) Len() int { return len(s.queue) }
