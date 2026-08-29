package runtime

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"io/fs"
	"math"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/AarinB1/tidemark/pkg/checkpoint"
	"github.com/AarinB1/tidemark/pkg/core"
	"github.com/AarinB1/tidemark/pkg/graph"
	"github.com/AarinB1/tidemark/pkg/operators"
	"github.com/AarinB1/tidemark/pkg/sinks"
	"github.com/AarinB1/tidemark/pkg/state"
)

// This file pins a DIVERGENCE that exists today. It is not a test of correct
// behaviour and the assertions below are deliberately the wrong way round from
// what a reader might expect, so read this before "fixing" it.
//
// There are two watermarks in a restored operator subtask and they do not
// agree. The runtime keeps its own copy of the last watermark it delivered and
// hands it out through core.Context.CurrentWatermark; that copy starts at
// MinInt64 in every subtask, restored or not, and is written only when a
// watermark is delivered. The window operator keeps its own under
// state.PrefixOperatorState, which IS in the checkpoint. So from the moment a
// subtask is restored until its resumed sources produce a watermark, the two
// report different things, and the operator's lateness rule is driven entirely
// by the second one.
//
// Everywhere else they agree, which is what makes the pair dangerous: two
// sources of truth that agree until a restart are the exact shape of the bug
// this phase was written to fix.
//
// # Why a test and not a comment
//
// Because both of the changes worth catching are silent.
//
// Make the runtime restore its copy and TestContextWatermarkDivergesFromOperatorStateAfterRestore
// fails, which is the correct outcome: that change is an improvement, and it is
// also the moment every operator relying on the state-backed value needs
// re-reading, because the two sources of truth would then agree and the reason
// for preferring one would stop being visible.
//
// Delete the 0x02 write and it fails too, from the other side: the state-backed
// watermark comes back as MinInt64, the divergence closes, and the operator
// silently loses its lateness rule across a restart. The job-level recovery
// suite cannot see that -- see the note on
// operators.TestRestoredWindowRecoversItsWatermark for why the generator's
// bounded out-of-orderness hides it -- so this is where it is caught.
//
// # When this stops being prospective
//
// core.Context.CurrentWatermark has no caller in any operator today, which is
// the only reason the exposure is theoretical, and
// TestContextCurrentWatermarkHasNoOperatorCallers is what keeps that true by
// accident rather than by hope.
//
// The concrete moment it bites is two phases out and not hypothetical. Nexmark
// q5 is a sliding-window count followed by a selection over those counts. That
// second stage is an event-time operator somebody will write from scratch, it
// will need a lateness rule, and core.Context.CurrentWatermark will be sitting
// on the Context it was handed looking like the obvious source. It is not.

// watermarkProbe wraps the window operator and records, at the first call the
// runtime makes into it after a restore, what each of the two watermarks says.
//
// It WRAPS rather than replaces, because the state-backed watermark only exists
// if something writes it, and the thing that writes it is the window operator.
// Every method is forwarded explicitly; embedding core.Operator would compile
// and would leave the forwarding to whatever the embedded nil happened to do.
//
// The capture is on ProcessElement and never on ProcessWatermark, and that is
// load-bearing. The runtime assigns its copy BEFORE it calls ProcessWatermark,
// so a capture there would read the watermark being delivered rather than the
// one the subtask was restored with, and the test would pass against a runtime
// that did restore its copy.
//
// Reading the first delivered element as a record rather than a watermark is
// not an assumption: the gate's output watermark is the minimum across its
// inputs and starts at MinInt64 on every channel, so it cannot advance until
// every channel has produced a watermark, which takes a full watermark interval
// of records. It is asserted below anyway.
type watermarkProbe struct {
	inner *operators.WindowCount

	mu             sync.Mutex
	captured       bool
	sawWatermark   bool
	ctxWatermark   int64
	stateWatermark int64
}

func (p *watermarkProbe) capture(ctx core.Context) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.captured {
		return
	}
	p.captured = true
	p.ctxWatermark = ctx.CurrentWatermark()
	v, ok := ctx.State().Get(append([]byte{state.PrefixOperatorState}, "watermark"...))
	if !ok {
		p.stateWatermark = math.MinInt64
		return
	}
	p.stateWatermark = state.DecodeOrderedInt64(v)
}

func (p *watermarkProbe) Open(ctx core.Context) error { return p.inner.Open(ctx) }

func (p *watermarkProbe) ProcessElement(rec *core.Record, ctx core.Context) error {
	p.capture(ctx)
	return p.inner.ProcessElement(rec, ctx)
}

func (p *watermarkProbe) ProcessWatermark(wm int64, ctx core.Context) error {
	p.mu.Lock()
	if !p.captured {
		p.sawWatermark = true
	}
	p.mu.Unlock()
	return p.inner.ProcessWatermark(wm, ctx)
}

func (p *watermarkProbe) OnEndOfStream(ctx core.Context) error { return p.inner.OnEndOfStream(ctx) }
func (p *watermarkProbe) Snapshot(w io.Writer) error           { return p.inner.Snapshot(w) }
func (p *watermarkProbe) Restore(r io.Reader) error            { return p.inner.Restore(r) }
func (p *watermarkProbe) Close() error                         { return p.inner.Close() }

var _ core.Operator = (*watermarkProbe)(nil)

// TestContextWatermarkDivergesFromOperatorStateAfterRestore asserts that the
// runtime's watermark and the operator's state-backed one DISAGREE in a
// genuinely restored subtask.
//
// It runs a real job through RunWithOptions and observes from inside the
// restored operator, rather than reproducing the restore by hand. That
// distinction is the whole value of the test: a hand-rolled restore observes
// only what the test itself did, so it would keep passing after somebody
// changed the runtime's restore path, which is precisely the change this is
// here to catch.
func TestContextWatermarkDivergesFromOperatorStateAfterRestore(t *testing.T) {
	forEachStateBackend(t, testContextWatermarkDivergesFromOperatorStateAfterRestore)
}

func testContextWatermarkDivergesFromOperatorStateAfterRestore(t *testing.T, backend stateBackend) {
	const (
		count = 12000
		// Does not divide the range, so the last checkpoint leaves 2000
		// elements to replay. At an exact multiple the resumed run would
		// resume at the end, deliver nothing, and the probe would never fire.
		barrierInterval = 5000
		parallelism     = 1
	)
	root := t.TempDir()
	cfg := restoreConfig(8, count)

	if err := RunWithOptions(context.Background(),
		windowGraph(t, sinks.NewCollect(), parallelism, &windowFactory{},
			windowSourceVertex("src", cfg, parallelism, barrierInterval)),
		Options{CheckpointRoot: root, Seed: cfg.Seed, NewState: backend.newState}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if _, ok, err := checkpoint.NewStorage(root).Latest(); err != nil || !ok {
		t.Fatalf("Latest = (ok %t, err %v), want a complete checkpoint", ok, err)
	}

	probe := &watermarkProbe{inner: operators.NewTumblingCount(recoveryWindowSize, recoveryWindowLateness)}
	g := buildGraph(t,
		[]graph.Vertex{
			windowSourceVertex("src", cfg, parallelism, barrierInterval),
			{ID: "window", Kind: graph.VertexOperator, Parallelism: parallelism,
				NewOperator: func() core.Operator { return probe }},
			{ID: "out", Kind: graph.VertexSink, Parallelism: parallelism,
				NewSink: func() core.Sink { return sinks.NewCollect() }},
		},
		[][2]string{{"src", "window"}, {"window", "out"}},
	)
	if err := RunWithOptions(context.Background(), g,
		Options{RestoreFrom: root, Seed: cfg.Seed, NewState: backend.newState}); err != nil {
		t.Fatalf("restored Run: %v", err)
	}

	probe.mu.Lock()
	defer probe.mu.Unlock()

	if !probe.captured {
		t.Fatal("the restored operator was never given a record, so nothing was observed: the resume point left nothing to replay")
	}
	if probe.sawWatermark {
		t.Fatal("a watermark reached the restored operator before any record, so the capture would read the watermark being " +
			"delivered rather than the one the subtask was restored with")
	}

	// Guard first. If the 0x02 write is gone the state-backed watermark is
	// MinInt64 too, the two agree, and every assertion below would pass for the
	// wrong reason: a divergence test is vacuous the moment both sides hold the
	// same value.
	if probe.stateWatermark == math.MinInt64 {
		t.Fatal("the restored operator's state carries no watermark, so there is no divergence to pin: " +
			"the 0x02 write is gone, and WindowCount has silently lost its lateness rule across a restart")
	}

	// The divergence itself.
	if probe.ctxWatermark != math.MinInt64 {
		t.Errorf("core.Context.CurrentWatermark in a restored subtask = %d, want MinInt64 (%d).\n"+
			"The runtime now restores its own copy of the watermark. That is an improvement, and it means the two "+
			"sources of truth agree again -- so every operator that keeps its watermark in KeyedState to survive a "+
			"restart should be re-read, and the reason for preferring the state-backed value documented on "+
			"operators.WindowCount.currentWatermark needs updating.", probe.ctxWatermark, int64(math.MinInt64))
	}
	if probe.ctxWatermark == probe.stateWatermark {
		t.Errorf("both watermarks read %d in a restored subtask; this test exists because they do not agree", probe.stateWatermark)
	}
	t.Logf("restored subtask: core.Context.CurrentWatermark = %d, state-backed watermark = %d",
		probe.ctxWatermark, probe.stateWatermark)
}

// TestContextCurrentWatermarkHasNoOperatorCallers is the census that makes the
// exposure above prospective rather than live.
//
// Nothing in this engine calls core.Context.CurrentWatermark outside a test
// observer, so the divergence cannot currently produce a wrong answer. That is
// a fact about today rather than a property of the design, and it is asserted
// here so that the first operator to reach for it trips this test and is sent
// to the file that explains what the value actually is.
//
// It parses rather than greps, so the interface declaration in pkg/core and the
// method definitions on the Context implementations do not count: only a call
// through a selector does.
func TestContextCurrentWatermarkHasNoOperatorCallers(t *testing.T) {
	// Callers that exist today and are not operators. Each is an observer in a
	// test, watching what the runtime delivered; none drives a decision.
	allowed := map[string]string{
		filepath.Join("test", "oracle", "equivalence_test.go"):          "records the watermark each firing was observed at",
		filepath.Join("pkg", "runtime", "executor_test.go"):             "asserts a fresh opContext starts at MinInt64",
		filepath.Join("pkg", "runtime", "watermark_divergence_test.go"): "this file",
	}

	// Absolute, because the hidden-directory check below compares names and the
	// relative form of the repository root is "..", whose name begins with a
	// dot and would skip the entire walk on its first call.
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("resolving the repository root: %v", err)
	}
	found := make(map[string]bool)
	fset := token.NewFileSet()
	err = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if path != root && strings.HasPrefix(d.Name(), ".") {
				return fs.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		f, parseErr := parser.ParseFile(fset, path, nil, 0)
		if parseErr != nil {
			return parseErr
		}
		rel, relErr := filepath.Rel(root, path)
		if relErr != nil {
			return relErr
		}
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			if sel.Sel.Name == "CurrentWatermark" {
				found[rel] = true
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("walking the tree: %v", err)
	}

	// Not vacuous: the scanner must actually find the callers that are known to
	// be there. A parse that matched nothing would otherwise pass forever.
	for path := range allowed {
		if !found[path] {
			t.Errorf("the scan did not find the known call in %s, so it is not looking where it thinks it is", path)
		}
	}

	for path := range found {
		if _, ok := allowed[path]; ok {
			continue
		}
		t.Errorf("%s calls core.Context.CurrentWatermark.\n"+
			"That value is the RUNTIME's copy of the last delivered watermark and it is NOT RESTORED: "+
			"it reads MinInt64 from the moment a subtask is restored until its resumed sources produce a watermark. "+
			"An operator with a lateness or purge rule must keep its own watermark in its KeyedState and read it "+
			"from there, as operators.WindowCount does under state.PrefixOperatorState. "+
			"See TestContextWatermarkDivergesFromOperatorStateAfterRestore in this file. "+
			"If this call really is only an observer, add it to the allow list above with a note saying so.", path)
	}
}
