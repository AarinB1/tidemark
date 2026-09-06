package main

import (
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// referenceFingerprint is a machine that is not this one, used wherever a test
// needs two machines that must not match.
func referenceFingerprint() Fingerprint {
	return Fingerprint{
		CPUModel:       "AMD EPYC 9R14",
		NumCPU:         16,
		MemoryBytes:    33285996544,
		DiskRotational: diskSolidState,
		GoVersion:      "go1.25.0",
		GOMAXPROCS:     16,
	}
}

func TestParseCPUModel(t *testing.T) {
	tests := []struct {
		name    string
		cpuinfo string
		want    string
	}{
		{
			name:    "x86 model name",
			cpuinfo: "processor\t: 0\nvendor_id\t: GenuineIntel\nmodel name\t: Intel(R) Xeon(R) Processor @ 2.80GHz\ncpu MHz\t: 2800.000\n",
			want:    "Intel(R) Xeon(R) Processor @ 2.80GHz",
		},
		{
			// An ARM kernel writes "Model" and no "model name", and an empty
			// answer there would make every ARM machine match every other one.
			name:    "arm Model",
			cpuinfo: "processor\t: 0\nBogoMIPS\t: 50.00\n\nModel\t\t: Raspberry Pi 5 Model B Rev 1.0\n",
			want:    "Raspberry Pi 5 Model B Rev 1.0",
		},
		{
			name:    "first of several wins",
			cpuinfo: "model name\t: little core\nmodel name\t: big core\n",
			want:    "little core",
		},
		{
			name:    "nothing recognisable",
			cpuinfo: "processor\t: 0\nflags\t\t: fpu vme\n",
			want:    "",
		},
		{
			name:    "empty",
			cpuinfo: "",
			want:    "",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := parseCPUModel(tc.cpuinfo); got != tc.want {
				t.Errorf("parseCPUModel = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestParseMemTotal(t *testing.T) {
	tests := []struct {
		name    string
		meminfo string
		want    int64
	}{
		{
			name:    "kB is kibibytes",
			meminfo: "MemTotal:       16461028 kB\nMemFree:         1234 kB\n",
			want:    16461028 * 1024,
		},
		{
			name:    "no unit is bytes",
			meminfo: "MemTotal:       1024\n",
			want:    1024,
		},
		{
			// MemAvailable is not MemTotal: free memory moves minute to minute
			// and a fingerprint built on it would not match itself.
			name:    "only MemAvailable",
			meminfo: "MemAvailable:   9999 kB\n",
			want:    0,
		},
		{
			name:    "unparseable",
			meminfo: "MemTotal:       lots kB\n",
			want:    0,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := parseMemTotal(tc.meminfo); got != tc.want {
				t.Errorf("parseMemTotal = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestDeviceForPath(t *testing.T) {
	const mountinfo = "22 27 0:21 / /proc rw,relatime - proc proc rw\n" +
		"27 1 254:0 / / rw,relatime - ext4 /dev/vda rw\n" +
		"31 27 254:16 / /home rw,relatime - ext4 /dev/vdb rw\n" +
		"33 27 0:35 / /home/user/scratch\\040dir rw,relatime shared:9 - tmpfs tmpfs rw\n"

	tests := []struct {
		name    string
		path    string
		want    string
		wantsOK bool
	}{
		{
			name:    "falls back to the root mount",
			path:    "/var/lib/tidemark",
			want:    "254:0",
			wantsOK: true,
		},
		{
			// The longest matching mount point wins, which is what makes a
			// separate /home answer for itself rather than deferring to /.
			name:    "longest prefix wins",
			path:    "/home/user/tidemark",
			want:    "254:16",
			wantsOK: true,
		},
		{
			name:    "the mount point itself",
			path:    "/home",
			want:    "254:16",
			wantsOK: true,
		},
		{
			// /homeless is not under /home, and a prefix test on the raw string
			// would say it was.
			name:    "a sibling with a shared prefix is not under the mount",
			path:    "/homeless",
			want:    "254:0",
			wantsOK: true,
		},
		{
			// Without the octal unescaping this resolves to / and reports the
			// root device's rotational flag as if it were the tmpfs's.
			name:    "escaped mount point",
			path:    "/home/user/scratch dir/checkpoints",
			want:    "0:35",
			wantsOK: true,
		},
		{
			name:    "no mounts at all",
			path:    "/anything",
			want:    "",
			wantsOK: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			info := mountinfo
			if !tc.wantsOK {
				info = ""
			}
			got, ok := deviceForPath(info, tc.path)
			if ok != tc.wantsOK {
				t.Fatalf("deviceForPath ok = %v, want %v", ok, tc.wantsOK)
			}
			if got != tc.want {
				t.Errorf("deviceForPath = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestFingerprintMatches(t *testing.T) {
	base := referenceFingerprint()

	tests := []struct {
		name  string
		other func(Fingerprint) Fingerprint
		want  bool
	}{
		{
			name:  "identical",
			other: func(f Fingerprint) Fingerprint { return f },
			want:  true,
		},
		{
			// MemTotal is a few pages short of the installed amount and the
			// shortfall moves with the kernel. Matching to the gibibyte is what
			// stops a machine failing to match itself across an upgrade.
			name: "memory differs within a gibibyte",
			other: func(f Fingerprint) Fingerprint {
				f.MemoryBytes -= 200 << 20
				return f
			},
			want: true,
		},
		{
			name: "memory differs by a gibibyte",
			other: func(f Fingerprint) Fingerprint {
				f.MemoryBytes -= 2 << 30
				return f
			},
			want: false,
		},
		{
			name: "different cpu model",
			other: func(f Fingerprint) Fingerprint {
				f.CPUModel = "Intel(R) Xeon(R) Processor @ 2.80GHz"
				return f
			},
			want: false,
		},
		{
			name: "different core count",
			other: func(f Fingerprint) Fingerprint {
				f.NumCPU = 4
				return f
			},
			want: false,
		},
		{
			// The whole reason recovery latency is not measurable in a
			// container whose disks report ROTA=1.
			name: "different disk class",
			other: func(f Fingerprint) Fingerprint {
				f.DiskRotational = diskRotational
				return f
			},
			want: false,
		},
		{
			name: "unknown disk is not solid state",
			other: func(f Fingerprint) Fingerprint {
				f.DiskRotational = diskUnknown
				return f
			},
			want: false,
		},
		{
			// A toolchain bump that moves throughput is indistinguishable from
			// a regression, so it invalidates the baseline.
			name: "different go version",
			other: func(f Fingerprint) Fingerprint {
				f.GoVersion = "go1.26.0"
				return f
			},
			want: false,
		},
		{
			name: "different gomaxprocs on the same cores",
			other: func(f Fingerprint) Fingerprint {
				f.GOMAXPROCS = 8
				return f
			},
			want: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			other := tc.other(referenceFingerprint())
			if got := base.Matches(other); got != tc.want {
				t.Errorf("Matches = %v, want %v\n  a: %s\n  b: %s", got, tc.want, base, other)
			}
			if got := other.Matches(base); got != tc.want {
				t.Errorf("Matches is not symmetric: reversed = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestCompareReports covers the three outcomes, and the SKIP is the one this
// step exists for.
//
// A skip that never happens and a skip that always happens look identical from
// a green build, so both the mismatch and the match are asserted here, and each
// is asserted on what it PRINTED as well as on what it returned: a skip that
// returned nil without saying so is a check that silently passed.
func TestCompareReports(t *testing.T) {
	local := Fingerprint{
		CPUModel:       "Intel(R) Xeon(R) Processor @ 2.80GHz",
		NumCPU:         4,
		MemoryBytes:    16461028 * 1024,
		DiskRotational: diskRotational,
		GoVersion:      runtime.Version(),
		GOMAXPROCS:     4,
	}
	results := func(rate float64) []Result {
		return []Result{{
			Config:        Config{Query: queryIdentity, Parallelism: 1, Records: 2000000, Seed: 1, Keys: 10000},
			RecordsPerSec: rate,
		}}
	}

	tests := []struct {
		name        string
		want        Report
		got         Report
		wantErr     bool
		wantOutputs []string
		notOutputs  []string
	}{
		{
			name:        "a different machine skips",
			want:        Report{Fingerprint: referenceFingerprint(), Results: results(2000000)},
			got:         Report{Fingerprint: local, Results: results(700000)},
			wantErr:     false,
			wantOutputs: []string{"SKIPPED", "AMD EPYC 9R14", "Intel(R) Xeon(R) Processor"},
			// A 65% shortfall against a machine three times faster must not be
			// reported as a comparison of any kind.
			notOutputs: []string{"against baseline"},
		},
		{
			name:        "the same machine compares and passes",
			want:        Report{Fingerprint: local, Results: results(700000)},
			got:         Report{Fingerprint: local, Results: results(690000)},
			wantErr:     false,
			wantOutputs: []string{"identity p=1", "against baseline"},
			notOutputs:  []string{"SKIPPED"},
		},
		{
			name:        "the same machine catches a regression",
			want:        Report{Fingerprint: local, Results: results(700000)},
			got:         Report{Fingerprint: local, Results: results(500000)},
			wantErr:     true,
			wantOutputs: []string{"against baseline"},
			notOutputs:  []string{"SKIPPED"},
		},
		{
			// A matching fingerprint that compared nothing looks exactly like a
			// pass, which is the other way this check can go quiet.
			name: "the same machine with no overlapping configuration says so",
			want: Report{Fingerprint: local, Results: []Result{{
				Config:        Config{Query: queryIdentity, Parallelism: 8, Records: 2000000, Seed: 1, Keys: 10000},
				RecordsPerSec: 1,
			}}},
			got: Report{Fingerprint: local, Results: results(700000)},

			wantErr:     false,
			wantOutputs: []string{"compared nothing"},
			notOutputs:  []string{"SKIPPED", "against baseline"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			out, err := captureStdout(t, func() error {
				return compareReports(tc.want, tc.got, 15)
			})
			if (err != nil) != tc.wantErr {
				t.Fatalf("compareReports error = %v, want error %v\noutput:\n%s", err, tc.wantErr, out)
			}
			for _, want := range tc.wantOutputs {
				if !strings.Contains(out, want) {
					t.Errorf("output does not mention %q:\n%s", want, out)
				}
			}
			for _, unwanted := range tc.notOutputs {
				if strings.Contains(out, unwanted) {
					t.Errorf("output mentions %q and should not:\n%s", unwanted, out)
				}
			}
		})
	}
}

// TestCompareBaselineSkipsOnAFixtureFromAnotherMachine runs the skip through
// the file-reading path, against a committed baseline that carries a
// fingerprint no machine running this test can have.
//
// The fixture is the point: the in-memory test above can be made to skip by
// construction, and this one proves the skip survives a round trip through the
// JSON a real baseline is stored as -- including a baseline whose fingerprint
// field is absent entirely, which is what every baseline written before this
// step looks like.
func TestCompareBaselineSkipsOnAFixtureFromAnotherMachine(t *testing.T) {
	got := Report{
		Fingerprint: machineFingerprint(t.TempDir()),
		Results: []Result{{
			Config:        Config{Query: queryIdentity, Parallelism: 1, Records: 2000000, Seed: 1, Keys: 10000},
			RecordsPerSec: 1,
		}},
	}

	fixtures := []string{
		filepath.Join("testdata", "baseline_other_machine.json"),
		// A baseline with no fingerprint at all: the zero value, which cannot
		// match a real machine, so an old baseline skips rather than being
		// compared against as if it belonged here.
		writeTempReport(t, Report{Results: got.Results}),
	}
	for _, path := range fixtures {
		t.Run(filepath.Base(path), func(t *testing.T) {
			out, err := captureStdout(t, func() error { return compareBaseline(path, got, 15) })
			if err != nil {
				t.Fatalf("compareBaseline returned %v; a mismatch must exit zero\noutput:\n%s", err, out)
			}
			if !strings.Contains(out, "SKIPPED") {
				t.Errorf("a baseline from another machine did not report a skip:\n%s", out)
			}
			if strings.Contains(out, "against baseline") {
				t.Errorf("a baseline from another machine was compared against:\n%s", out)
			}
		})
	}
}

// TestMachineFingerprintIsSelfConsistent is the guard against a fingerprint
// that is empty everywhere: a machine that matched every other machine would
// make the skip above never fire.
func TestMachineFingerprintIsSelfConsistent(t *testing.T) {
	dir := t.TempDir()
	first := machineFingerprint(dir)
	if !first.Matches(machineFingerprint(dir)) {
		t.Fatalf("a machine does not match itself across two reads:\n  %s\n  %s", first, machineFingerprint(dir))
	}
	if first.Matches(referenceFingerprint()) {
		t.Fatalf("this machine matched a fixture describing another one: %s", first)
	}
	if first.GoVersion != runtime.Version() || first.NumCPU != runtime.NumCPU() {
		t.Errorf("fingerprint disagrees with the runtime: %s", first)
	}

	if runtime.GOOS != "linux" {
		t.Skipf("the /proc and /sys fields are Linux-only; this is %s", runtime.GOOS)
	}
	if first.CPUModel == "" {
		t.Errorf("no CPU model on Linux: %s", first)
	}
	if first.MemoryBytes <= 0 {
		t.Errorf("no total memory on Linux: %s", first)
	}
	switch first.DiskRotational {
	case diskRotational, diskSolidState, diskUnknown:
	default:
		t.Errorf("disk_rotational is %q, which is none of the three answers", first.DiskRotational)
	}

	// The zero Fingerprint -- what a baseline with no fingerprint field
	// unmarshals to -- can never match a real machine, because a real one
	// always answers one of the three above and the zero value answers "".
	// test/bench/baseline.json relies on exactly this to hold its unattributed
	// Phase 6a numbers without them ever being compared against.
	if (Fingerprint{}).Matches(first) {
		t.Errorf("a fingerprint-less baseline matched this machine: %s", first)
	}
}

// writeTempReport writes r to a temporary file and returns the path.
func writeTempReport(t *testing.T, r Report) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "baseline.json")
	if err := writeReport(path, r); err != nil {
		t.Fatalf("writing a baseline fixture: %v", err)
	}
	return path
}

// captureStdout runs fn with os.Stdout redirected to a pipe and returns what it
// printed.
//
// The comparison prints its verdict and returns an error only for a regression,
// so a test that read the error alone could not tell a skip from a pass. The
// pipe is drained after the writer is closed, which is safe for output this
// size and avoids a reader goroutine in a test that must not outlive itself.
func captureStdout(t *testing.T, fn func() error) (string, error) {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	saved := os.Stdout
	os.Stdout = w
	callErr := fn()
	os.Stdout = saved
	if err := w.Close(); err != nil {
		t.Fatalf("closing the pipe: %v", err)
	}
	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("reading the pipe: %v", err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("closing the pipe reader: %v", err)
	}
	return string(out), callErr
}
