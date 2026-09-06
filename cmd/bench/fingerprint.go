package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
)

// Fingerprint identifies the machine a benchmark ran on.
//
// It exists because bench-check compares a run against a committed baseline,
// and the comparison is meaningless across machines. The canonical baseline is
// generated once on reference hardware; without a fingerprint, every run
// anywhere else would be measured against it and the check would be
// permanently red. A target that is always red is a target nobody reads, and
// the first regression it hides is the one it was built to catch.
//
// So the fields here are the ones that move a throughput number, and they are
// compared rather than merely reported:
//
//   - CPUModel and NumCPU, because the scaling curve is a function of the cores
//     it ran on.
//   - MemoryBytes, because a state-heavy configuration that fits in RAM on one
//     machine swaps on another.
//   - DiskRotational, because recovery reads a checkpoint back and a rotational
//     device answers on a different order of magnitude from an NVMe one.
//   - GoVersion, because the compiler and the scheduler are part of the
//     measurement. A toolchain bump that moves throughput is indistinguishable
//     from a regression, so it invalidates the baseline rather than being
//     absorbed into the threshold.
//   - GOMAXPROCS, because it is the parallelism the runtime actually gets and
//     it need not equal NumCPU.
//
// Everything is read from /proc and /sys, which means non-Linux fills in what
// runtime can answer and leaves the rest empty or "unknown". That degrades to a
// fingerprint that will not match a Linux baseline, which is the correct
// outcome: a Mac laptop is not the reference machine either.
type Fingerprint struct {
	CPUModel string `json:"cpu_model"`
	NumCPU   int    `json:"num_cpu"`
	// MemoryBytes is total physical memory, not what is free. Free memory
	// varies minute to minute and would make a machine fail to match itself.
	MemoryBytes int64 `json:"memory_bytes"`
	// DiskRotational is "yes", "no" or "unknown" for the block device backing
	// the directory passed to machineFingerprint.
	//
	// A string and not a bool because there are three answers and the third one
	// is common: a container on an overlay or tmpfs has an anonymous device
	// number with no /sys/dev/block entry behind it. A bool would report that
	// as "no", which is the answer an NVMe gives, and a rotational-disk number
	// would then be compared against an NVMe baseline with nothing to say so.
	DiskRotational string `json:"disk_rotational"`
	GoVersion      string `json:"go_version"`
	GOMAXPROCS     int    `json:"gomaxprocs"`
}

// The three answers DiskRotational can hold.
const (
	diskRotational = "yes"
	diskSolidState = "no"
	diskUnknown    = "unknown"
)

// machineFingerprint reads this machine, probing the device that backs dir.
//
// The recovery harness passes the directory checkpoints land in -- --state-dir,
// or the system temp directory when that flag is empty -- because that is the
// device whose latency it measures. A caller passing a path on another
// filesystem gets that filesystem's answer, which is the honest one.
func machineFingerprint(dir string) Fingerprint {
	return Fingerprint{
		CPUModel:       cpuModel(),
		NumCPU:         runtime.NumCPU(),
		MemoryBytes:    totalMemoryBytes(),
		DiskRotational: diskRotationalFor(dir),
		GoVersion:      runtime.Version(),
		GOMAXPROCS:     runtime.GOMAXPROCS(0),
	}
}

// String is the one-line description a skip message names a machine by.
//
// The zero value gets a name of its own. It is what a baseline written before
// fingerprints existed unmarshals to, and rendering it as a machine with no CPU
// and no memory reads as a broken reading rather than as an absent one.
func (f Fingerprint) String() string {
	if f == (Fingerprint{}) {
		return "an unattributed run: no machine was recorded"
	}
	model := f.CPUModel
	if model == "" {
		model = "unknown cpu"
	}
	return fmt.Sprintf("%s, %d cpus, %.1f GiB, disk rotational=%s, %s, gomaxprocs=%d",
		model, f.NumCPU, float64(f.MemoryBytes)/(1<<30), f.DiskRotational, f.GoVersion, f.GOMAXPROCS)
}

// Matches reports whether two runs are comparable.
//
// Every field has to agree, with one deliberate softening: memory has to agree
// only to within a gibibyte. MemTotal is a few pages short of the installed
// amount and the shortfall moves with the kernel, so an exact comparison would
// make a machine stop matching itself across a kernel upgrade -- a skip that
// always happens, which is the failure this whole mechanism exists to avoid.
//
// A TOLERANCE and not a rounding to whole gibibytes. Rounding puts a cliff at
// every boundary, and a machine whose MemTotal sits just above one stops
// matching itself after a change of a few pages, which is the exact fragility
// the softening is for. The cost is that Matches is not transitive; nothing
// here chains it, since a run is compared against one baseline.
//
// A gibibyte is far finer than any difference that changes a throughput number,
// and far coarser than any drift in reading the same machine twice.
func (f Fingerprint) Matches(other Fingerprint) bool {
	memory := f.MemoryBytes - other.MemoryBytes
	if memory < 0 {
		memory = -memory
	}
	return f.CPUModel == other.CPUModel &&
		f.NumCPU == other.NumCPU &&
		memory < 1<<30 &&
		f.DiskRotational == other.DiskRotational &&
		f.GoVersion == other.GoVersion &&
		f.GOMAXPROCS == other.GOMAXPROCS
}

// compareBaseline is bench-check.
//
// Three outcomes, and the middle one is the point of this file:
//
//   - the fingerprints match and no level regressed: nil, and the per-level
//     comparison is printed.
//   - the fingerprints do NOT match: the run is SKIPPED. It prints both
//     machines and returns nil, so the target exits zero. It does not fail,
//     because the baseline says nothing about this machine; and it does not
//     print a passing comparison, because it did not make one.
//   - the fingerprints match and something regressed: an error.
func compareBaseline(path string, got Report, threshold float64) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("baseline: %w", err)
	}
	var want Report
	if err := json.Unmarshal(data, &want); err != nil {
		return fmt.Errorf("baseline %s: %w", path, err)
	}
	return compareReports(want, got, threshold)
}

// compareReports is compareBaseline with the file read already done, so that
// both of its paths are reachable from a test without a real baseline on disk.
func compareReports(want, got Report, threshold float64) error {
	if !want.Fingerprint.Matches(got.Fingerprint) {
		fmt.Printf("bench-check SKIPPED: the baseline was measured on a different machine.\n"+
			"  baseline: %s\n"+
			"  this run: %s\n"+
			"Nothing was compared. Published numbers come from one machine on purpose; "+
			"see docs/BENCHMARKS.md.\n", want.Fingerprint, got.Fingerprint)
		// The baseline's own caveat, at the moment somebody is looking at why
		// nothing was compared. A note that only lives in the file is a note
		// read by whoever opens the file, which is nobody.
		if want.Note != "" {
			fmt.Printf("The baseline says: %s\n", want.Note)
		}
		return nil
	}

	var regressions []string
	compared := 0
	for _, res := range got.Results {
		base, ok := baselineFor(want, res)
		if !ok {
			continue
		}
		compared++
		change := (res.RecordsPerSec - base.RecordsPerSec) / base.RecordsPerSec * 100
		fmt.Printf("%s: %.0f rec/s against baseline %.0f (%+.1f%%)\n",
			res.label(), res.RecordsPerSec, base.RecordsPerSec, change)
		if change < -threshold {
			regressions = append(regressions, fmt.Sprintf("%s is %.1f%% slower", res.label(), -change))
		}
	}
	if len(regressions) > 0 {
		return fmt.Errorf("regression worse than %.0f%%: %s", threshold, strings.Join(regressions, "; "))
	}
	// A matching fingerprint that compared nothing is a baseline covering no
	// configuration this run measured, which looks exactly like a pass. Say so.
	if compared == 0 {
		fmt.Printf("bench-check compared nothing: the baseline holds %d results and none matches a "+
			"configuration this run measured\n", len(want.Results))
	}
	return nil
}

// cpuModel returns the first model name in /proc/cpuinfo, or "".
//
// The first, and not a check that every core reports the same: a big.LITTLE
// machine reports two and neither of them is wrong. What matters is that the
// same machine answers the same string twice.
func cpuModel() string {
	data, err := os.ReadFile("/proc/cpuinfo")
	if err != nil {
		return ""
	}
	return parseCPUModel(string(data))
}

func parseCPUModel(cpuinfo string) string {
	for _, line := range strings.Split(cpuinfo, "\n") {
		key, value, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		switch strings.TrimSpace(key) {
		// "model name" on x86, "Model" on some ARM kernels, and
		// "cpu model" on others. All three are tried because the
		// fingerprint being empty on ARM would make every ARM machine
		// match every other one.
		case "model name", "Model", "cpu model":
			return strings.TrimSpace(value)
		}
	}
	return ""
}

// totalMemoryBytes returns MemTotal from /proc/meminfo, or 0.
func totalMemoryBytes() int64 {
	data, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0
	}
	return parseMemTotal(string(data))
}

func parseMemTotal(meminfo string) int64 {
	for _, line := range strings.Split(meminfo, "\n") {
		key, value, ok := strings.Cut(line, ":")
		if !ok || strings.TrimSpace(key) != "MemTotal" {
			continue
		}
		fields := strings.Fields(value)
		if len(fields) == 0 {
			return 0
		}
		n, err := strconv.ParseInt(fields[0], 10, 64)
		if err != nil {
			return 0
		}
		// MemTotal is in kB, which /proc/meminfo means as kibibytes.
		if len(fields) > 1 && fields[1] == "kB" {
			return n * 1024
		}
		return n
	}
	return 0
}

// diskRotationalFor reports whether the block device backing dir says it spins.
//
// Two hops. /proc/self/mountinfo maps a path to the major:minor of the device
// its filesystem sits on, and /sys/dev/block/<major>:<minor>/queue/rotational
// is what the kernel says about that device. A partition has no queue of its
// own, so its parent in sysfs is tried second.
//
// mountinfo rather than a stat of the directory, and the reason is portability
// rather than taste: st_dev is a syscall.Stat_t field whose type and device
// encoding differ per GOOS, so reading it would mean build tags on a file whose
// whole purpose is to describe one machine. On a system with no /proc this
// returns "unknown", which is the right answer there anyway.
func diskRotationalFor(dir string) string {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return diskUnknown
	}
	data, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		return diskUnknown
	}
	dev, ok := deviceForPath(string(data), abs)
	if !ok {
		return diskUnknown
	}
	return readRotational("/sys/dev/block/" + dev)
}

// deviceForPath returns the "major:minor" of the mount that path sits on.
//
// The longest matching mount point wins, which is what makes a bind mount or a
// separate /home answer for itself rather than deferring to /.
func deviceForPath(mountinfo, path string) (string, bool) {
	var (
		best    string
		bestLen = -1
	)
	for _, line := range strings.Split(mountinfo, "\n") {
		// Fields 0 to 4 are fixed: mount id, parent id, major:minor, root
		// within the filesystem, mount point. Optional fields follow and are
		// terminated by a lone "-", which is why nothing past index 4 is read
		// positionally.
		fields := strings.Fields(line)
		if len(fields) < 5 {
			continue
		}
		mount := unescapeMountPoint(fields[4])
		if !underMount(path, mount) || len(mount) <= bestLen {
			continue
		}
		best, bestLen = fields[2], len(mount)
	}
	return best, bestLen >= 0
}

func underMount(path, mount string) bool {
	return mount == "/" || path == mount || strings.HasPrefix(path, mount+"/")
}

// unescapeMountPoint decodes the octal escapes mountinfo writes for the four
// characters that would otherwise break its field separation.
//
// Without this, a working directory under a mount point containing a space
// would fail to match its own mount and fall back to a shorter prefix -- almost
// always "/" -- reporting the root device's rotational flag as if it were this
// filesystem's. Wrong quietly, which is the shape of bug this file is about.
func unescapeMountPoint(s string) string {
	if !strings.ContainsRune(s, '\\') {
		return s
	}
	var b strings.Builder
	for i := 0; i < len(s); {
		if s[i] == '\\' && i+3 < len(s) {
			if n, err := strconv.ParseUint(s[i+1:i+4], 8, 8); err == nil {
				b.WriteByte(byte(n))
				i += 4
				continue
			}
		}
		b.WriteByte(s[i])
		i++
	}
	return b.String()
}

// readRotational reads queue/rotational under sysDir, then under its parent.
//
// The parent hop is what makes a partition answer. /sys/dev/block/8:1 is a
// partition and holds no queue; /sys/dev/block/8:0, its parent in the resolved
// device path, does.
func readRotational(sysDir string) string {
	resolved, err := filepath.EvalSymlinks(sysDir)
	if err != nil {
		return diskUnknown
	}
	for _, dir := range []string{resolved, filepath.Dir(resolved)} {
		data, err := os.ReadFile(filepath.Join(dir, "queue", "rotational"))
		if err != nil {
			continue
		}
		switch strings.TrimSpace(string(data)) {
		case "1":
			return diskRotational
		case "0":
			return diskSolidState
		}
	}
	return diskUnknown
}
