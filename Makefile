.PHONY: build test vet lint check demo bench bench-check bench-full chaos chaos-race chaos-nexmark

build:
	go build ./...

test:
	go test ./... -race -count=1

vet:
	go vet ./...

lint: vet
	@unformatted=$$(gofmt -l .); \
	if [ -n "$$unformatted" ]; then \
		echo "gofmt: files need formatting:"; \
		echo "$$unformatted"; \
		exit 1; \
	fi

check: vet lint test

demo:
	go run ./cmd/worker --job identity --records 100000

# Throughput. Deliberately not run under -race: the race detector costs 5 to 20x
# and the number it produces means nothing.
BENCH_RECORDS ?= 2000000
BENCH_PARALLELISM ?= 1,2,4,8
BENCH_SEED ?= 1
BENCH_KEYS ?= 10000
BENCH_OUT ?= bench.json
BENCH_BASELINE ?= test/bench/baseline.json
# Local. CI overrides this: shared runners are noisy, and a tight threshold
# there produces false alarms that train you to ignore the check.
BENCH_THRESHOLD ?= 15

bench:
	go run ./cmd/bench \
		--records $(BENCH_RECORDS) \
		--parallelism $(BENCH_PARALLELISM) \
		--seed $(BENCH_SEED) \
		--keys $(BENCH_KEYS) \
		--json $(BENCH_OUT)

# Compares against the committed baseline ONLY when the machine matches the one
# the baseline was measured on; see Fingerprint in cmd/bench. On any other
# machine it prints both machines and exits zero, because a baseline from
# elsewhere says nothing about this hardware and a target that is always red is
# a target nobody reads. Today every machine skips: test/bench/baseline.json is
# unattributed and the canonical one comes from the reference machine in
# docs/BENCHMARKS.md.
bench-check:
	go run ./cmd/bench \
		--records $(BENCH_RECORDS) \
		--parallelism $(BENCH_PARALLELISM) \
		--seed $(BENCH_SEED) \
		--keys $(BENCH_KEYS) \
		--auctions $(BENCH_AUCTIONS) \
		--json $(BENCH_OUT) \
		--baseline $(BENCH_BASELINE) \
		--threshold $(BENCH_THRESHOLD)

# The whole measurement suite, in one command, which is what the reference
# machine runs. See docs/BENCHMARKS.md for the procedure and for what a
# published number requires.
#
# Three invocations rather than one `--mode all`, because the three measurements
# want different sweeps: throughput wants every query across the parallelism
# curve at one cardinality, the state sweep wants the cardinality axis at the
# parallelism levels where the difference shows, and the recovery curve wants
# one query across snapshot sizes. Running them under one flag set would mean
# measuring 120 throughput configurations to get 12 state rows.
#
# All three write beside BENCH_OUT: bench.json, bench.state.json and
# bench.recovery.json.
BENCH_WINDOW ?= 5000
BENCH_SLIDE ?= 1250
BENCH_AUCTIONS ?= 1000
BENCH_CHECKPOINTS ?= 8
BENCH_FULL_QUERIES ?= all
BENCH_FULL_PARALLELISM ?= 1,2,4,8,16
BENCH_STATE_QUERIES ?= q7,q5
BENCH_STATE_PARALLELISM ?= 1,2,4
BENCH_STATE_AUCTIONS ?= 100,1000,10000,100000
BENCH_RECOVERY_QUERY ?= q7
BENCH_RECOVERY_PARALLELISM ?= 2
BENCH_RECOVERY_AUCTIONS ?= 100,1000,10000,100000

bench-full:
	go run ./cmd/bench --mode throughput \
		--query $(BENCH_FULL_QUERIES) \
		--parallelism $(BENCH_FULL_PARALLELISM) \
		--records $(BENCH_RECORDS) \
		--seed $(BENCH_SEED) \
		--keys $(BENCH_KEYS) \
		--auctions $(BENCH_AUCTIONS) \
		--window $(BENCH_WINDOW) \
		--slide $(BENCH_SLIDE) \
		--json $(BENCH_OUT)
	go run ./cmd/bench --mode state \
		--query $(BENCH_STATE_QUERIES) \
		--parallelism $(BENCH_STATE_PARALLELISM) \
		--records $(BENCH_RECORDS) \
		--seed $(BENCH_SEED) \
		--auctions $(BENCH_STATE_AUCTIONS) \
		--window $(BENCH_WINDOW) \
		--slide $(BENCH_SLIDE) \
		--checkpoints $(BENCH_CHECKPOINTS) \
		--json $(BENCH_OUT)
	go run ./cmd/bench --mode recovery \
		--query $(BENCH_RECOVERY_QUERY) \
		--parallelism $(BENCH_RECOVERY_PARALLELISM) \
		--records $(BENCH_RECORDS) \
		--seed $(BENCH_SEED) \
		--auctions $(BENCH_RECOVERY_AUCTIONS) \
		--window $(BENCH_WINDOW) \
		--slide $(BENCH_SLIDE) \
		--checkpoints $(BENCH_CHECKPOINTS) \
		--json $(BENCH_OUT)

# Seeded fault schedules. Two shapes, and the difference is the race detector.
#
# CHAOS_SEEDS is contiguous from 1, so a failure names a seed that reproduces
# it directly:
#
#	go test ./test/chaos -run 'TestChaosSuite/all/seed137' -count=1
#
# The full five hundred run WITHOUT -race. The detector costs five to twenty
# times, and five hundred schedules under it fit no reasonable budget; a target
# nobody can afford to run is a target nobody runs. The twenty-five-seed subset
# is what runs under it instead, and it is a subset rather than a sample --
# schedules are a pure function of the seed, so those twenty-five are the same
# twenty-five every time and the census floors were measured on both.
#
# Twenty-five is also the package default, so `go test ./...` in `check` above
# runs the race subset without being told to.
CHAOS_SEEDS ?= 500
CHAOS_RACE_SEEDS ?= 25
CHAOS_TIMEOUT ?= 30m

chaos:
	go test ./test/chaos -run TestChaosSuite -count=1 -v \
		-chaos.seeds=$(CHAOS_SEEDS) -timeout $(CHAOS_TIMEOUT)

chaos-race:
	go test ./test/chaos -run TestChaosSuite -count=1 -race -v \
		-chaos.seeds=$(CHAOS_RACE_SEEDS) -timeout $(CHAOS_TIMEOUT)

# The Nexmark chaos suite: the same fault schedules against the five queries.
#
# Five hundred contiguous seeds PER QUERY, so this target is two and a half
# thousand schedules. It exists because the number was previously reachable only
# by knowing the flag name, and a run that has to be assembled from a comment is
# a run nobody makes.
#
# Without -race, for the reason the five-hundred-seed keyed-count target is:
# five to twenty times the cost across five queries fits no budget anybody will
# pay. The race subset for this suite is the package default of ten seeds per
# query, which `go test ./...` in `check` already runs under the detector.
NEXMARK_CHAOS_SEEDS ?= 500
NEXMARK_CHAOS_TIMEOUT ?= 60m

chaos-nexmark:
	go test ./test/chaos -run TestNexmarkChaosSuite -count=1 -v \
		-chaos.nexmark.seeds=$(NEXMARK_CHAOS_SEEDS) -timeout $(NEXMARK_CHAOS_TIMEOUT)
