.PHONY: build test vet lint check demo bench bench-check

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

bench-check:
	go run ./cmd/bench \
		--records $(BENCH_RECORDS) \
		--parallelism $(BENCH_PARALLELISM) \
		--seed $(BENCH_SEED) \
		--keys $(BENCH_KEYS) \
		--json $(BENCH_OUT) \
		--baseline $(BENCH_BASELINE) \
		--threshold $(BENCH_THRESHOLD)
