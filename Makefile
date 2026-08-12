.PHONY: build test vet lint check wordcount

# Phase 0 has no Go packages yet, and `go vet ./...` / `go test ./...` exit
# non-zero when the pattern matches nothing. Skip explicitly in that case so
# `make check` is meaningful now; once a package exists these run unguarded.

build:
	go build ./...

test:
	@if [ -z "$$(go list ./... 2>/dev/null)" ]; then \
		echo "test: no Go packages yet, skipping"; \
	else \
		go test ./... -race -count=1; \
	fi

vet:
	@if [ -z "$$(go list ./... 2>/dev/null)" ]; then \
		echo "vet: no Go packages yet, skipping"; \
	else \
		go vet ./...; \
	fi

lint: vet
	@unformatted=$$(gofmt -l .); \
	if [ -n "$$unformatted" ]; then \
		echo "gofmt: files need formatting:"; \
		echo "$$unformatted"; \
		exit 1; \
	fi

check: vet lint test

wordcount:
	go run ./cmd/worker --job wordcount --records 1000000
