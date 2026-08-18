.PHONY: build test vet lint check demo

build:
	go build ./...

test:
	go test ./... -race -count=1

# stdmethods is off because it insists any method named Seek match io.Seeker.
# core.Source.Seek takes a logical element offset, has no whence argument, and
# returns no position; it is deliberately not an io.Seeker. Every other
# analyzer stays on.
vet:
	go vet -stdmethods=false ./...

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
