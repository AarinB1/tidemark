.PHONY: build test vet lint check demo

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
