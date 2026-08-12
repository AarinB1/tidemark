.PHONY: build test vet lint check wordcount

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

wordcount:
	go run ./cmd/worker --job wordcount --records 1000000
