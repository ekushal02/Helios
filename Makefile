.PHONY: run test race cover lint

run:
	go run ./cmd/helios

test:
	go test ./...

race:
	go test -race ./...

cover:
	go test -cover ./...

lint:
	golangci-lint run