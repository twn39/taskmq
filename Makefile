# TaskMQ developer targets (optional; CI uses scripts directly).
.PHONY: build test test-unit test-race test-integration cover cover-report cover-gate keys lint tidy proto

build:
	go build ./...

tidy:
	go mod tidy

# Requires protoc + protoc-gen-go + protoc-gen-go-grpc on PATH.
proto:
	protoc \
		--go_out=. --go_opt=module=github.com/twn39/taskmq \
		--go-grpc_out=. --go-grpc_opt=module=github.com/twn39/taskmq \
		api/proto/taskmq/v1/taskmq.proto

keys:
	./scripts/check_keys_schema.sh

test-unit:
	go test ./internal/... ./tests/unit/... ./cmd/... -count=1 -timeout 10m

test-race:
	go test ./internal/... ./tests/unit/... -race -count=1 -timeout 10m

test-integration:
	go test ./tests/integration/... -count=1 -timeout 20m

test: test-unit

# Per-package minimums from coverage.yaml
cover-gate:
	./scripts/check_coverage.sh

# Gates + merged coverage.out / coverage.html
cover cover-report:
	./scripts/check_coverage.sh --report

lint:
	golangci-lint run --timeout=5m
