.DEFAULT_GOAL := all

.PHONY: compile
compile:
	mkdir -p bin && \
	go mod tidy && \
	go build -o ./bin/. ./...

.PHONY: test
test: compile
	go test -v -timeout 3s -race ./...

.PHONY: install
install:
	go install google.golang.org/protobuf/cmd/protoc-gen-go && \
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc

.PHONY: coverage
coverage: compile
	go test -race -cover ./...

.PHONY: clean
clean:
	rm -rf bin

.PHONY: all
all: compile test
