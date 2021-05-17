.DEFAULT_GOAL := all

.PHONY: compile
compile:
	mkdir -p bin && \
	go mod tidy && \
	go build -o ./bin/. ./...

.PHONY: test
test: compile
	go test -race -cover ./...

.PHONY: install
install:
	go install google.golang.org/protobuf/cmd/protoc-gen-go && \
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc

.PHONY: clean
clean:
	rm -rf bin

.PHONY: all
all: compile test
