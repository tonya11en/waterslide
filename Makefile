.DEFAULT_GOAL := all

.PHONY: compile
compile:
	go build -o ./bin/. ./...

.PHONY: test
test: compile
	go test -race -cover ./...

.PHONY: install
install:
	go mod tidy

.PHONY: clean
clean:
	rm -rf bin


.PHONY: all
all: install compile test
