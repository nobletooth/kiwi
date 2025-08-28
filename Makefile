.PHONY: help proto

SRCS = $(patsubst ./%,%,$(shell find . -name "*.go" -not -path "*vendor*" -not -path "*.pb.go"))
PROTOS = $(patsubst ./%,%,$(shell find . -name "*.proto"))

help: ## Display this help screen
	@grep -h -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

proto: $(PROTOS) ## To generate protobuf code.
	@echo "Generating protobuf code..."
	@buf generate
	@echo "Generating protobuf code done."

kiwi: $(SRCS) | .bins proto ## To build the kiwi binary.
	@echo "Building kiwi..."
	@go build -o ./bin/kiwi ./cmd/kiwi
	@echo "Building kiwi done."

run: kiwi ## To run a minimal kiwi server.
	@./bin/kiwi

.bins:
	@mkdir -p bin
