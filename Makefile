.PHONY: help proto

SRCS = $(patsubst ./%,%,$(shell find . -name "*.go" -not -path "*vendor*" -not -path "*.pb.go"))
PROTOS = $(patsubst ./%,%,$(shell find . -name "*.proto"))

ROOT := github.com/nobletooth/kiwi
GIT ?= git
COMMIT := $(shell $(GIT) rev-parse HEAD)
VERSION ?= $(shell cat version)
BUILD_TIME := $(shell LANG=en_US date +"%F_%T_%z")
BUILD_PKG := $(ROOT)/pkg/utils
LD_FLAGS := -X $(BUILD_PKG).Version=$(VERSION) -X $(BUILD_PKG).Commit=$(COMMIT) -X $(BUILD_PKG).BuildTime=$(BUILD_TIME)
TEST_LD_FLAGS := $(LD_FLAGS) -X $(BUILD_PKG).Test=true

help: ## Display this help screen
	@echo "$(ROOT):$(VERSION)"
	@grep -h -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

proto: $(PROTOS) ## To generate protobuf code.
	@echo "Generating protobuf code..."
	@buf generate
	@echo "Generating protobuf code done."

kiwi: $(SRCS) | .bins proto ## To build the kiwi binary.
	@echo "Building kiwi..."
	@go build -o ./bin/kiwi -ldflags="$(LD_FLAGS)" ./cmd/kiwi
	@echo "Building kiwi done."

test: $(SRCS) | proto ## To run tests.
	@go test -ldflags="$(TEST_LD_FLAGS)" ./...

run: kiwi ## To run a minimal kiwi server.
	@./bin/kiwi $(filter-out $@,$(MAKECMDGOALS))

%:
	@:

version: kiwi ## To print the build info.
	@./bin/kiwi -print_version -log_level info | jq

.bins:
	@mkdir -p bin
