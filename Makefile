.PHONY: help proto test image

# Use bash for recipes and enable strict mode for safer scripts (Google style).
SHELL := bash
.SHELLFLAGS := -eu -o pipefail -c
.ONESHELL:
.SILENT: test

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
	@grep -h -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

proto: ## To generate protobuf code.
	@buf generate

.bins:
	@mkdir -p bin

bin/kiwi: $(SRCS) | .bins proto ## To build the kiwi binary.
	@go build -o ./bin/kiwi -ldflags="$(LD_FLAGS)" ./cmd/kiwi

test: $(SRCS) | bin/kiwi proto ## To run tests. Usage: make test [--pkg <path>] [--test <regex-or-name>]  (use `--` before flags)
	# Parse flags from MAKECMDGOALS (the ones that come after `--` in `make test -- --pkg <some_package>).
	pkg=""; test_filter="";
	@for arg in $(filter-out $@,$(MAKECMDGOALS)); do
	  case "$$arg" in
	    --pkg) next_is_pkg=1; continue ;;
	    --test) next_is_test=1; continue ;;
	  esac
	  if [[ "${next_is_pkg-}" == 1 ]]; then pkg="$$arg"; unset next_is_pkg; continue; fi
	  if [[ "${next_is_test-}" == 1 ]]; then test_filter="$$arg"; unset next_is_test; continue; fi
	done

	# Default package pattern is all modules.
	pkg_pattern="./..."
	@if [[ -n "$$pkg" ]]; then
	  case "$$pkg" in
	    ./*|*/...) pkg_pattern="$$pkg" ;;
	    *)         pkg_pattern="./$$pkg" ;;
	  esac
	fi

	# Run tests with or without -run filter.
	@if [[ -n "$$test_filter" ]]; then
	  go test -ldflags="$(TEST_LD_FLAGS)" -run "$$test_filter" $$pkg_pattern
	else
	  go test -ldflags="$(TEST_LD_FLAGS)" $$pkg_pattern
	fi

run: bin/kiwi ## To run a minimal kiwi server. Example: make run -- --address 0.0.0.0:6122
	@./bin/kiwi $(filter-out $@,$(MAKECMDGOALS))

%:
	@:

version: bin/kiwi ## To print the build info.
	@./bin/kiwi -print_version -log_level info | jq

image: ## Build Docker image with version and latest tags
	@docker build \
		--build-arg VERSION=$(VERSION) \
		--build-arg COMMIT=$(COMMIT) \
		--build-arg BUILD_TIME=$(BUILD_TIME) \
		-t kiwi:$(VERSION) \
		-t kiwi:latest \
		.
	@echo "Successfully built image: kiwi:$(VERSION) and kiwi:latest"
