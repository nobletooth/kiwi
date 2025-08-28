bins:
	@mkdir -p bin

build: bins
	@echo "Building kiwi..."
	@go build -o ./bin/kiwi ./cmd/kiwi

run: build
	@./bin/kiwi
