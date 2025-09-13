# syntax=docker/dockerfile:1

# =============================================================================
# Base layer: Alpine with Go 1.25 and essential build tools
# This layer is cached across builds and contains the Go toolchain
# =============================================================================
FROM golang:1.25-alpine AS base

# Install essential build dependencies and buf for protobuf generation
RUN apk add --no-cache \
    git \
    make \
    bash \
    ca-certificates \
    tzdata

# Install buf for protobuf code generation.
RUN go install github.com/bufbuild/buf/cmd/buf@latest
# Install protobuf Go plugin
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@latest

WORKDIR /src

# =============================================================================
# Dependencies layer: Cache Go modules separately for better layer caching.
# =============================================================================
FROM base AS deps

COPY go.mod go.sum ./
RUN go mod download && go mod verify

# =============================================================================
# Builder layer: Build the application with all source code.
# This layer contains the full build environment and compiles the binary.
# =============================================================================
FROM deps AS builder

# Generate protobuf code with buf.
ENV PATH="${PATH}:/go/bin"
COPY buf.yaml buf.gen.yaml ./
COPY proto/ ./proto/
RUN make proto

# Copy source code
COPY . .

# Define ldflags build variables.
ARG VERSION
ARG COMMIT
ARG BUILD_TIME
ENV BUILD_PKG=github.com/nobletooth/kiwi/pkg/utils

# Build the binary with optimizations for containerized deployment.
RUN mkdir -p bin && \
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -o ./bin/kiwi \
    -ldflags="-X ${BUILD_PKG}.Version=${VERSION} -X ${BUILD_PKG}.Commit=${COMMIT} -X ${BUILD_PKG}.BuildTime=${BUILD_TIME}" \
    ./cmd/kiwi

# =============================================================================
# Runtime layer: Minimal production image with security hardening
# This layer contains only the compiled binary and essential runtime files
# =============================================================================
FROM alpine:3.20 AS runtime

# Install CA certificates for HTTPS connections and timezone data.
RUN apk add --no-cache ca-certificates tzdata

# Create a non-root user for security.
RUN addgroup -g 1001 -S kiwi && \
    adduser -u 1001 -S kiwi -G kiwi

# Copy the binary from builder stage.
RUN mkdir -p /app && chown kiwi:kiwi /app
COPY --from=builder --chown=kiwi:kiwi /src/bin/kiwi /app/kiwi

# Prepare runtime env.
USER kiwi
WORKDIR /app
EXPOSE 6379
ENTRYPOINT ["/app/kiwi"]
