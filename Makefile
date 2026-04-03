# Makefile for SQL Server to OceanBase Sync Tool

# Variables
BINARY_NAME=sync-tool
BUILD_DIR=build
VERSION=$(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
BUILD_TIME=$(shell date -u '+%Y-%m-%d_%H:%M:%S')
LDFLAGS=-ldflags "-X main.version=$(VERSION) -X main.buildTime=$(BUILD_TIME)"

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
GOMOD=$(GOCMD) mod

# Targets
.PHONY: all build clean test deps run help

all: deps build

build:
	@echo "Building $(BINARY_NAME)..."
	@mkdir -p $(BUILD_DIR)
	$(GOBUILD) $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY_NAME) ./cmd/sync
	@echo "Build complete: $(BUILD_DIR)/$(BINARY_NAME)"

build-static:
	@echo "Building static binary $(BINARY_NAME)..."
	@mkdir -p $(BUILD_DIR)
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(GOBUILD) $(LDFLAGS) -a -installsuffix cgo -o $(BUILD_DIR)/$(BINARY_NAME)-static ./cmd/sync
	@echo "Static build complete: $(BUILD_DIR)/$(BINARY_NAME)-static"
	@echo ""
	@echo "This binary can be deployed without any dependencies!"

build-linux:
	@echo "Building for Linux..."
	@mkdir -p $(BUILD_DIR)
	GOOS=linux GOARCH=amd64 $(GOBUILD) $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY_NAME)-linux-amd64 ./cmd/sync
	@echo "Build complete: $(BUILD_DIR)/$(BINARY_NAME)-linux-amd64"

build-windows:
	@echo "Building for Windows..."
	@mkdir -p $(BUILD_DIR)
	GOOS=windows GOARCH=amd64 $(GOBUILD) $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY_NAME)-windows-amd64.exe ./cmd/sync
	@echo "Build complete: $(BUILD_DIR)/$(BINARY_NAME)-windows-amd64.exe"

clean:
	$(GOCLEAN)
	@rm -rf $(BUILD_DIR)
	@echo "Clean complete"

test:
	$(GOTEST) -v ./...

deps:
	$(GOMOD) download
	$(GOMOD) tidy

run: build
	./$(BUILD_DIR)/$(BINARY_NAME) -c config.yaml

run-example: build
	./$(BUILD_DIR)/$(BINARY_NAME) -c config/example.yaml

docker-build:
	docker build -t sqlserver-ob-sync:$(VERSION) .

docker-run:
	docker run -v $(PWD)/config.yaml:/app/config.yaml \
	           -e SOURCE_DB_PASSWORD=$$SOURCE_DB_PASSWORD \
	           -e TARGET_DB_PASSWORD=$$TARGET_DB_PASSWORD \
	           sqlserver-ob-sync:$(VERSION)

fmt:
	$(GOCMD) fmt ./...

vet:
	$(GOCMD) vet ./...

lint:
	@if command -v golangci-lint >/dev/null 2>&1; then \
		golangci-lint run ./...; \
	else \
		echo "golangci-lint not installed, skipping..."; \
	fi

help:
	@echo "Available targets:"
	@echo "  make build        - Build the binary"
	@echo "  make build-linux  - Build for Linux (amd64)"
	@echo "  make build-windows- Build for Windows (amd64)"
	@echo "  make clean        - Clean build artifacts"
	@echo "  make test         - Run tests"
	@echo "  make deps         - Download dependencies"
	@echo "  make run          - Build and run"
	@echo "  make run-example  - Run with example config"
	@echo "  make docker-build - Build Docker image"
	@echo "  make docker-run   - Run Docker container"
	@echo "  make fmt          - Format code"
	@echo "  make vet          - Run go vet"
	@echo "  make lint         - Run linter"
