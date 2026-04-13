#!/bin/bash

# FastSync - SQL Server to OceanBase MySQL Sync Tool
# One-click build script

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "========================================"
echo "  FastSync Build Script"
echo "========================================"
echo ""

# Check if Go is installed
if ! command -v go &> /dev/null; then
    echo -e "${RED}Error: Go is not installed${NC}"
    echo "Please install Go 1.22 or later from https://golang.org/dl/"
    exit 1
fi

# Check Go version
GO_VERSION=$(go version | awk '{print $3}' | sed 's/go//')
echo "Go version: $GO_VERSION"

# Get project info
VERSION=$(date '+%Y.%-m.%-d')
BUILD_TIME=$(date '+%Y-%m-%d_%H:%M:%S')

echo "Version: $VERSION"
echo "Build time: $BUILD_TIME"
echo ""

# Create build directory
mkdir -p build

# Build flags
LDFLAGS="-X main.version=$VERSION -X main.buildTime=$BUILD_TIME"

echo -e "${YELLOW}Building...${NC}"

# Standard build
echo "  -> Building fastsync..."
go build -ldflags "$LDFLAGS" -o build/fastsync ./cmd/sync

# Static build (recommended for deployment)
echo "  -> Building fastsync-static..."
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags "$LDFLAGS" -a -installsuffix cgo -o build/fastsync-static ./cmd/sync

echo ""
echo -e "${GREEN}Build completed successfully!${NC}"
echo ""
echo "Binaries:"
echo "  - build/fastsync         (dynamic build)"
echo "  - build/fastsync-static  (static build for deployment)"
echo ""
echo "Usage:"
echo "  ./build/fastsync -c config.yaml"
echo "  ./build/fastsync -c config.yaml --create-schema --dry-run"
echo ""
