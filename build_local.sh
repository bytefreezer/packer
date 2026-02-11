#!/bin/bash
set -e

# ByteFreezer Proxy Build Script
# Builds the binary for local Ansible deployment

PROJECT_NAME="bytefreezer-packer"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ANSIBLE_DIST_DIR="$SCRIPT_DIR/ansible/playbooks/dist"

echo "🔨 Building $PROJECT_NAME..."

# Create ansible/playbooks/dist directory
mkdir -p "$ANSIBLE_DIST_DIR"

# Get version info
VERSION="local-$(git rev-parse --short HEAD)"
BUILD_TIME=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
GIT_COMMIT=$(git rev-parse --short HEAD 2>/dev/null || echo "unknown")

echo "Version: $VERSION"
echo "Build Time: $BUILD_TIME"
echo "Git Commit: $GIT_COMMIT"

# Remove old binary if it exists
if [[ -f "$ANSIBLE_DIST_DIR/$PROJECT_NAME" ]]; then
    echo "Removing old binary..."
    rm -f "$ANSIBLE_DIST_DIR/$PROJECT_NAME"
fi

# Build the binary
echo "Building binary..."
CGO_ENABLED=0 go build \
    -ldflags="-s -w -X main.version=$VERSION -X main.buildTime=$BUILD_TIME -X main.gitCommit=$GIT_COMMIT" \
    -o "$ANSIBLE_DIST_DIR/$PROJECT_NAME" \
    .

# Verify binary was created
if [[ ! -f "$ANSIBLE_DIST_DIR/$PROJECT_NAME" ]]; then
    echo "❌ Error: Binary not found at $ANSIBLE_DIST_DIR/$PROJECT_NAME"
    exit 1
fi

# Test binary
echo "Testing binary..."
"$ANSIBLE_DIST_DIR/$PROJECT_NAME" --version

echo "✅ Build successful!"
echo "📦 Binary location: $ANSIBLE_DIST_DIR/$PROJECT_NAME"
echo ""
echo "Binary info:"
stat "$ANSIBLE_DIST_DIR/$PROJECT_NAME"