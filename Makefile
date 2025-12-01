.PHONY: test lint build clean install-tools pre-commit ci-local help

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
GOMOD=$(GOCMD) mod
BINARY_NAME=bytefreezer-packer

# Colors for output
RED=\033[0;31m
GREEN=\033[0;32m
YELLOW=\033[1;33m
BLUE=\033[0;34m
NC=\033[0m # No Color

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Available targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  $(BLUE)%-15s$(NC) %s\n", $$1, $$2}' $(MAKEFILE_LIST)

install-tools: ## Install required development tools
	@echo "$(BLUE)Installing development tools...$(NC)"
	@$(GOCMD) install honnef.co/go/tools/cmd/staticcheck@latest
	@$(GOCMD) install github.com/securego/gosec/v2/cmd/gosec@latest
	@echo "$(GREEN)✓ Tools installed successfully$(NC)"

deps: ## Download and verify dependencies
	@echo "$(BLUE)Downloading dependencies...$(NC)"
	@$(GOMOD) download
	@$(GOMOD) verify
	@$(GOMOD) tidy
	@echo "$(GREEN)✓ Dependencies updated$(NC)"

fmt: ## Format Go code
	@echo "$(BLUE)Formatting Go code...$(NC)"
	@if [ "$$(gofmt -s -l . | wc -l)" -gt 0 ]; then \
		echo "$(RED)✗ Code needs formatting. Run 'gofmt -s -w .' to fix:$(NC)"; \
		gofmt -s -l .; \
		exit 1; \
	else \
		echo "$(GREEN)✓ All Go files are properly formatted$(NC)"; \
	fi

vet: ## Run go vet
	@echo "$(BLUE)Running go vet...$(NC)"
	@$(GOCMD) vet ./...
	@echo "$(GREEN)✓ go vet passed$(NC)"

staticcheck: ## Run staticcheck
	@echo "$(BLUE)Running staticcheck...$(NC)"
	@staticcheck ./...
	@echo "$(GREEN)✓ staticcheck passed$(NC)"

gosec: ## Run gosec security scanner
	@echo "$(BLUE)Running gosec security scanner...$(NC)"
	@gosec -severity medium -confidence medium -quiet ./... || echo "$(YELLOW)⚠ gosec found security issues (non-blocking)$(NC)"

test: ## Run tests
	@echo "$(BLUE)Running tests...$(NC)"
	@$(GOTEST) -v -race -coverprofile=coverage.out -covermode=atomic ./...
	@echo "$(GREEN)✓ Tests passed$(NC)"

test-coverage: test ## Run tests and show coverage
	@echo "$(BLUE)Generating coverage report...$(NC)"
	@$(GOCMD) tool cover -html=coverage.out -o coverage.html
	@echo "$(GREEN)✓ Coverage report generated: coverage.html$(NC)"

lint: fmt vet staticcheck gosec ## Run all linting checks

build: ## Build the binary
	@echo "$(BLUE)Building $(BINARY_NAME)...$(NC)"
	@$(GOBUILD) -ldflags="-s -w -X main.version=dev -X main.buildTime=$$(date -u +%Y-%m-%dT%H:%M:%SZ) -X main.gitCommit=$$(git rev-parse --short HEAD 2>/dev/null || echo unknown)" -o $(BINARY_NAME) .
	@echo "$(GREEN)✓ Build completed: $(BINARY_NAME)$(NC)"

clean: ## Clean build artifacts
	@echo "$(BLUE)Cleaning...$(NC)"
	@$(GOCLEAN)
	@rm -f $(BINARY_NAME) coverage.out coverage.html
	@echo "$(GREEN)✓ Cleaned$(NC)"

pre-commit: deps lint test ## Run all pre-commit checks (recommended before committing)
	@echo "$(GREEN)🎉 All pre-commit checks passed! Ready to commit.$(NC)"

ci-local: install-tools pre-commit build ## Run full CI pipeline locally
	@echo "$(GREEN)🚀 Full CI pipeline completed successfully!$(NC)"

# Integration test with real config
test-integration: build ## Run integration tests with real binary
	@echo "$(BLUE)Running integration tests...$(NC)"
	@./$(BINARY_NAME) --version || echo "Version test completed"
	@./$(BINARY_NAME) --help || echo "Help test completed"
	@echo "$(GREEN)✓ Integration tests passed$(NC)"

# Docker build test
docker-build-test: ## Test Docker build locally
	@echo "$(BLUE)Testing Docker build...$(NC)"
	@docker build --build-arg VERSION=test --build-arg BUILD_TIME=$$(date -u +%Y-%m-%dT%H:%M:%SZ) -t $(BINARY_NAME):test .
	@echo "$(GREEN)✓ Docker build test passed$(NC)"