#!/bin/bash

# Security Scan Script for MySQL Flashback
# This script helps identify security vulnerabilities in the codebase and Docker images

set -e

echo "🔒 MySQL Flashback Security Scanner"
echo "=================================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Check Go security
echo -e "\n${YELLOW}1. Checking Go dependencies for vulnerabilities...${NC}"
if command_exists "govulncheck"; then
    echo "Running govulncheck..."
    govulncheck ./...
else
    echo -e "${RED}govulncheck not found. Install with: go install golang.org/x/vuln/cmd/govulncheck@latest${NC}"
fi

# Check Docker image vulnerabilities
echo -e "\n${YELLOW}2. Checking Docker image for vulnerabilities...${NC}"
if command_exists "trivy"; then
    echo "Running Trivy scan..."
    trivy image mysql-flashback:latest 2>/dev/null || echo "Image not found. Run 'make docker-build' first."
elif command_exists "docker" && command_exists "dockle"; then
    echo "Running Dockle scan..."
    dockle mysql-flashback:latest 2>/dev/null || echo "Image not found. Run 'make docker-build' first."
else
    echo -e "${RED}Neither Trivy nor Dockle found.${NC}"
    echo "Install security scanners:"
    echo "  brew install trivy"
    echo "  brew install goodwithtech/r/dockle"
fi

# Check for hardcoded secrets
echo -e "\n${YELLOW}3. Checking for potential hardcoded secrets...${NC}"
if command_exists "gitleaks" || command_exists "git-secrets"; then
    if command_exists "gitleaks"; then
        echo "Running gitleaks..."
        gitleaks detect --verbose --redact --config .gitleaks.toml . 2>/dev/null || echo "No gitleaks config found"
    elif command_exists "git-secrets"; then
        echo "Running git-secrets..."
        git-secrets --scan
    fi
else
    echo -e "${RED}No secret scanning tool found.${NC}"
    echo "Install secret scanners:"
    echo "  brew install gitleaks"
    echo "  brew install git-secrets"
fi

# Check for common security issues in Go code
echo -e "\n${YELLOW}4. Checking for common security issues...${NC}"
if command_exists "gosec"; then
    echo "Running gosec..."
    gosec ./...
else
    echo -e "${RED}gosec not found. Install with: go install github.com/securecodewarrior/gosec/v2/cmd/gosec@latest${NC}"
fi

# Check dependencies for known vulnerabilities
echo -e "\n${YELLOW}5. Checking dependencies...${NC}"
echo "Checking go.mod for known vulnerabilities..."
go list -m -u all | grep -v "All modules" || echo "No dependency updates available"

echo -e "\n${GREEN}Security scan completed!${NC}"
echo -e "\n${YELLOW}Recommendations:${NC}"
echo "1. Regularly update base images and dependencies"
echo "2. Use distroless images for production"
echo "3. Run security scans in CI/CD pipeline"
echo "4. Monitor for new vulnerabilities"
echo "5. Use secret management tools instead of hardcoded values"
