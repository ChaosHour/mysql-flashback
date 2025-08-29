# Docker Bake configuration for MySQL Flashback
# This file configures Docker Bake for optimal performance and security

# Default target
target "default" {
  dockerfile = "Dockerfile"
  context    = "."
  platforms  = ["linux/amd64"]
  tags       = ["mysql-flashback:latest"]
  args = {
    BUILDKIT_INLINE_CACHE = "1"
  }
  labels = {
    "org.opencontainers.image.title"       = "MySQL Flashback"
    "org.opencontainers.image.description" = "Secure MySQL flashback tool"
    "org.opencontainers.image.vendor"      = "ChaosHour"
    "org.opencontainers.image.source"      = "https://github.com/ChaosHour/mysql-flashback"
  }
}

# Multi-platform build target
target "multi-platform" {
  inherits = ["default"]
  platforms = ["linux/amd64", "linux/arm64"]
  tags      = ["mysql-flashback:latest", "mysql-flashback:multi"]
}

# Development build target
target "dev" {
  inherits = ["default"]
  tags     = ["mysql-flashback:dev"]
  args = {
    BUILDKIT_INLINE_CACHE = "1"
  }
}

# Production build target
target "prod" {
  inherits = ["default"]
  tags     = ["mysql-flashback:prod"]
  args = {
    BUILDKIT_INLINE_CACHE = "1"
  }
  labels = {
    "org.opencontainers.image.title"       = "MySQL Flashback"
    "org.opencontainers.image.description" = "Production MySQL flashback tool"
    "org.opencontainers.image.vendor"      = "ChaosHour"
    "org.opencontainers.image.version"     = "latest"
  }
}
