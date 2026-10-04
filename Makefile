# eth-client-monorepo — developer entrypoints
# Toolchain pin lives only in rust-toolchain.toml; do not hardcode the Rust version here.
#
# S0a-B-04: CI_JOBS must match `.github/workflows/ci.yml` `jobs:` keys
# (scripts/check-ci-job-list.sh). `make ci` runs the local required subset;
# vectors/compose stay opt-in (spec-vector download / docker proof).

SHELL := /bin/bash
.SHELLFLAGS := -eu -o pipefail -c
.DEFAULT_GOAL := help

# ── Cargo ────────────────────────────────────────────────────────────────────
CARGO       ?= cargo
CARGO_FLAGS ?= --locked
CLIPPY_FLAGS ?= --workspace --all-targets --all-features $(CARGO_FLAGS)
NEXTEST_FLAGS ?= --workspace $(CARGO_FLAGS) --profile ci

# ── Coverage (cargo-llvm-cov; install: cargo install cargo-llvm-cov --locked) ─
# HTML lands under target/llvm-cov/html; lcov under COVERAGE_LCOV by default.
# Same test selection as `make test` (lib + integration). Do **not** pass
# `--all-targets`: harness=false custom benches (e.g. cc-fork-choice head)
# break nextest's `--list` discovery. Use `cargo bench` / scripts/bench-*.sh
# for benches.
COVERAGE_FLAGS ?= --workspace --all-features $(CARGO_FLAGS)
COVERAGE_LCOV  ?= target/llvm-cov/lcov.info
# Package filter for focused runs, e.g. `make coverage-html PKG=cc-p2p`
PKG            ?=

# ── Services (compose / binaries) ────────────────────────────────────────────
SERVICES := chain p2p attestation engine beacon-api storage

# ── Git SHA for compose build_info ───────────────────────────────────────────
CC_GIT_SHA ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)

# ── Paths ────────────────────────────────────────────────────────────────────
SCRIPTS := scripts
PROTO   := proto

# Job ids — must equal ci.yml `jobs:` keys. Edit both sides together.
CI_JOBS := fmt clippy test proto vectors deps compose

.PHONY: help
help: ## Show this help
	@awk 'BEGIN {FS = ":.*##"; printf "Usage: make \033[36m<target>\033[0m\n\nTargets:\n"} \
		/^[a-zA-Z0-9_.-]+:.*?##/ { printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

# ══════════════════════════════════════════════════════════════════════════════
# Build
# ══════════════════════════════════════════════════════════════════════════════

.PHONY: build
build: ## Build workspace (debug)
	$(CARGO) build --workspace $(CARGO_FLAGS)

.PHONY: release
release: ## Build workspace (release)
	$(CARGO) build --workspace --release $(CARGO_FLAGS)

.PHONY: check
check: ## Type-check workspace (no codegen of binaries)
	$(CARGO) check --workspace --all-targets $(CARGO_FLAGS)

# ══════════════════════════════════════════════════════════════════════════════
# Test
# ══════════════════════════════════════════════════════════════════════════════

.PHONY: test
test: ## Run tests with cargo-nextest (CI profile)
	$(CARGO) nextest run $(NEXTEST_FLAGS)

.PHONY: test-seam
test-seam: ## Conformance suite against both seam impls (E1.3)
	$(CARGO) test -p cc-seam $(CARGO_FLAGS)

.PHONY: test-cargo
test-cargo: ## Run tests with cargo test (fallback without nextest)
	$(CARGO) test --workspace $(CARGO_FLAGS)

# ══════════════════════════════════════════════════════════════════════════════
# Coverage (local; cargo-llvm-cov + nextest)
# ══════════════════════════════════════════════════════════════════════════════
# Requires: rustup component add llvm-tools-preview
#           cargo install cargo-llvm-cov --locked
# Matches `make test` target set (not benches/examples). Pass PKG=cc-types etc.
# for a single crate.

.PHONY: coverage
coverage: ## Line coverage summary (nextest, workspace)
	@command -v cargo-llvm-cov >/dev/null 2>&1 || { \
		echo "error: cargo-llvm-cov not found; install with: cargo install cargo-llvm-cov --locked" >&2; \
		exit 1; \
	}
	$(CARGO) llvm-cov nextest \
		$(if $(PKG),-p $(PKG),$(COVERAGE_FLAGS)) \
		--profile ci \
		--summary-only

.PHONY: coverage-html
coverage-html: ## HTML report → target/llvm-cov/html/index.html
	@command -v cargo-llvm-cov >/dev/null 2>&1 || { \
		echo "error: cargo-llvm-cov not found; install with: cargo install cargo-llvm-cov --locked" >&2; \
		exit 1; \
	}
	$(CARGO) llvm-cov nextest \
		$(if $(PKG),-p $(PKG),$(COVERAGE_FLAGS)) \
		--profile ci \
		--html \
		--output-dir target/llvm-cov/html
	@echo "HTML report: target/llvm-cov/html/index.html"

.PHONY: coverage-lcov
coverage-lcov: ## LCOV report → $(COVERAGE_LCOV) (default target/llvm-cov/lcov.info)
	@command -v cargo-llvm-cov >/dev/null 2>&1 || { \
		echo "error: cargo-llvm-cov not found; install with: cargo install cargo-llvm-cov --locked" >&2; \
		exit 1; \
	}
	@mkdir -p $(dir $(COVERAGE_LCOV))
	$(CARGO) llvm-cov nextest \
		$(if $(PKG),-p $(PKG),$(COVERAGE_FLAGS)) \
		--profile ci \
		--lcov \
		--output-path $(COVERAGE_LCOV)
	@echo "LCOV report: $(COVERAGE_LCOV)"

.PHONY: coverage-open
coverage-open: coverage-html ## HTML report and open in browser (macOS)
	@open target/llvm-cov/html/index.html 2>/dev/null \
		|| xdg-open target/llvm-cov/html/index.html 2>/dev/null \
		|| echo "Open target/llvm-cov/html/index.html in a browser"

.PHONY: coverage-clean
coverage-clean: ## Remove llvm-cov artifacts (profraw + reports)
	$(CARGO) llvm-cov clean --workspace
	rm -rf target/llvm-cov

# ══════════════════════════════════════════════════════════════════════════════
# Lint / format (required CI: fmt, clippy)
# ══════════════════════════════════════════════════════════════════════════════

.PHONY: fmt
fmt: ## Format all Rust sources
	$(CARGO) fmt --all

.PHONY: fmt-check
fmt-check: ## Check formatting (CI: fmt job)
	$(CARGO) fmt --all --check

.PHONY: clippy
clippy: ## Clippy with -D warnings (CI: clippy job)
	$(CARGO) clippy $(CLIPPY_FLAGS) -- -D warnings

.PHONY: lint
lint: fmt-check clippy check-dag check-env check-commit-anchor check-http check-inproc-grpc check-gha-pins check-ci-jobs check-compose-uris check-fork-schedule check-offhost-policy check-adr-resolver ## Local lint suite (fmt + clippy + guards)

# ══════════════════════════════════════════════════════════════════════════════
# Policy guards (scripts/ — wired into the matching CI job and `make ci`)
# ══════════════════════════════════════════════════════════════════════════════

.PHONY: check-dag
check-dag: ## Enforce crate dependency DAG (Architecture §2.2)
	bash $(SCRIPTS)/check-crate-dag.sh

.PHONY: check-env
check-env: ## No std::env::var outside crates/config (CC-09/3)
	bash $(SCRIPTS)/check-no-env-reads.sh

.PHONY: check-commit-anchor
check-commit-anchor: ## Exactly one non-test commit_anchor( call, under bin/beacon-core/src
	bash $(SCRIPTS)/check-one-commit-anchor.sh

.PHONY: check-http
check-http: ## No HTTP client on the block-import path (CC-28/2)
	bash $(SCRIPTS)/check-no-http-import-path.sh

.PHONY: check-inproc-grpc
check-inproc-grpc: ## S2-A-13/A-14: in-process import path cargo tree has no tonic / cc-proto
	bash $(SCRIPTS)/check-no-grpc-beacon-inproc.sh

.PHONY: check-remodelling
check-remodelling: ## No consensus containers remodelled as protos (CC-02/4)
	bash $(SCRIPTS)/check-no-remodelling.sh

.PHONY: check-gha-pins
check-gha-pins: ## GitHub Actions pinned to commit SHAs (S0a-B-06)
	bash $(SCRIPTS)/check-gha-sha-pins.sh

.PHONY: check-ci-jobs
check-ci-jobs: ## Makefile CI_JOBS matches ci.yml job ids (S0a-B-04)
	bash $(SCRIPTS)/check-ci-job-list.sh

.PHONY: check-compose-uris
check-compose-uris: ## P0-07 / P1-A/27 compose URI overrides + identity mount
	bash $(SCRIPTS)/check-compose-uri-overrides.sh

.PHONY: check-fork-schedule
check-fork-schedule: ## One fork-schedule walk outside cc-types (S0-A-12)
	bash $(SCRIPTS)/check-fork-schedule-walks.sh

.PHONY: check-offhost-policy
check-offhost-policy: ## S0-B-20 compose host-publish policy (fixtures + live file; not E0.8)
	bash $(SCRIPTS)/offhost-port-scan.sh --policy

.PHONY: check-offhost-scan
check-offhost-scan: ## compose-flag alpine replay (not E0.8 / not M4)
	bash $(SCRIPTS)/offhost-port-scan.sh --scan

.PHONY: check-adr-resolver
check-adr-resolver: ## Extract ADR[ -]<id> + Architecture §<n>; fail unknown ADR ids (S1-B-06)
	bash $(SCRIPTS)/check-adr-resolver.sh

# ══════════════════════════════════════════════════════════════════════════════
# Proto (required CI: proto job)
# ══════════════════════════════════════════════════════════════════════════════

.PHONY: proto-lint
proto-lint: ## buf lint
	buf lint $(PROTO)

.PHONY: proto-fmt
proto-fmt: ## Format protos with buf
	buf format -w $(PROTO)

.PHONY: proto-fmt-check
proto-fmt-check: ## Check proto formatting
	buf format -d --exit-code $(PROTO)

.PHONY: proto-breaking
proto-breaking: ## FILE-category breaking vs origin/develop (needs fetch)
	buf breaking $(PROTO) --against '.git#branch=origin/develop,subdir=proto'

.PHONY: proto-breaking-against
proto-breaking-against: ## Dry-run proto breaking_against from workflow event fixtures
	bash $(SCRIPTS)/proto-breaking-against.sh --self-test

.PHONY: proto
proto: proto-lint proto-fmt-check proto-breaking-against check-remodelling ## Local proto suite (lint + format + baseline + remodelling)

# ══════════════════════════════════════════════════════════════════════════════
# Supply chain (required CI: deps job)
# ══════════════════════════════════════════════════════════════════════════════

.PHONY: deny
deny: ## cargo deny advisories bans licenses sources
	$(CARGO) deny check advisories bans licenses sources

.PHONY: deps
deps: deny check-dag ## Supply-chain gate (deny + DAG)

# ══════════════════════════════════════════════════════════════════════════════
# Spec vectors (non-required CI: vectors job; never implicit in cargo)
# ══════════════════════════════════════════════════════════════════════════════

.PHONY: vectors
vectors: ## Fetch/verify pinned consensus-spec vectors (~8–10 GB)
	bash $(SCRIPTS)/fetch-spec-vectors.sh

.PHONY: vectors-force
vectors-force: ## Re-download every vector artifact
	bash $(SCRIPTS)/fetch-spec-vectors.sh --force

.PHONY: vectors-layout
vectors-layout: ## Regenerate spec-vectors-layout.md from cache
	bash $(SCRIPTS)/record-vector-layout.sh

# ══════════════════════════════════════════════════════════════════════════════
# Docker / compose (non-required CI: compose job)
# ══════════════════════════════════════════════════════════════════════════════

.PHONY: compose-build
compose-build: ## Build compose images (sets CC_GIT_SHA)
	CC_GIT_SHA=$(CC_GIT_SHA) docker compose build

.PHONY: compose-up
compose-up: ## Start stack detached
	CC_GIT_SHA=$(CC_GIT_SHA) docker compose up -d

.PHONY: compose-down
compose-down: ## Stop stack
	docker compose down

.PHONY: compose-ps
compose-ps: ## Show compose service status
	docker compose ps

.PHONY: wait-healthy
wait-healthy: ## Wait until all six services are healthy (default 90s)
	bash $(SCRIPTS)/wait-healthy.sh

.PHONY: prove-health
prove-health: ## Mutual-health proof (stop chain → NOT_SERVING → recover)
	bash $(SCRIPTS)/prove-mutual-health.sh

.PHONY: compose
compose: compose-build compose-up wait-healthy ## Build, up, and wait-healthy

.PHONY: check-compose-uris-runtime
check-compose-uris-runtime: ## P0-07 keys + identity mount inside a running stack
	bash $(SCRIPTS)/check-compose-uri-overrides.sh --runtime

.PHONY: check-offhost-scan-live
check-offhost-scan-live: ## S0-B-20 / E0.8 live-stack scan (the M4 record)
	bash $(SCRIPTS)/offhost-port-scan.sh --live

.PHONY: compose-proof
compose-proof: compose check-compose-uris-runtime prove-health check-offhost-scan-live ## Full mutual-health proof (compose CI path)

# ══════════════════════════════════════════════════════════════════════════════
# Meta
# ══════════════════════════════════════════════════════════════════════════════

.PHONY: ci
ci: lint test-seam test proto deps ## Required local CI gates (no compose/vectors)

.PHONY: clean
clean: ## cargo clean
	$(CARGO) clean

.PHONY: clean-all
clean-all: clean coverage-clean compose-down ## cargo clean + coverage artifacts + compose down
