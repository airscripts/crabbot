CARGO ?= cargo
JOBS ?= 4
COVERAGE_PACKAGES = \
  -p crabbot-core \
  -p crabbot-file \
  -p crabbot-runtime \
  -p crabbot \
  -p crabbot-daemon

.DEFAULT_GOAL := help
.PHONY: help install hooks fmt clippy check test coverage build release metrics verify

help:
	@printf '%s\n' 'Crabbot development targets:'
	@printf '%s\n' '  install   Install the crabbot and crabbot-daemon binaries'
	@printf '%s\n' '  hooks     Install Lefthook git hooks'
	@printf '%s\n' '  fmt       Check Rust formatting'
	@printf '%s\n' '  clippy    Run Clippy with warnings denied'
	@printf '%s\n' '  check     Type-check the workspace'
	@printf '%s\n' '  test      Run workspace tests'
	@printf '%s\n' '  coverage  Run the required 80% coverage gate'
	@printf '%s\n' '  build     Build the debug workspace'
	@printf '%s\n' '  release   Build optimized binaries'
	@printf '%s\n' '  metrics   Report source and test counts'
	@printf '%s\n' '  verify    Run the complete local quality workflow'

install:
	$(CARGO) install --path crabbot --force
	$(CARGO) install --path crabbot-daemon --force

hooks:
	lefthook install

fmt:
	$(CARGO) fmt --all --check

clippy:
	CARGO_BUILD_JOBS=$(JOBS) $(CARGO) clippy --workspace --all-targets --locked -- -D warnings

check:
	CARGO_BUILD_JOBS=$(JOBS) $(CARGO) check --workspace --locked

test:
	CARGO_BUILD_JOBS=$(JOBS) $(CARGO) test --workspace --locked

coverage:
	$(CARGO) llvm-cov --fail-under-lines 80 --locked $(COVERAGE_PACKAGES)

build:
	CARGO_BUILD_JOBS=$(JOBS) $(CARGO) build --workspace --locked

release:
	CARGO_BUILD_JOBS=$(JOBS) $(CARGO) build --workspace --release --locked

metrics:
	bash crabbot-scripts/metrics.sh

verify: fmt clippy check test coverage build metrics
