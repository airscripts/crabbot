CARGO ?= cargo
JOBS ?= 4
COVERAGE_PROFILE ?= full
comma := ,
.DEFAULT_GOAL := all
.PHONY: all help install hooks fmt spacing clippy check test coverage build release metrics ci revloop verify

all: build
	$(MAKE) install

help:
	@printf '%s\n' 'Krabb development targets:'
	@printf '%s\n' '  all       Build the workspace, then install the CLI and daemon (default)'
	@printf '%s\n' '  install   Install the krabb and krabb-daemon binaries only'
	@printf '%s\n' '  hooks     Install Lefthook git hooks'
	@printf '%s\n' '  fmt       Check Rust, JavaScript, and Python formatting and spacing'
	@printf '%s\n' '  spacing   Apply code block spacing'
	@printf '%s\n' '  clippy    Run Clippy with warnings denied'
	@printf '%s\n' '  check     Type-check the workspace'
	@printf '%s\n' '  test      Run all workspace tests without stopping at the first failure'
	@printf '%s\n' '  coverage  Run all package tests and coverage gates, then summarize failures'
	@printf '%s\n' '  build     Build the debug workspace'
	@printf '%s\n' '  release   Build optimized binaries'
	@printf '%s\n' '  metrics   Report source and test counts'
	@printf '%s\n' '  ci        Run local preflight, then the non-release CI pipeline with act'
	@printf '%s\n' '  revloop   Run the bounded independent review loop'
	@printf '%s\n' '  verify    Run the complete local quality workflow'

install:
	$(CARGO) install --path krabb --locked --force
	$(CARGO) install --path krabb-daemon --locked --force

hooks:
	lefthook install

fmt:
	$(CARGO) fmt --all --check
	bash krabb-scripts/spacing.sh --check
	python3 krabb-scripts/code-spacing.py --check

spacing:
	bash krabb-scripts/spacing.sh
	python3 krabb-scripts/code-spacing.py --fix

clippy:
	CARGO_BUILD_JOBS=$(JOBS) $(CARGO) clippy --workspace --all-targets --locked -- -D warnings

check:
	CARGO_BUILD_JOBS=$(JOBS) $(CARGO) check --workspace --locked

test:
	CARGO_BUILD_JOBS=$(JOBS) $(CARGO) test --workspace --no-fail-fast --locked

coverage:
	bash krabb-scripts/coverage.sh "$(COVERAGE_PROFILE)"

build:
	CARGO_BUILD_JOBS=$(JOBS) $(CARGO) build --workspace --locked

release:
	CARGO_BUILD_JOBS=$(JOBS) $(CARGO) build --workspace --release --locked

metrics:
	bash krabb-scripts/metrics.sh

ci:
	bash krabb-ci/ci.sh

revloop:
	bash krabb-scripts/revloop.sh $(subst $(comma), ,$(REVLOOP_ARGS))

verify: fmt clippy check test coverage build metrics
