.DEFAULT_GOAL := all

.PHONY: install-rust-coverage
install-rust-coverage:
	cargo install rustfilt cargo-binutils
	rustup component add llvm-tools-preview

.PHONY: build-prod
build-prod:
	cargo build --release

.PHONY: format
format:
	cargo fmt

.PHONY: lint
lint:
	cargo fmt --version
	cargo fmt --all -- --check
	cargo clippy --version
	cargo clippy -- -D warnings -A incomplete_features
	cargo doc

.PHONY: test
test:
	RUSTFLAGS='-Z macro-backtrace' cargo test

.PHONY: bench
bench:
	cargo bench

.PHONY: testcov
testcov:
	RUSTFLAGS='-C instrument-coverage' cargo test --test main
	coverage-prepare --ignore-filename-regex '/tests/' lcov $(shell find target/debug/deps -regex '.*/main[^.]*')
	genhtml rust_coverage.lcov --output-directory htmlcov
	@echo "HTML coverage report available at htmlcov/index.html"

.PHONY: all
all: format lint test
