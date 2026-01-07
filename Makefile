# Convenience targets for local development.
#
# Notes:
# - Rust commands are run via manifest paths so you don't have to `cd`.
# - Python targets assume you've installed dev deps (e.g. `python -m pip install -e '.[dev]'`).

RUST_CLIENT_MANIFEST := rust-client/Cargo.toml
INGESTION_SERVICE_MANIFEST := ingestion-service/Cargo.toml

# Prefer python3, fall back to python.
PYTHON ?= $(shell command -v python3 2>/dev/null || command -v python 2>/dev/null)

.PHONY: \
	ci test \
	rust-test rust-clippy build fix \
	python-install-dev python-test python-lint python-typecheck python-ci \
	check-python

# --- Top-level wrappers ---

test: rust-test python-test

ci: rust-test rust-clippy python-ci

# --- Rust ---

fix:
	cargo fix --manifest-path $(RUST_CLIENT_MANIFEST) --lib

rust-test:
	cargo test --manifest-path $(RUST_CLIENT_MANIFEST)
	cargo test --manifest-path $(INGESTION_SERVICE_MANIFEST)

rust-clippy:
	cargo clippy --manifest-path $(RUST_CLIENT_MANIFEST) --all-targets --all-features
	cargo clippy --manifest-path $(INGESTION_SERVICE_MANIFEST) --all-targets --all-features

build:
	cargo build --manifest-path $(RUST_CLIENT_MANIFEST) --release

# --- Python ---

check-python:
	@if [ -z "$(PYTHON)" ]; then \
		echo "python3/python not found on PATH"; \
		exit 1; \
	fi

python-install-dev: check-python
	$(PYTHON) -m pip install -e ".[dev]"

python-test: check-python
	$(PYTHON) -m pytest -q

python-lint: check-python
	$(PYTHON) -m ruff check .

python-typecheck: check-python
	$(PYTHON) -m mypy src/utility_ts_analytics

python-ci: python-lint python-typecheck python-test
