# Convenience targets for the rust-client crate

.PHONY: fix test clippy build

fix:
	cd rust-client && cargo fix --lib

test:
	cd rust-client && cargo test

clippy:
	cd rust-client && cargo clippy --all-targets --all-features

build:
	cd rust-client && cargo build --release
