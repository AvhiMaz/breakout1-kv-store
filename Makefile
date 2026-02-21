.PHONY: build run test check clean fmt lint

build:
	cargo build

run:
	cargo run

test:
	cargo test

check:
	cargo clippy 

clean:
	cargo clean

fmt:
	cargo +nightly fmt --all
