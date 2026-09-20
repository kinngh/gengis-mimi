#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
mkdir -p .cargo-home .tmp
export CARGO_HOME="$PWD/.cargo-home"
export TMPDIR="$PWD/.tmp"
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked --all-targets
cargo doc --locked --no-deps
