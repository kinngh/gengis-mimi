#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
: "${GENGIS_MIMI_TEST_BUCKET:?Set GENGIS_MIMI_TEST_BUCKET to an existing test bucket}"
mkdir -p .cargo-home .tmp
export CARGO_HOME="$PWD/.cargo-home"
export TMPDIR="$PWD/.tmp"
cargo test --locked --test s3 --test cluster -- --ignored --nocapture
