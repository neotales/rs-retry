#!/usr/bin/env bash
# The gates CI runs, in the same order, from the repository root.
set -euo pipefail
cd "$(dirname "$0")/.."

cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-targets --all-features
cargo test --workspace --doc --all-features
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps
