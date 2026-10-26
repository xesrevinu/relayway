#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ARCH="${TARGETARCH:-arm64}"
OUT_DIR="$SCRIPT_DIR/bin/linux-$ARCH"

case "$ARCH" in
arm64)
  RUST_TARGET="${RUST_TARGET:-aarch64-unknown-linux-musl}"
  ;;
amd64)
  RUST_TARGET="${RUST_TARGET:-x86_64-unknown-linux-musl}"
  ;;
*)
  echo "unsupported TARGETARCH=$ARCH" >&2
  exit 1
  ;;
esac

mkdir -p "$OUT_DIR"

if [ -x "$HOME/.rustup/toolchains/stable-aarch64-apple-darwin/bin/cargo" ]; then
  export PATH="$HOME/.rustup/toolchains/stable-aarch64-apple-darwin/bin:$PATH"
fi
export RUSTFLAGS="${RUSTFLAGS:--C linker=rust-lld}"

echo "Build relayway-proxy ($RUST_TARGET) -> $OUT_DIR/relayway-proxy"
if command -v cargo >/dev/null 2>&1; then
  if (
    cd "$SCRIPT_DIR"
    cargo build --release --locked --target "$RUST_TARGET" &&
      cp "target/$RUST_TARGET/release/relayway-proxy" "$OUT_DIR/relayway-proxy"
  ); then
    chmod +x "$OUT_DIR/relayway-proxy"
    file "$OUT_DIR/relayway-proxy"
    exit 0
  fi
fi

echo "Native cross build unavailable; falling back to Docker linux/$ARCH builder"
docker run --rm \
  --platform "linux/$ARCH" \
  -v "$SCRIPT_DIR:/work" \
  -w /work \
  rust:1-bookworm \
  sh -c 'cargo build --release --locked && cp target/release/relayway-proxy /work/bin/linux-'"$ARCH"'/relayway-proxy'

chmod +x "$OUT_DIR/relayway-proxy"
file "$OUT_DIR/relayway-proxy"
