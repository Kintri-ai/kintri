#!/bin/sh
# Install the latest kintri release for this machine.
#
#   curl -fsSL https://raw.githubusercontent.com/Kintri-ai/kintri/main/install.sh | sh
#
# Environment (optional):
#   KINTRI_VERSION       a tag such as v0.1.0 (default: the latest release)
#   KINTRI_INSTALL_DIR   where to put the binary (default: ~/.local/bin)
#   GITHUB_TOKEN         needed while the repository is private and `gh` is
#                        not installed
#
# The asset's SHA-256 is checked against the SHA256SUMS file the release
# workflow publishes next to it.
set -eu

REPO="Kintri-ai/kintri"
INSTALL_DIR="${KINTRI_INSTALL_DIR:-$HOME/.local/bin}"

die() { printf 'error: %s\n' "$*" >&2; exit 1; }

os="$(uname -s)"
arch="$(uname -m)"
case "$os" in
    Darwin) case "$arch" in
        arm64|aarch64) target=aarch64-apple-darwin ;;
        x86_64)        target=x86_64-apple-darwin ;;
        *) die "unsupported macOS architecture: $arch" ;;
    esac ;;
    Linux) case "$arch" in
        aarch64|arm64) target=aarch64-unknown-linux-musl ;;
        x86_64)        target=x86_64-unknown-linux-musl ;;
        *) die "unsupported Linux architecture: $arch" ;;
    esac ;;
    *) die "unsupported OS: $os (macOS and Linux are built; see README.md for cargo install)" ;;
esac

if command -v sha256sum >/dev/null 2>&1; then
    sum() { sha256sum "$1" | cut -d' ' -f1; }
elif command -v shasum >/dev/null 2>&1; then
    sum() { shasum -a 256 "$1" | cut -d' ' -f1; }
else
    die "neither sha256sum nor shasum is available"
fi

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

# Two ways to fetch: `gh` (uses its own login, works on private repositories)
# or curl (public repositories, or GITHUB_TOKEN).
if command -v gh >/dev/null 2>&1 && gh auth status >/dev/null 2>&1; then
    tag="${KINTRI_VERSION:-$(gh release view --repo "$REPO" --json tagName -q .tagName)}"
    [ -n "$tag" ] || die "could not determine the latest release"
    version="${tag#v}"
    asset="kintri-${version}-${target}.tar.gz"
    printf '==> kintri %s (%s), via gh\n' "$tag" "$target"
    gh release download "$tag" --repo "$REPO" --dir "$tmp" --pattern "$asset" --pattern SHA256SUMS
else
    auth=""
    [ -n "${GITHUB_TOKEN:-}" ] && auth="Authorization: Bearer ${GITHUB_TOKEN}"
    if [ -n "${KINTRI_VERSION:-}" ]; then
        tag="$KINTRI_VERSION"
    else
        tag="$(curl -fsSL ${auth:+-H "$auth"} -H 'Accept: application/vnd.github+json' \
            "https://api.github.com/repos/${REPO}/releases/latest" \
            | sed -n 's/.*"tag_name": *"\([^"]*\)".*/\1/p' | head -n1)"
    fi
    [ -n "$tag" ] || die "could not determine the latest release (private repository? install gh, or set GITHUB_TOKEN)"
    version="${tag#v}"
    asset="kintri-${version}-${target}.tar.gz"
    printf '==> kintri %s (%s)\n' "$tag" "$target"
    if [ -n "$auth" ]; then
        # Asset downloads on a private repository go through the API with the
        # octet-stream accept header.
        for name in "$asset" SHA256SUMS; do
            id="$(curl -fsSL -H "$auth" -H 'Accept: application/vnd.github+json' \
                "https://api.github.com/repos/${REPO}/releases/tags/${tag}" \
                | tr ',' '\n' | grep -B3 "\"name\": *\"${name}\"" | sed -n 's/.*"id": *\([0-9]*\).*/\1/p' | head -n1)"
            [ -n "$id" ] || die "release ${tag} has no asset ${name}"
            curl -fsSL -H "$auth" -H 'Accept: application/octet-stream' \
                -o "$tmp/$name" "https://api.github.com/repos/${REPO}/releases/assets/${id}"
        done
    else
        for name in "$asset" SHA256SUMS; do
            curl -fsSL -o "$tmp/$name" "https://github.com/${REPO}/releases/download/${tag}/${name}"
        done
    fi
fi

expected="$(grep " ${asset}\$" "$tmp/SHA256SUMS" | cut -d' ' -f1)"
[ -n "$expected" ] || die "SHA256SUMS has no entry for ${asset}"
actual="$(sum "$tmp/$asset")"
[ "$expected" = "$actual" ] || die "checksum mismatch for ${asset}: expected ${expected}, got ${actual}"

tar -xzf "$tmp/$asset" -C "$tmp"
mkdir -p "$INSTALL_DIR"
install -m 0755 "$tmp/kintri-${version}-${target}/kintri" "$INSTALL_DIR/kintri"
printf '==> installed %s\n' "$INSTALL_DIR/kintri"
"$INSTALL_DIR/kintri" --version
case ":$PATH:" in
    *":$INSTALL_DIR:"*) ;;
    *) printf 'note: %s is not on your PATH\n' "$INSTALL_DIR" ;;
esac
