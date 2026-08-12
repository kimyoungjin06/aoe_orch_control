#!/bin/bash
set -e

REPO="kimyoungjin06/forager-cli"
INSTALL_DIR="${INSTALL_DIR:-/usr/local/bin}"
BINARY_NAME="forager"
LEGACY_BINARY_NAME="aoe"

info() { printf "\033[34m[info]\033[0m %s\n" "$1"; }
success() { printf "\033[32m[ok]\033[0m %s\n" "$1"; }
error() { printf "\033[31m[error]\033[0m %s\n" "$1" >&2; exit 1; }

detect_platform() {
    local os arch
    os=$(uname -s | tr '[:upper:]' '[:lower:]')
    arch=$(uname -m)

    case "$os" in
        linux) os="linux" ;;
        darwin) os="darwin" ;;
        *) error "Unsupported OS: $os" ;;
    esac

    case "$arch" in
        x86_64|amd64) arch="amd64" ;;
        aarch64|arm64) arch="arm64" ;;
        *) error "Unsupported architecture: $arch" ;;
    esac

    echo "${os}-${arch}"
}

get_latest_version() {
    if [ -n "${FORAGER_VERSION:-}" ]; then
        echo "$FORAGER_VERSION"
        return 0
    fi

    curl -fsSL "https://api.github.com/repos/${REPO}/releases/latest" \
        | grep '"tag_name"' \
        | sed -E 's/.*"([^"]+)".*/\1/'
}

sha256_file() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$1" | awk '{print $1}'
    elif command -v shasum >/dev/null 2>&1; then
        shasum -a 256 "$1" | awk '{print $1}'
    else
        error "No SHA-256 tool found. Install sha256sum or shasum and retry."
    fi
}

verify_archive() {
    local archive checksum_file expected actual
    archive="$1"
    checksum_file="$2"
    expected=$(awk 'NR == 1 {print $1}' "$checksum_file")
    if ! printf '%s\n' "$expected" | grep -qE '^[[:xdigit:]]{64}$'; then
        error "Release checksum file is malformed"
    fi
    actual=$(sha256_file "$archive")
    if [ "$(printf '%s' "$actual" | tr '[:upper:]' '[:lower:]')" != \
         "$(printf '%s' "$expected" | tr '[:upper:]' '[:lower:]')" ]; then
        error "Release checksum verification failed"
    fi
}

extract_archive() {
    local archive archive_member destination
    archive="$1"
    archive_member="$2"
    destination="$3"
    if [ "$(tar tzf "$archive")" != "$archive_member" ]; then
        error "Release archive contains unexpected files"
    fi
    tar xzf "$archive" -C "$destination" "$archive_member"
    if [ ! -f "$destination/$archive_member" ] || [ -L "$destination/$archive_member" ]; then
        error "Release archive does not contain a regular binary"
    fi
}

main() {
    info "Detecting platform..."
    platform=$(detect_platform)
    success "Platform: $platform"

    if [ -n "${FORAGER_VERSION:-}" ]; then
        info "Using pinned version from FORAGER_VERSION..."
    else
        info "Fetching latest release version..."
    fi
    if ! version=$(get_latest_version); then
        error "Failed to fetch latest GitHub release. Set FORAGER_VERSION=vX.Y.Z or build from source."
    fi
    if [ -z "$version" ]; then
        error "No GitHub release version found. Set FORAGER_VERSION=vX.Y.Z or build from source."
    fi
    success "Version: $version"

    download_url="https://github.com/${REPO}/releases/download/${version}/forager-${platform}.tar.gz"
    legacy_download_url="https://github.com/${REPO}/releases/download/${version}/aoe-${platform}.tar.gz"
    checksum_url="${download_url}.sha256"
    archive_member="forager-${platform}"
    info "Downloading from: $download_url"

    tmp_dir=$(mktemp -d)
    trap 'rm -rf "$tmp_dir"' EXIT

    if ! curl -fsSL "$download_url" -o "$tmp_dir/forager.tar.gz"; then
        info "Primary Forager artifact not found; trying legacy artifact: $legacy_download_url"
        archive_member="aoe-${platform}"
        checksum_url="${legacy_download_url}.sha256"
        curl -fsSL "$legacy_download_url" -o "$tmp_dir/forager.tar.gz" || error "Download failed"
    fi
    success "Downloaded successfully"

    info "Verifying release checksum..."
    curl -fsSL "$checksum_url" -o "$tmp_dir/forager.tar.gz.sha256" \
        || error "Release checksum download failed"
    verify_archive "$tmp_dir/forager.tar.gz" "$tmp_dir/forager.tar.gz.sha256"
    success "Checksum verified"

    info "Extracting..."
    extract_archive "$tmp_dir/forager.tar.gz" "$archive_member" "$tmp_dir"

    info "Installing to $INSTALL_DIR..."
    if [ -w "$INSTALL_DIR" ]; then
        mv "$tmp_dir/$archive_member" "$INSTALL_DIR/$BINARY_NAME"
        ln -sf "$BINARY_NAME" "$INSTALL_DIR/$LEGACY_BINARY_NAME"
        chmod +x "$INSTALL_DIR/$BINARY_NAME"
    else
        sudo mv "$tmp_dir/$archive_member" "$INSTALL_DIR/$BINARY_NAME"
        sudo ln -sf "$BINARY_NAME" "$INSTALL_DIR/$LEGACY_BINARY_NAME"
        sudo chmod +x "$INSTALL_DIR/$BINARY_NAME"
    fi

    success "Installed $BINARY_NAME $version to $INSTALL_DIR/$BINARY_NAME"
    info "Legacy alias available at $INSTALL_DIR/$LEGACY_BINARY_NAME"

    if ! command -v tmux &> /dev/null; then
        info ""
        info "Note: tmux is required but not installed."
        info "Install it with:"
        info "  Debian/Ubuntu: sudo apt install tmux"
        info "  Fedora/RHEL:   sudo dnf install tmux"
        info "  Arch:          sudo pacman -S tmux"
        info "  macOS:         brew install tmux"
    fi

    echo ""
    success "Run 'forager' to get started!"
    echo ""
    info "For shell completions, run: forager completion --help"
}

if [ "${FORAGER_INSTALL_LIBRARY_ONLY:-0}" != "1" ]; then
    main "$@"
fi
