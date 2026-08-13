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

validate_version() {
    printf '%s\n' "$1" | grep -qE '^v[0-9]+\.[0-9]+\.[0-9]+(-[a-zA-Z0-9.]+)?$'
}

read_release_source_sha() {
    local manifest source_sha
    manifest="$1"
    source_sha=$(sed -n 's/^[[:space:]]*"source_sha":[[:space:]]*"\([0-9a-fA-F]*\)".*/\1/p' \
        "$manifest" | head -1)
    if ! printf '%s\n' "$source_sha" | grep -qE '^[[:xdigit:]]{40}$'; then
        return 1
    fi
    printf '%s\n' "$(printf '%s' "$source_sha" | tr '[:upper:]' '[:lower:]')"
}

release_manifest_matches_archive() {
    local manifest archive_name archive_sha
    manifest="$1"
    archive_name="$2"
    archive_sha="$3"
    grep -Fq "\"name\": \"$archive_name\"" "$manifest" &&
        grep -Fq "\"sha256\": \"$archive_sha\"" "$manifest"
}

default_install_receipt() {
    local platform state_root
    platform="$1"
    case "$platform" in
        linux-*)
            state_root="${XDG_STATE_HOME:-$HOME/.local/state}"
            printf '%s\n' "$state_root/forager/install-receipt.txt"
            ;;
        darwin-*)
            printf '%s\n' "$HOME/.forager/install-receipt.txt"
            ;;
        *)
            return 1
            ;;
    esac
}

write_install_receipt() {
    local receipt_file version platform binary_path binary_sha archive_sha archive_url source_sha
    local receipt_dir receipt_tmp installed_at
    receipt_file="$1"
    version="$2"
    platform="$3"
    binary_path="$4"
    binary_sha="$5"
    archive_sha="$6"
    archive_url="$7"
    source_sha="$8"
    receipt_dir=$(dirname "$receipt_file")
    receipt_tmp="${receipt_file}.tmp.$$"
    installed_at=$(date -u +'%Y-%m-%dT%H:%M:%SZ')

    mkdir -p "$receipt_dir" || return 1
    umask 077
    {
        printf 'schema=forager_install_receipt.v1\n'
        printf 'version=%s\n' "$version"
        printf 'source_sha=%s\n' "$source_sha"
        printf 'platform=%s\n' "$platform"
        printf 'installed_at=%s\n' "$installed_at"
        printf 'binary_path=%s\n' "$binary_path"
        printf 'binary_sha256=%s\n' "$binary_sha"
        printf 'archive_sha256=%s\n' "$archive_sha"
        printf 'archive_url=%s\n' "$archive_url"
    } > "$receipt_tmp" || return 1
    mv "$receipt_tmp" "$receipt_file" || return 1
    chmod 600 "$receipt_file" || return 1
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
    if ! validate_version "$version"; then
        error "Invalid release version: $version"
    fi
    success "Version: $version"

    download_url="https://github.com/${REPO}/releases/download/${version}/forager-${platform}.tar.gz"
    legacy_download_url="https://github.com/${REPO}/releases/download/${version}/aoe-${platform}.tar.gz"
    checksum_url="${download_url}.sha256"
    provenance_url="https://github.com/${REPO}/releases/download/${version}/release-provenance.json"
    resolved_download_url="$download_url"
    archive_member="forager-${platform}"
    info "Downloading from: $download_url"

    tmp_dir=$(mktemp -d)
    trap 'rm -rf "$tmp_dir"' EXIT

    if ! curl -fsSL "$download_url" -o "$tmp_dir/forager.tar.gz"; then
        info "Primary Forager artifact not found; trying legacy artifact: $legacy_download_url"
        archive_member="aoe-${platform}"
        checksum_url="${legacy_download_url}.sha256"
        resolved_download_url="$legacy_download_url"
        curl -fsSL "$legacy_download_url" -o "$tmp_dir/forager.tar.gz" || error "Download failed"
    fi
    success "Downloaded successfully"

    info "Verifying release checksum..."
    curl -fsSL "$checksum_url" -o "$tmp_dir/forager.tar.gz.sha256" \
        || error "Release checksum download failed"
    verify_archive "$tmp_dir/forager.tar.gz" "$tmp_dir/forager.tar.gz.sha256"
    archive_sha=$(sha256_file "$tmp_dir/forager.tar.gz")
    success "Checksum verified"

    source_sha="unavailable"
    if curl -fsSL "$provenance_url" -o "$tmp_dir/release-provenance.json"; then
        if release_manifest_matches_archive \
            "$tmp_dir/release-provenance.json" \
            "${archive_member}.tar.gz" \
            "$archive_sha" &&
            parsed_source_sha=$(read_release_source_sha "$tmp_dir/release-provenance.json"); then
            source_sha="$parsed_source_sha"
            success "Release source: $source_sha"
        else
            info "Release provenance does not match the archive; install receipt will omit the source commit"
        fi
    else
        info "Release provenance is unavailable; install receipt will omit the source commit"
    fi

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

    installed_binary="$INSTALL_DIR/$BINARY_NAME"
    installed_sha=$(sha256_file "$installed_binary")
    receipt_file="${FORAGER_INSTALL_RECEIPT:-$(default_install_receipt "$platform")}"
    if write_install_receipt \
        "$receipt_file" \
        "$version" \
        "$platform" \
        "$installed_binary" \
        "$installed_sha" \
        "$archive_sha" \
        "$resolved_download_url" \
        "$source_sha"; then
        success "Install receipt: $receipt_file"
    else
        info "Warning: could not write install receipt to $receipt_file"
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
