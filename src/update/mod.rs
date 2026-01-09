//! Self-update functionality

use anyhow::Result;
use serde::Deserialize;
use std::fs;
use std::io::Write;
use std::path::PathBuf;

use crate::session::{get_app_dir, get_update_settings};

const GITHUB_API_URL: &str = "https://api.github.com/repos/nbrake/agent-of-empires/releases/latest";

#[derive(Debug, Clone)]
pub struct UpdateInfo {
    pub available: bool,
    pub current_version: String,
    pub latest_version: String,
    pub download_url: String,
    pub release_url: String,
}

#[derive(Debug, Deserialize)]
struct GitHubRelease {
    tag_name: String,
    html_url: String,
    assets: Vec<GitHubAsset>,
}

#[derive(Debug, Deserialize)]
struct GitHubAsset {
    name: String,
    browser_download_url: String,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct UpdateCache {
    checked_at: chrono::DateTime<chrono::Utc>,
    latest_version: String,
    download_url: String,
    release_url: String,
}

fn cache_path() -> Result<PathBuf> {
    Ok(get_app_dir()?.join("update_cache.json"))
}

fn load_cache() -> Option<UpdateCache> {
    let path = cache_path().ok()?;
    let content = fs::read_to_string(&path).ok()?;
    serde_json::from_str(&content).ok()
}

fn save_cache(cache: &UpdateCache) -> Result<()> {
    let path = cache_path()?;
    let content = serde_json::to_string_pretty(cache)?;
    fs::write(&path, content)?;
    Ok(())
}

pub async fn check_for_update(current_version: &str, force: bool) -> Result<UpdateInfo> {
    let settings = get_update_settings();

    // Check cache first (unless forcing)
    if !force {
        if let Some(cache) = load_cache() {
            let age = chrono::Utc::now() - cache.checked_at;
            let max_age = chrono::Duration::hours(settings.check_interval_hours as i64);

            if age < max_age {
                let available = is_newer_version(&cache.latest_version, current_version);
                return Ok(UpdateInfo {
                    available,
                    current_version: current_version.to_string(),
                    latest_version: cache.latest_version,
                    download_url: cache.download_url,
                    release_url: cache.release_url,
                });
            }
        }
    }

    // Fetch from GitHub
    let client = reqwest::Client::builder()
        .user_agent("agent-of-empires")
        .build()?;

    let response = client.get(GITHUB_API_URL).send().await?;

    if !response.status().is_success() {
        anyhow::bail!("Failed to check for updates: HTTP {}", response.status());
    }

    let release: GitHubRelease = response.json().await?;
    let latest_version = release.tag_name.trim_start_matches('v').to_string();

    // Find the right asset for this platform
    let download_url = find_download_url(&release.assets)?;

    // Update cache
    let cache = UpdateCache {
        checked_at: chrono::Utc::now(),
        latest_version: latest_version.clone(),
        download_url: download_url.clone(),
        release_url: release.html_url.clone(),
    };
    let _ = save_cache(&cache);

    let available = is_newer_version(&latest_version, current_version);

    Ok(UpdateInfo {
        available,
        current_version: current_version.to_string(),
        latest_version,
        download_url,
        release_url: release.html_url,
    })
}

fn find_download_url(assets: &[GitHubAsset]) -> Result<String> {
    let arch = std::env::consts::ARCH;

    #[cfg(target_os = "macos")]
    let expected_name = if arch == "aarch64" {
        "agent-of-empires-darwin-arm64"
    } else {
        "agent-of-empires-darwin-amd64"
    };

    #[cfg(target_os = "linux")]
    let expected_name = if arch == "aarch64" {
        "agent-of-empires-linux-arm64"
    } else {
        "agent-of-empires-linux-amd64"
    };

    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    anyhow::bail!("Unsupported platform for auto-update");

    for asset in assets {
        if asset.name.contains(expected_name) {
            return Ok(asset.browser_download_url.clone());
        }
    }

    anyhow::bail!("No compatible binary found for this platform")
}

fn is_newer_version(latest: &str, current: &str) -> bool {
    let parse_version =
        |v: &str| -> Vec<u32> { v.split('.').filter_map(|s| s.parse().ok()).collect() };

    let latest_parts = parse_version(latest);
    let current_parts = parse_version(current);

    for i in 0..latest_parts.len().max(current_parts.len()) {
        let l = latest_parts.get(i).copied().unwrap_or(0);
        let c = current_parts.get(i).copied().unwrap_or(0);
        if l > c {
            return true;
        }
        if l < c {
            return false;
        }
    }
    false
}

pub async fn perform_update(download_url: &str) -> Result<()> {
    println!("Downloading update...");

    let client = reqwest::Client::builder()
        .user_agent("agent-of-empires")
        .build()?;

    let response = client.get(download_url).send().await?;

    if !response.status().is_success() {
        anyhow::bail!("Failed to download update: HTTP {}", response.status());
    }

    let bytes = response.bytes().await?;

    // Get current executable path
    let current_exe = std::env::current_exe()?;
    let backup_path = current_exe.with_extension("bak");

    // Backup current binary
    println!("Backing up current version...");
    fs::rename(&current_exe, &backup_path)?;

    // Write new binary
    println!("Installing new version...");
    let mut file = fs::File::create(&current_exe)?;
    file.write_all(&bytes)?;

    // Make executable
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = file.metadata()?.permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&current_exe, perms)?;
    }

    // Remove backup
    let _ = fs::remove_file(&backup_path);

    println!("✓ Update complete!");

    Ok(())
}

pub async fn print_update_notice() {
    let settings = get_update_settings();
    if !settings.check_enabled || !settings.notify_in_cli {
        return;
    }

    let version = env!("CARGO_PKG_VERSION");

    if let Ok(info) = check_for_update(version, false).await {
        if info.available {
            eprintln!(
                "\n💡 Update available: v{} → v{} (run: agent-of-empires update)",
                info.current_version, info.latest_version
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version_comparison() {
        assert!(is_newer_version("1.0.1", "1.0.0"));
        assert!(is_newer_version("1.1.0", "1.0.9"));
        assert!(is_newer_version("2.0.0", "1.9.9"));
        assert!(!is_newer_version("1.0.0", "1.0.0"));
        assert!(!is_newer_version("1.0.0", "1.0.1"));
    }
}
