//! Process utilities for tmux session management

use std::collections::BTreeSet;
use std::ffi::OsString;
use std::fs::{File, Metadata};
use std::io;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Instant, SystemTime};

#[cfg(target_os = "linux")]
mod linux;

#[cfg(target_os = "macos")]
mod macos;

#[derive(Debug, Clone)]
pub struct FilesystemBoundary {
    root: PathBuf,
    root_device: Option<u64>,
    mount_points: BTreeSet<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FilesystemObjectSnapshot {
    identity: Option<(u64, u64)>,
    size_bytes: u64,
    modified_at: Option<SystemTime>,
    changed_at: Option<(i64, i64)>,
    is_directory: bool,
}

impl FilesystemObjectSnapshot {
    pub fn from_metadata(metadata: &Metadata) -> Self {
        Self {
            identity: filesystem_file_identity(metadata),
            size_bytes: metadata.len(),
            modified_at: metadata.modified().ok(),
            changed_at: filesystem_change_time(metadata),
            is_directory: metadata.is_dir(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DirectoryReadLimit {
    EntryCount,
    ElapsedTime,
}

#[derive(Debug)]
pub struct BoundedDirectoryRead {
    pub names: Vec<OsString>,
    pub snapshot: FilesystemObjectSnapshot,
    pub entries_observed: u64,
    pub limit: Option<DirectoryReadLimit>,
    pub stable: bool,
}

impl FilesystemBoundary {
    pub fn for_root(root: &Path, metadata: &Metadata) -> Self {
        Self {
            root: root.to_path_buf(),
            root_device: filesystem_device_id(metadata),
            mount_points: known_mount_points(),
        }
    }

    pub fn is_descendant_mount(&self, path: &Path, metadata: &Metadata) -> bool {
        path != self.root
            && (self.mount_points.contains(path)
                || self
                    .root_device
                    .zip(filesystem_device_id(metadata))
                    .is_some_and(|(root_device, path_device)| root_device != path_device))
    }
}

pub fn open_bounded_root(root: &Path) -> io::Result<File> {
    #[cfg(target_os = "linux")]
    {
        linux::open_bounded_root(root)
    }

    #[cfg(target_os = "macos")]
    {
        macos::open_bounded_root(root)
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        File::open(root)
    }
}

pub fn open_bounded_regular_file(root: &File, relative_path: &Path) -> io::Result<File> {
    #[cfg(target_os = "linux")]
    {
        linux::open_bounded_regular_file(root, relative_path)
    }

    #[cfg(target_os = "macos")]
    {
        macos::open_bounded_regular_file(root, relative_path)
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = (root, relative_path);
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "bounded no-follow document reads are unavailable on this platform",
        ))
    }
}

pub fn open_bounded_subdirectory(root: &File, relative_path: &Path) -> io::Result<File> {
    #[cfg(target_os = "linux")]
    {
        linux::open_bounded_subdirectory(root, relative_path)
    }

    #[cfg(target_os = "macos")]
    {
        macos::open_bounded_subdirectory(root, relative_path)
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = (root, relative_path);
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "bounded no-follow directory reads are unavailable on this platform",
        ))
    }
}

pub fn ensure_bounded_directory(root: &File, relative_path: &Path) -> io::Result<File> {
    #[cfg(target_os = "linux")]
    {
        linux::ensure_bounded_directory(root, relative_path)
    }

    #[cfg(target_os = "macos")]
    {
        macos::ensure_bounded_directory(root, relative_path)
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = (root, relative_path);
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "bounded directory creation is unavailable on this platform",
        ))
    }
}

pub fn open_or_create_bounded_regular_file(root: &File, relative_path: &Path) -> io::Result<File> {
    #[cfg(target_os = "linux")]
    {
        linux::open_or_create_bounded_regular_file(root, relative_path)
    }

    #[cfg(target_os = "macos")]
    {
        macos::open_or_create_bounded_regular_file(root, relative_path)
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = (root, relative_path);
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "bounded file creation is unavailable on this platform",
        ))
    }
}

pub fn create_new_bounded_regular_file(root: &File, relative_path: &Path) -> io::Result<File> {
    #[cfg(target_os = "linux")]
    {
        linux::create_new_bounded_regular_file(root, relative_path)
    }

    #[cfg(target_os = "macos")]
    {
        macos::create_new_bounded_regular_file(root, relative_path)
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = (root, relative_path);
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "bounded file creation is unavailable on this platform",
        ))
    }
}

pub fn finalize_bounded_file_noreplace(
    root: &File,
    directory: &Path,
    partial_name: &Path,
    final_name: &Path,
) -> io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        linux::finalize_bounded_file_noreplace(root, directory, partial_name, final_name)
    }

    #[cfg(target_os = "macos")]
    {
        macos::finalize_bounded_file_noreplace(root, directory, partial_name, final_name)
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = (root, directory, partial_name, final_name);
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "bounded no-replace finalization is unavailable on this platform",
        ))
    }
}

pub fn replace_bounded_root_file(
    root: &File,
    temporary_name: &Path,
    final_name: &Path,
) -> io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        linux::replace_bounded_root_file(root, temporary_name, final_name)
    }

    #[cfg(target_os = "macos")]
    {
        macos::replace_bounded_root_file(root, temporary_name, final_name)
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = (root, temporary_name, final_name);
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "bounded atomic replacement is unavailable on this platform",
        ))
    }
}

pub fn remove_bounded_root_file(root: &File, name: &Path) -> io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        linux::remove_bounded_root_file(root, name)
    }

    #[cfg(target_os = "macos")]
    {
        macos::remove_bounded_root_file(root, name)
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = (root, name);
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "bounded file removal is unavailable on this platform",
        ))
    }
}

pub fn rename_bounded_root_file_unsynced(
    root: &File,
    source_name: &Path,
    target_name: &Path,
) -> io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        linux::rename_bounded_root_file_unsynced(root, source_name, target_name)
    }

    #[cfg(target_os = "macos")]
    {
        macos::rename_bounded_root_file_unsynced(root, source_name, target_name)
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = (root, source_name, target_name);
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "bounded unsynced rename is unavailable on this platform",
        ))
    }
}

pub fn link_bounded_root_file_noreplace_unsynced(
    root: &File,
    source_name: &Path,
    target_name: &Path,
) -> io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        linux::link_bounded_root_file_noreplace_unsynced(root, source_name, target_name)
    }

    #[cfg(target_os = "macos")]
    {
        macos::link_bounded_root_file_noreplace_unsynced(root, source_name, target_name)
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = (root, source_name, target_name);
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "bounded no-replace link is unavailable on this platform",
        ))
    }
}

pub fn remove_bounded_root_file_unsynced(root: &File, name: &Path) -> io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        linux::remove_bounded_root_file_unsynced(root, name)
    }

    #[cfg(target_os = "macos")]
    {
        macos::remove_bounded_root_file_unsynced(root, name)
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = (root, name);
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "bounded unsynced file removal is unavailable on this platform",
        ))
    }
}

pub fn create_bounded_root_directory_unsynced(root: &File, name: &Path) -> io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        linux::create_bounded_root_directory_unsynced(root, name)
    }

    #[cfg(target_os = "macos")]
    {
        macos::create_bounded_root_directory_unsynced(root, name)
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = (root, name);
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "bounded directory creation is unavailable on this platform",
        ))
    }
}

pub fn remove_bounded_root_directory_unsynced(root: &File, name: &Path) -> io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        linux::remove_bounded_root_directory_unsynced(root, name)
    }

    #[cfg(target_os = "macos")]
    {
        macos::remove_bounded_root_directory_unsynced(root, name)
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = (root, name);
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "bounded directory removal is unavailable on this platform",
        ))
    }
}

pub fn read_bounded_directory(
    root: &File,
    relative_path: &Path,
    max_entries: u64,
    scan_started: Instant,
    max_elapsed_ms: u64,
) -> io::Result<BoundedDirectoryRead> {
    #[cfg(target_os = "linux")]
    {
        linux::read_bounded_directory(
            root,
            relative_path,
            max_entries,
            scan_started,
            max_elapsed_ms,
        )
    }

    #[cfg(target_os = "macos")]
    {
        macos::read_bounded_directory(
            root,
            relative_path,
            max_entries,
            scan_started,
            max_elapsed_ms,
        )
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = (
            root,
            relative_path,
            max_entries,
            scan_started,
            max_elapsed_ms,
        );
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "bounded no-follow directory reads are unavailable on this platform",
        ))
    }
}

pub fn bounded_directory_snapshot(
    root: &File,
    relative_path: &Path,
) -> io::Result<FilesystemObjectSnapshot> {
    #[cfg(target_os = "linux")]
    {
        linux::bounded_directory_snapshot(root, relative_path)
    }

    #[cfg(target_os = "macos")]
    {
        macos::bounded_directory_snapshot(root, relative_path)
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = (root, relative_path);
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "bounded no-follow directory snapshots are unavailable on this platform",
        ))
    }
}

pub fn filesystem_file_identity(metadata: &Metadata) -> Option<(u64, u64)> {
    #[cfg(target_os = "linux")]
    {
        Some(linux::filesystem_file_identity(metadata))
    }

    #[cfg(target_os = "macos")]
    {
        Some(macos::filesystem_file_identity(metadata))
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = metadata;
        None
    }
}

fn filesystem_device_id(metadata: &Metadata) -> Option<u64> {
    filesystem_file_identity(metadata).map(|(device, _)| device)
}

fn filesystem_change_time(metadata: &Metadata) -> Option<(i64, i64)> {
    #[cfg(target_os = "linux")]
    {
        Some(linux::filesystem_change_time(metadata))
    }

    #[cfg(target_os = "macos")]
    {
        Some(macos::filesystem_change_time(metadata))
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = metadata;
        None
    }
}

fn known_mount_points() -> BTreeSet<PathBuf> {
    #[cfg(target_os = "linux")]
    {
        linux::known_mount_points()
    }

    #[cfg(target_os = "macos")]
    {
        macos::known_mount_points()
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        BTreeSet::new()
    }
}

/// Get the PID of the shell process running in a tmux pane
pub fn get_pane_pid(session_name: &str) -> Option<u32> {
    let output = Command::new("tmux")
        .args(["display-message", "-t", session_name, "-p", "#{pane_pid}"])
        .output()
        .ok()?;

    if !output.status.success() {
        return None;
    }

    String::from_utf8_lossy(&output.stdout).trim().parse().ok()
}

/// Get the foreground process group leader PID for a given shell PID
/// This finds the actual process that has the terminal foreground
pub fn get_foreground_pid(shell_pid: u32) -> Option<u32> {
    #[cfg(target_os = "linux")]
    {
        linux::get_foreground_pid(shell_pid)
    }

    #[cfg(target_os = "macos")]
    {
        macos::get_foreground_pid(shell_pid)
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = shell_pid;
        None
    }
}

/// Return a platform-specific identity for a live process.
///
/// The identity includes process start metadata so a persisted PID cannot be
/// reused as termination authority for an unrelated process.
pub fn process_identity(pid: u32) -> Option<String> {
    #[cfg(target_os = "linux")]
    {
        linux::process_identity(pid)
    }

    #[cfg(target_os = "macos")]
    {
        macos::process_identity(pid)
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = pid;
        None
    }
}

/// Kill a process and all its descendants
/// Sends SIGTERM first, then SIGKILL to any survivors
pub fn kill_process_tree(pid: u32) {
    #[cfg(target_os = "linux")]
    {
        linux::kill_process_tree(pid);
    }

    #[cfg(target_os = "macos")]
    {
        macos::kill_process_tree(pid);
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = pid;
        // No-op on unsupported platforms, fall back to tmux kill-session only
    }
}
