//! macOS-specific process utilities

use std::collections::{BTreeSet, HashMap};
use std::ffi::OsString;
use std::fs::{File, Metadata};
use std::io;
use std::os::fd::{AsFd, OwnedFd};
use std::os::unix::ffi::OsStringExt;
use std::os::unix::fs::MetadataExt;
use std::path::{Component, Path, PathBuf};
use std::process::Command;
use std::time::Instant;

use nix::dir::Dir;
use nix::errno::Errno;
use nix::fcntl::{open, openat, renameat, AtFlags, OFlag};
use nix::sys::signal::{kill, Signal};
use nix::sys::stat::{mkdirat, Mode};
use nix::unistd::{linkat, unlinkat, Pid, UnlinkatFlags};
use tracing::debug;

use super::{BoundedDirectoryRead, DirectoryReadLimit, FilesystemObjectSnapshot};

pub fn open_bounded_root(root: &Path) -> io::Result<File> {
    if !root.is_absolute() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "bounded root must be an absolute path",
        ));
    }
    let descriptor = open(
        Path::new("/"),
        OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_CLOEXEC | OFlag::O_NOFOLLOW,
        Mode::empty(),
    )
    .map_err(errno_to_io)?;
    let mut directory = File::from(descriptor);
    for component in root.components() {
        let name = match component {
            Component::RootDir => continue,
            Component::Normal(name) => name,
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "bounded root must contain only normal absolute components",
                ));
            }
        };
        let descriptor = openat(
            &directory,
            name,
            OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_CLOEXEC | OFlag::O_NOFOLLOW,
            Mode::empty(),
        )
        .map_err(errno_to_io)?;
        directory = File::from(descriptor);
    }
    Ok(directory)
}

pub fn open_bounded_regular_file(root: &File, relative_path: &Path) -> io::Result<File> {
    validate_relative_path(relative_path)?;
    let components: Vec<_> = relative_path.components().collect();
    let mut directory = root.try_clone()?;
    for (index, component) in components.iter().enumerate() {
        let Component::Normal(name) = component else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "bounded path must contain only normal relative components",
            ));
        };
        let last = index + 1 == components.len();
        let flags = if last {
            OFlag::O_RDONLY | OFlag::O_CLOEXEC | OFlag::O_NOFOLLOW | OFlag::O_NONBLOCK
        } else {
            OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_CLOEXEC | OFlag::O_NOFOLLOW
        };
        let descriptor = openat(&directory, *name, flags, Mode::empty()).map_err(errno_to_io)?;
        let opened = File::from(descriptor);
        ensure_same_device(root, &opened)?;
        if last {
            if !opened.metadata()?.is_file() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "bounded document descriptor is not a regular file",
                ));
            }
            return Ok(opened);
        }
        directory = opened;
    }
    Err(io::Error::new(
        io::ErrorKind::InvalidInput,
        "bounded file path must not be empty",
    ))
}

pub fn open_bounded_subdirectory(root: &File, relative_path: &Path) -> io::Result<File> {
    validate_relative_path(relative_path)?;
    open_bounded_directory(root, relative_path).map(File::from)
}

pub fn ensure_bounded_directory(root: &File, relative_path: &Path) -> io::Result<File> {
    validate_relative_path(relative_path)?;
    let mut directory = root.try_clone()?;
    for component in relative_path.components() {
        let Component::Normal(name) = component else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "bounded path must contain only normal relative components",
            ));
        };
        match mkdirat(&directory, name, Mode::from_bits_truncate(0o700)) {
            Ok(()) => directory.sync_all()?,
            Err(Errno::EEXIST) => {}
            Err(error) => return Err(errno_to_io(error)),
        }
        let descriptor = openat(
            &directory,
            name,
            OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_CLOEXEC | OFlag::O_NOFOLLOW,
            Mode::empty(),
        )
        .map_err(errno_to_io)?;
        let opened = File::from(descriptor);
        ensure_same_device(root, &opened)?;
        let metadata = opened.metadata()?;
        if !metadata.is_dir() || metadata.mode() & 0o077 != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "bounded lifecycle component is not an owner-only directory",
            ));
        }
        directory = opened;
    }
    Ok(directory)
}

pub fn open_or_create_bounded_regular_file(root: &File, relative_path: &Path) -> io::Result<File> {
    open_bounded_writable_file(root, relative_path, false)
}

pub fn create_new_bounded_regular_file(root: &File, relative_path: &Path) -> io::Result<File> {
    open_bounded_writable_file(root, relative_path, true)
}

pub fn finalize_bounded_file_noreplace(
    root: &File,
    directory: &Path,
    partial_name: &Path,
    final_name: &Path,
) -> io::Result<()> {
    validate_single_name(partial_name)?;
    validate_single_name(final_name)?;
    let directory = File::from(open_bounded_directory(root, directory)?);
    linkat(
        &directory,
        partial_name,
        &directory,
        final_name,
        AtFlags::empty(),
    )
    .map_err(errno_to_io)?;
    unlinkat(&directory, partial_name, UnlinkatFlags::NoRemoveDir).map_err(errno_to_io)?;
    directory.sync_all()
}

pub fn replace_bounded_root_file(
    root: &File,
    temporary_name: &Path,
    final_name: &Path,
) -> io::Result<()> {
    validate_single_name(temporary_name)?;
    validate_single_name(final_name)?;
    renameat(root, temporary_name, root, final_name).map_err(errno_to_io)?;
    root.sync_all()
}

pub fn rename_bounded_root_file_unsynced(
    root: &File,
    source_name: &Path,
    target_name: &Path,
) -> io::Result<()> {
    validate_single_name(source_name)?;
    validate_single_name(target_name)?;
    renameat(root, source_name, root, target_name).map_err(errno_to_io)
}

pub fn link_bounded_root_file_noreplace_unsynced(
    root: &File,
    source_name: &Path,
    target_name: &Path,
) -> io::Result<()> {
    validate_single_name(source_name)?;
    validate_single_name(target_name)?;
    linkat(root, source_name, root, target_name, AtFlags::empty()).map_err(errno_to_io)
}

pub fn remove_bounded_root_file(root: &File, name: &Path) -> io::Result<()> {
    validate_single_name(name)?;
    unlinkat(root, name, UnlinkatFlags::NoRemoveDir).map_err(errno_to_io)?;
    root.sync_all()
}

pub fn remove_bounded_root_file_unsynced(root: &File, name: &Path) -> io::Result<()> {
    validate_single_name(name)?;
    unlinkat(root, name, UnlinkatFlags::NoRemoveDir).map_err(errno_to_io)
}

pub fn create_bounded_root_directory_unsynced(root: &File, name: &Path) -> io::Result<()> {
    validate_single_name(name)?;
    mkdirat(root, name, Mode::from_bits_truncate(0o700)).map_err(errno_to_io)
}

pub fn remove_bounded_root_directory_unsynced(root: &File, name: &Path) -> io::Result<()> {
    validate_single_name(name)?;
    unlinkat(root, name, UnlinkatFlags::RemoveDir).map_err(errno_to_io)
}

fn open_bounded_writable_file(
    root: &File,
    relative_path: &Path,
    create_new: bool,
) -> io::Result<File> {
    validate_relative_path(relative_path)?;
    let parent = relative_path
        .parent()
        .filter(|path| !path.as_os_str().is_empty());
    let directory = match parent {
        Some(parent) => File::from(open_bounded_directory(root, parent)?),
        None => root.try_clone()?,
    };
    let name = relative_path.file_name().ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidInput, "bounded file name is missing")
    })?;
    let mut flags = OFlag::O_RDWR | OFlag::O_CREAT | OFlag::O_CLOEXEC | OFlag::O_NOFOLLOW;
    if create_new {
        flags |= OFlag::O_EXCL;
    }
    let descriptor =
        openat(&directory, name, flags, Mode::from_bits_truncate(0o600)).map_err(errno_to_io)?;
    let file = File::from(descriptor);
    ensure_same_device(root, &file)?;
    if !file.metadata()?.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "bounded lifecycle descriptor is not a regular file",
        ));
    }
    Ok(file)
}

fn validate_single_name(path: &Path) -> io::Result<()> {
    if path.is_absolute()
        || path.components().count() != 1
        || !matches!(path.components().next(), Some(Component::Normal(_)))
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "bounded finalization names must be one normal component",
        ));
    }
    Ok(())
}

pub fn read_bounded_directory(
    root: &File,
    relative_path: &Path,
    max_entries: u64,
    scan_started: Instant,
    max_elapsed_ms: u64,
) -> io::Result<BoundedDirectoryRead> {
    let descriptor = open_bounded_directory(root, relative_path)?;
    let descriptor_file = File::from(descriptor);
    let snapshot = FilesystemObjectSnapshot::from_metadata(&descriptor_file.metadata()?);
    let descriptor = OwnedFd::from(descriptor_file);
    let mut directory = Dir::from_fd(descriptor).map_err(errno_to_io)?;
    let mut names = Vec::new();
    let mut entries_observed = 0_u64;
    let mut limit = None;
    let mut entries = directory.iter();
    loop {
        if scan_started.elapsed().as_millis() as u64 >= max_elapsed_ms {
            limit = Some(DirectoryReadLimit::ElapsedTime);
            break;
        }
        let Some(entry) = entries.next() else {
            break;
        };
        let entry = entry.map_err(errno_to_io)?;
        let name = entry.file_name().to_bytes();
        if name == b"." || name == b".." {
            continue;
        }
        if entries_observed >= max_entries {
            limit = Some(DirectoryReadLimit::EntryCount);
            break;
        }
        names.push(OsString::from_vec(name.to_vec()));
        entries_observed += 1;
    }
    drop(entries);
    let final_file = File::from(directory.as_fd().try_clone_to_owned()?);
    let final_snapshot = FilesystemObjectSnapshot::from_metadata(&final_file.metadata()?);
    let current_snapshot = bounded_directory_snapshot(root, relative_path).ok();
    let stable = snapshot == final_snapshot && current_snapshot.as_ref() == Some(&snapshot);
    if limit.is_some() || !stable {
        names.clear();
    }
    names.sort();
    Ok(BoundedDirectoryRead {
        names,
        snapshot,
        entries_observed,
        limit,
        stable,
    })
}

pub fn bounded_directory_snapshot(
    root: &File,
    relative_path: &Path,
) -> io::Result<FilesystemObjectSnapshot> {
    let descriptor = open_bounded_directory(root, relative_path)?;
    let directory = File::from(descriptor);
    Ok(FilesystemObjectSnapshot::from_metadata(
        &directory.metadata()?,
    ))
}

pub fn filesystem_file_identity(metadata: &Metadata) -> (u64, u64) {
    (metadata.dev(), metadata.ino())
}

pub fn filesystem_change_time(metadata: &Metadata) -> (i64, i64) {
    (metadata.ctime(), metadata.ctime_nsec())
}

pub fn known_mount_points() -> BTreeSet<PathBuf> {
    BTreeSet::new()
}

fn open_directory_components(root: &File, relative_path: &Path) -> io::Result<OwnedFd> {
    let components: Vec<_> = relative_path.components().collect();
    let mut directory = root.try_clone()?;
    for (index, component) in components.iter().enumerate() {
        let Component::Normal(name) = component else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "bounded path must contain only normal relative components",
            ));
        };
        let descriptor = openat(
            &directory,
            *name,
            OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_CLOEXEC | OFlag::O_NOFOLLOW,
            Mode::empty(),
        )
        .map_err(errno_to_io)?;
        let opened = File::from(descriptor);
        ensure_same_device(root, &opened)?;
        if index + 1 == components.len() {
            return Ok(OwnedFd::from(opened));
        }
        directory = opened;
    }
    Err(io::Error::new(
        io::ErrorKind::InvalidInput,
        "bounded directory path must not be empty",
    ))
}

fn open_bounded_directory(root: &File, relative_path: &Path) -> io::Result<OwnedFd> {
    if relative_path.as_os_str().is_empty() {
        return Ok(OwnedFd::from(root.try_clone()?));
    }
    validate_relative_path(relative_path)?;
    open_directory_components(root, relative_path)
}

fn validate_relative_path(path: &Path) -> io::Result<()> {
    if path.as_os_str().is_empty()
        || path.is_absolute()
        || !path
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "bounded path must contain only normal relative components",
        ));
    }
    Ok(())
}

fn errno_to_io(error: Errno) -> io::Error {
    io::Error::from_raw_os_error(error as i32)
}

fn ensure_same_device(root: &File, opened: &File) -> io::Result<()> {
    if root.metadata()?.dev() != opened.metadata()?.dev() {
        return Err(io::Error::from_raw_os_error(nix::libc::EXDEV));
    }
    Ok(())
}

/// Kill a process and all its descendants
/// Uses SIGTERM first, then SIGKILL after a short delay for stragglers
pub fn kill_process_tree(pid: u32) {
    // Build a map of parent -> children by parsing the process list once
    let children_map = build_children_map();

    // Collect all descendant PIDs (children, grandchildren, etc.)
    let mut pids_to_kill = vec![pid];
    collect_descendants_from_map(pid, &children_map, &mut pids_to_kill);

    debug!(
        pid,
        descendants = ?pids_to_kill,
        "Killing process tree"
    );

    // Kill in reverse order (children first, then parent) with SIGTERM
    for &p in pids_to_kill.iter().rev() {
        let _ = kill(Pid::from_raw(p as i32), Signal::SIGTERM);
    }

    // Brief pause to let processes handle SIGTERM gracefully
    std::thread::sleep(std::time::Duration::from_millis(100));

    // SIGKILL any survivors
    for &p in pids_to_kill.iter().rev() {
        if process_exists(p) {
            debug!(pid = p, "Process survived SIGTERM, sending SIGKILL");
            let _ = kill(Pid::from_raw(p as i32), Signal::SIGKILL);
        }
    }
}

/// Build a map of parent PID -> list of child PIDs by parsing `ps` output once
fn build_children_map() -> HashMap<u32, Vec<u32>> {
    let mut children_map: HashMap<u32, Vec<u32>> = HashMap::new();

    let Ok(output) = Command::new("ps").args(["-o", "pid=,ppid=", "-A"]).output() else {
        return children_map;
    };

    if !output.status.success() {
        return children_map;
    }

    for line in String::from_utf8_lossy(&output.stdout).lines() {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() >= 2 {
            if let (Ok(child_pid), Ok(ppid)) = (parts[0].parse::<u32>(), parts[1].parse::<u32>()) {
                children_map.entry(ppid).or_default().push(child_pid);
            }
        }
    }

    children_map
}

/// Recursively collect all descendant PIDs using the pre-built children map
fn collect_descendants_from_map(
    pid: u32,
    children_map: &HashMap<u32, Vec<u32>>,
    pids: &mut Vec<u32>,
) {
    if let Some(children) = children_map.get(&pid) {
        for &child_pid in children {
            pids.push(child_pid);
            collect_descendants_from_map(child_pid, children_map, pids);
        }
    }
}

/// Check if a process still exists
fn process_exists(pid: u32) -> bool {
    // Use kill with signal 0 to check if process exists
    // EPERM means the process exists but we lack permission (still exists)
    // ESRCH means the process doesn't exist
    match kill(Pid::from_raw(pid as i32), None) {
        Ok(()) => true,
        Err(Errno::EPERM) => true,
        Err(_) => false,
    }
}

/// Return a stable identity for a live process, including its launch time.
///
/// The launch time prevents a persisted PID from authorizing termination after
/// macOS has reused that PID for an unrelated process.
pub fn process_identity(pid: u32) -> Option<String> {
    let output = Command::new("ps")
        .args(["-o", "stat=,lstart=", "-p", &pid.to_string()])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let fields = String::from_utf8_lossy(&output.stdout)
        .split_whitespace()
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    if fields.len() < 6 || fields[0].contains('Z') {
        return None;
    }
    let started_at = fields[1..].join(" ");
    Some(format!("macos:{pid}:{started_at}"))
}

/// Get the foreground process group leader for a shell PID
pub fn get_foreground_pid(shell_pid: u32) -> Option<u32> {
    // Use ps to get the foreground process group
    // ps -o tpgid= -p <pid> gives us the terminal foreground process group ID
    let output = Command::new("ps")
        .args(["-o", "tpgid=", "-p", &shell_pid.to_string()])
        .output()
        .ok()?;

    if !output.status.success() {
        return Some(shell_pid);
    }

    let tpgid: i32 = String::from_utf8_lossy(&output.stdout)
        .trim()
        .parse()
        .ok()?;

    if tpgid <= 0 {
        return Some(shell_pid);
    }

    // Find a process in the foreground group
    find_process_in_group(tpgid as u32).or(Some(shell_pid))
}

/// Find a process belonging to the given process group
fn find_process_in_group(pgrp: u32) -> Option<u32> {
    // Use ps to find processes in this group
    // ps -o pid=,pgid= -A lists all processes with their PIDs and PGIDs
    let output = Command::new("ps")
        .args(["-o", "pid=,pgid=", "-A"])
        .output()
        .ok()?;

    if !output.status.success() {
        return None;
    }

    for line in String::from_utf8_lossy(&output.stdout).lines() {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() >= 2 {
            if let (Ok(pid), Ok(proc_pgrp)) = (parts[0].parse::<u32>(), parts[1].parse::<u32>()) {
                if proc_pgrp == pgrp {
                    return Some(pid);
                }
            }
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_collect_descendants_from_map_empty() {
        let children_map = HashMap::new();
        let mut pids = vec![100];
        collect_descendants_from_map(100, &children_map, &mut pids);
        assert_eq!(pids, vec![100]);
    }

    #[test]
    fn test_collect_descendants_from_map_single_child() {
        let mut children_map = HashMap::new();
        children_map.insert(100, vec![101]);

        let mut pids = vec![100];
        collect_descendants_from_map(100, &children_map, &mut pids);
        assert_eq!(pids, vec![100, 101]);
    }

    #[test]
    fn test_collect_descendants_from_map_multiple_children() {
        let mut children_map = HashMap::new();
        children_map.insert(100, vec![101, 102, 103]);

        let mut pids = vec![100];
        collect_descendants_from_map(100, &children_map, &mut pids);
        assert_eq!(pids, vec![100, 101, 102, 103]);
    }

    #[test]
    fn test_collect_descendants_from_map_nested() {
        // Tree: 100 -> 101 -> 102 -> 103
        let mut children_map = HashMap::new();
        children_map.insert(100, vec![101]);
        children_map.insert(101, vec![102]);
        children_map.insert(102, vec![103]);

        let mut pids = vec![100];
        collect_descendants_from_map(100, &children_map, &mut pids);
        assert_eq!(pids, vec![100, 101, 102, 103]);
    }

    #[test]
    fn test_collect_descendants_from_map_branching() {
        // Tree: 100 -> [101, 102], 101 -> [103, 104], 102 -> [105]
        let mut children_map = HashMap::new();
        children_map.insert(100, vec![101, 102]);
        children_map.insert(101, vec![103, 104]);
        children_map.insert(102, vec![105]);

        let mut pids = vec![100];
        collect_descendants_from_map(100, &children_map, &mut pids);

        // Should contain all PIDs
        assert!(pids.contains(&100));
        assert!(pids.contains(&101));
        assert!(pids.contains(&102));
        assert!(pids.contains(&103));
        assert!(pids.contains(&104));
        assert!(pids.contains(&105));
        assert_eq!(pids.len(), 6);
    }

    #[test]
    fn test_collect_descendants_unrelated_processes() {
        // Map has processes, but none are descendants of 100
        let mut children_map = HashMap::new();
        children_map.insert(200, vec![201, 202]);
        children_map.insert(300, vec![301]);

        let mut pids = vec![100];
        collect_descendants_from_map(100, &children_map, &mut pids);
        assert_eq!(pids, vec![100]);
    }

    #[test]
    fn current_process_has_stable_identity() {
        let identity = process_identity(std::process::id()).expect("current process identity");
        assert!(identity.starts_with(&format!("macos:{}:", std::process::id())));
        assert_eq!(process_identity(std::process::id()), Some(identity));
    }
}
