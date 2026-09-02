//! Linux-specific process utilities

use std::collections::BTreeSet;
use std::ffi::OsString;
use std::fs::{self, File, Metadata};
use std::io;
use std::os::fd::{AsFd, OwnedFd};
use std::os::unix::ffi::OsStringExt;
use std::os::unix::fs::MetadataExt;
use std::path::{Component, Path, PathBuf};
use std::time::Instant;

use nix::dir::Dir;
use nix::errno::Errno;
use nix::fcntl::{open, openat, openat2, renameat, AtFlags, OFlag, OpenHow, ResolveFlag};
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
    let how = OpenHow::new()
        .flags(OFlag::O_RDONLY | OFlag::O_CLOEXEC | OFlag::O_NOFOLLOW | OFlag::O_NONBLOCK)
        .resolve(
            ResolveFlag::RESOLVE_BENEATH
                | ResolveFlag::RESOLVE_NO_SYMLINKS
                | ResolveFlag::RESOLVE_NO_XDEV,
        );
    match openat2(root, relative_path, how) {
        Ok(descriptor) => regular_file_from_descriptor(root, descriptor),
        Err(Errno::ENOSYS) => Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "openat2 is required for no-cross-device bounded reads",
        )),
        Err(error) => Err(errno_to_io(error)),
    }
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
    regular_file_from_descriptor(root, descriptor)
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
    fs::read_to_string("/proc/self/mountinfo")
        .ok()
        .into_iter()
        .flat_map(|mountinfo| {
            mountinfo
                .lines()
                .filter_map(|line| line.split_whitespace().nth(4).map(decode_mountinfo_path))
                .collect::<Vec<_>>()
        })
        .collect()
}

fn open_bounded_directory(root: &File, relative_path: &Path) -> io::Result<OwnedFd> {
    if relative_path.as_os_str().is_empty() {
        return Ok(OwnedFd::from(root.try_clone()?));
    }
    validate_relative_path(relative_path)?;
    let how = OpenHow::new()
        .flags(OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_CLOEXEC | OFlag::O_NOFOLLOW)
        .resolve(
            ResolveFlag::RESOLVE_BENEATH
                | ResolveFlag::RESOLVE_NO_SYMLINKS
                | ResolveFlag::RESOLVE_NO_XDEV,
        );
    match openat2(root, relative_path, how) {
        Ok(descriptor) => Ok(descriptor),
        Err(Errno::ENOSYS) => Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "openat2 is required for no-cross-device bounded reads",
        )),
        Err(error) => Err(errno_to_io(error)),
    }
}

fn regular_file_from_descriptor(root: &File, descriptor: std::os::fd::OwnedFd) -> io::Result<File> {
    let file = File::from(descriptor);
    ensure_same_device(root, &file)?;
    if !file.metadata()?.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "bounded document descriptor is not a regular file",
        ));
    }
    Ok(file)
}

fn ensure_same_device(root: &File, opened: &File) -> io::Result<()> {
    if root.metadata()?.dev() != opened.metadata()?.dev() {
        return Err(io::Error::from_raw_os_error(nix::libc::EXDEV));
    }
    Ok(())
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

fn decode_mountinfo_path(value: &str) -> PathBuf {
    let source = value.as_bytes();
    let mut decoded = Vec::with_capacity(source.len());
    let mut index = 0;
    while index < source.len() {
        if source[index] == b'\\' && index + 3 < source.len() {
            let octal = &source[index + 1..index + 4];
            if octal.iter().all(|byte| matches!(byte, b'0'..=b'7')) {
                decoded.push((octal[0] - b'0') * 64 + (octal[1] - b'0') * 8 + octal[2] - b'0');
                index += 4;
                continue;
            }
        }
        decoded.push(source[index]);
        index += 1;
    }
    PathBuf::from(OsString::from_vec(decoded))
}

fn errno_to_io(error: Errno) -> io::Error {
    io::Error::from_raw_os_error(error as i32)
}

/// Kill a process and all its descendants
/// Uses SIGTERM first, then SIGKILL after a short delay for stragglers
pub fn kill_process_tree(pid: u32) {
    // Collect all descendant PIDs first (children, grandchildren, etc.)
    let mut pids_to_kill = vec![pid];
    collect_descendants(pid, &mut pids_to_kill);

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

/// Recursively collect all descendant PIDs of a process
fn collect_descendants(pid: u32, pids: &mut Vec<u32>) {
    let proc_dir = Path::new("/proc");
    if !proc_dir.exists() {
        return;
    }

    let Ok(entries) = fs::read_dir(proc_dir) else {
        return;
    };

    for entry in entries.flatten() {
        let name = entry.file_name();
        let name_str = name.to_string_lossy();

        // Skip non-numeric entries
        let Ok(child_pid) = name_str.parse::<u32>() else {
            continue;
        };

        // Read the process's parent PID
        let stat_path = entry.path().join("stat");
        let Ok(content) = fs::read_to_string(&stat_path) else {
            continue;
        };

        if let Some(ppid) = parse_stat_field(&content, 3) {
            if ppid as u32 == pid {
                pids.push(child_pid);
                // Recurse to find grandchildren
                collect_descendants(child_pid, pids);
            }
        }
    }
}

/// Check if a process still exists
fn process_exists(pid: u32) -> bool {
    Path::new(&format!("/proc/{}", pid)).exists()
}

/// Return a stable identity for a live process, including its kernel start tick.
///
/// A persisted PID alone is unsafe for later termination because the kernel can
/// reuse it. The start tick lets callers fail closed when a PID now belongs to
/// a different process.
pub fn process_identity(pid: u32) -> Option<String> {
    let stat = fs::read_to_string(Path::new("/proc").join(pid.to_string()).join("stat")).ok()?;
    let close_paren = stat.rfind(')')?;
    let state = stat[close_paren + 1..].split_whitespace().next()?;
    if state == "Z" {
        return None;
    }
    let start_tick = parse_stat_field(&stat, 21)?;
    Some(format!("linux:{pid}:{start_tick}"))
}

/// Get the foreground process group leader for a shell PID
/// Walks the process tree to find the actual foreground process
pub fn get_foreground_pid(shell_pid: u32) -> Option<u32> {
    // Read the shell's stat to get its controlling terminal
    let stat_path = format!("/proc/{}/stat", shell_pid);
    let stat_content = fs::read_to_string(&stat_path).ok()?;

    // Parse stat: pid (comm) state ppid pgrp session tty_nr tpgid ...
    // tpgid (field 8, 0-indexed 7) is the foreground process group ID
    let tpgid = parse_stat_field(&stat_content, 7)?;

    if tpgid <= 0 {
        return Some(shell_pid);
    }

    // Find a process in the foreground process group
    // The tpgid is a process group ID, we need to find a process in that group
    find_process_in_group(tpgid as u32).or(Some(shell_pid))
}

/// Find a process that belongs to the given process group
fn find_process_in_group(pgrp: u32) -> Option<u32> {
    let proc_dir = Path::new("/proc");
    if !proc_dir.exists() {
        return None;
    }

    for entry in fs::read_dir(proc_dir).ok()? {
        let entry = entry.ok()?;
        let name = entry.file_name();
        let name_str = name.to_string_lossy();

        // Skip non-numeric entries
        if !name_str.chars().all(|c| c.is_ascii_digit()) {
            continue;
        }

        let pid: u32 = name_str.parse().ok()?;
        let stat_path = entry.path().join("stat");

        if let Ok(content) = fs::read_to_string(&stat_path) {
            // Field 5 (0-indexed 4) is the process group ID
            if let Some(proc_pgrp) = parse_stat_field(&content, 4) {
                if proc_pgrp as u32 == pgrp {
                    return Some(pid);
                }
            }
        }
    }

    None
}

/// Parse a specific field from /proc/[pid]/stat
/// Fields are space-separated but comm (field 2) can contain spaces and is in parens
fn parse_stat_field(content: &str, field_idx: usize) -> Option<i64> {
    // Find the closing paren of comm field, then parse from there
    let close_paren = content.rfind(')')?;
    let after_comm = &content[close_paren + 2..]; // Skip ") "

    // Fields after comm start at index 2 (state is index 2)
    // So field_idx 4 means we want the 3rd field after comm (index 2 in after_comm split)
    let adjusted_idx = field_idx.checked_sub(2)?;
    let fields: Vec<&str> = after_comm.split_whitespace().collect();
    fields.get(adjusted_idx)?.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;
    use std::os::unix::fs::symlink;

    #[test]
    fn test_parse_stat_field() {
        // Example stat line (simplified)
        let stat = "1234 (bash) S 1233 1234 1234 34816 1234 4194304 1234 0 0 0";
        // Fields: pid(0) comm(1) state(2) ppid(3) pgrp(4) session(5) tty(6) tpgid(7) ...

        assert_eq!(parse_stat_field(stat, 3), Some(1233)); // ppid
        assert_eq!(parse_stat_field(stat, 4), Some(1234)); // pgrp
        assert_eq!(parse_stat_field(stat, 7), Some(1234)); // tpgid
    }

    #[test]
    fn current_process_has_stable_identity() {
        let identity = process_identity(std::process::id()).expect("current process identity");
        assert!(identity.starts_with(&format!("linux:{}:", std::process::id())));
        assert_eq!(process_identity(std::process::id()), Some(identity));
    }

    #[test]
    fn bounded_open_rejects_final_and_intermediate_symlinks() {
        let temp = tempfile::tempdir().expect("tempdir");
        let root = temp.path().join("root");
        let outside = temp.path().join("outside");
        fs::create_dir_all(root.join("docs")).expect("root docs");
        fs::create_dir_all(&outside).expect("outside");
        fs::write(root.join("docs/regular.md"), "regular").expect("regular document");
        fs::write(outside.join("secret.md"), "secret").expect("outside document");
        symlink(outside.join("secret.md"), root.join("docs/final.md")).expect("final symlink");
        symlink(&outside, root.join("alias")).expect("directory symlink");

        let root_handle = open_bounded_root(&root).expect("bounded root");
        let mut regular = open_bounded_regular_file(&root_handle, Path::new("docs/regular.md"))
            .expect("regular bounded file");
        let mut contents = String::new();
        regular.read_to_string(&mut contents).expect("read regular");
        assert_eq!(contents, "regular");
        assert!(open_bounded_regular_file(&root_handle, Path::new("docs/final.md")).is_err());
        assert!(open_bounded_regular_file(&root_handle, Path::new("alias/secret.md")).is_err());
        assert!(read_bounded_directory(
            &root_handle,
            Path::new("alias"),
            100,
            Instant::now(),
            1_000,
        )
        .is_err());
    }

    #[test]
    fn bounded_open_and_mount_table_reject_cross_device_descent() {
        if Path::new("/proc/version").exists() {
            let root_handle = open_bounded_root(Path::new("/")).expect("filesystem root");
            assert!(open_bounded_regular_file(&root_handle, Path::new("proc/version")).is_err());
            assert!(known_mount_points().contains(Path::new("/proc")));
        }
        assert_eq!(
            decode_mountinfo_path("/tmp/with\\040space"),
            PathBuf::from("/tmp/with space")
        );
    }

    #[test]
    fn bounded_directory_read_limits_entries_and_detects_replacement() {
        let temp = tempfile::tempdir().expect("tempdir");
        let root = temp.path().join("root");
        fs::create_dir_all(root.join("docs")).expect("docs");
        for name in ["a.md", "b.md", "c.md"] {
            fs::write(root.join("docs").join(name), name).expect("document");
        }
        let root_handle = open_bounded_root(&root).expect("bounded root");
        let limited =
            read_bounded_directory(&root_handle, Path::new("docs"), 1, Instant::now(), 1_000)
                .expect("bounded enumeration");
        assert_eq!(limited.limit, Some(DirectoryReadLimit::EntryCount));
        assert_eq!(limited.entries_observed, 1);
        assert!(limited.names.is_empty());

        let complete =
            read_bounded_directory(&root_handle, Path::new("docs"), 100, Instant::now(), 1_000)
                .expect("complete enumeration");
        assert!(complete.stable);
        fs::rename(root.join("docs"), root.join("docs-old")).expect("replace old docs");
        fs::create_dir(root.join("docs")).expect("replacement docs");
        let current = bounded_directory_snapshot(&root_handle, Path::new("docs"))
            .expect("replacement snapshot");
        assert_ne!(current, complete.snapshot);
    }
}
