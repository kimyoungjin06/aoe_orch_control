//! Linux-specific process state detection using /proc filesystem

use super::ProcessInputState;
use std::fs;
use std::path::Path;

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

/// Check if a process is waiting for input on stdin
pub fn is_waiting_for_input(pid: u32) -> ProcessInputState {
    // Strategy:
    // 1. Check process state from /proc/[pid]/stat (S = sleeping)
    // 2. Check wchan (wait channel) for tty/terminal related waits
    // 3. Check if stdin (fd 0) is a tty and process is in foreground

    let stat_path = format!("/proc/{}/stat", pid);
    let stat_content = match fs::read_to_string(&stat_path) {
        Ok(c) => c,
        Err(_) => return ProcessInputState::Unknown,
    };

    // Parse process state (field 3, 0-indexed 2)
    let state = parse_process_state(&stat_content);

    match state {
        Some('R') => return ProcessInputState::Running, // Running
        Some('S') => {} // Sleeping - need to check what it's waiting on
        Some('D') => return ProcessInputState::SleepingOther, // Uninterruptible sleep (disk I/O)
        Some('T') | Some('t') => return ProcessInputState::SleepingOther, // Stopped/traced
        Some('Z') => return ProcessInputState::SleepingOther, // Zombie
        _ => return ProcessInputState::Unknown,
    }

    // Process is sleeping (S state) - check what it's waiting on
    // Read wchan (wait channel) to see if it's a tty-related wait
    let wchan_path = format!("/proc/{}/wchan", pid);
    if let Ok(wchan) = fs::read_to_string(&wchan_path) {
        let wchan = wchan.trim();

        // Common wait channels indicating stdin read:
        // - "n_tty_read" - reading from tty
        // - "wait_woken" - generic wait (could be tty)
        // - "do_select" / "do_poll" - select/poll on fd (often stdin)
        // - "unix_stream_read_generic" - reading from unix socket (not stdin)
        // - "pipe_read" - reading from pipe (not direct stdin)
        // - "poll_schedule_timeout" - polling with timeout

        let tty_read_indicators = ["n_tty_read", "tty_read", "pty_read"];

        for indicator in tty_read_indicators {
            if wchan.contains(indicator) {
                return ProcessInputState::WaitingForInput;
            }
        }

        // Check for generic poll/select - TUI apps use these for both user input AND
        // network I/O, so we can't reliably distinguish. Return Unknown to let
        // pattern matching decide based on terminal content.
        let poll_indicators = ["do_select", "do_poll", "poll_schedule", "ep_poll"];
        for indicator in poll_indicators {
            if wchan.contains(indicator) {
                return ProcessInputState::Unknown;
            }
        }

        // Network or other I/O waits
        let network_indicators = ["sk_wait", "inet_", "tcp_", "unix_stream", "pipe_read"];
        for indicator in network_indicators {
            if wchan.contains(indicator) {
                return ProcessInputState::SleepingOther;
            }
        }
    }

    // For any other sleeping state, return Unknown to let pattern matching decide.
    // We can't reliably distinguish between TUI apps waiting for user input vs
    // waiting for background operations (network, timers, etc.)
    ProcessInputState::Unknown
}

/// Parse the process state character from /proc/[pid]/stat
fn parse_process_state(stat_content: &str) -> Option<char> {
    let close_paren = stat_content.rfind(')')?;
    let after_comm = &stat_content[close_paren + 2..];
    after_comm.chars().next()
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn test_parse_process_state() {
        assert_eq!(parse_process_state("1 (test) S 0 1 1 0 1"), Some('S'));
        assert_eq!(parse_process_state("1 (test) R 0 1 1 0 1"), Some('R'));
        assert_eq!(
            parse_process_state("1 (test with spaces) D 0 1 1 0 1"),
            Some('D')
        );
    }
}
