//! Process state detection for determining if a process is waiting for input
//!
//! This module provides cross-platform detection of whether a process is blocked
//! waiting for user input on stdin. This is used to determine if a CLI tool
//! (like Claude Code) is waiting for the user to type something.

use std::process::Command;

#[cfg(target_os = "linux")]
mod linux;

#[cfg(target_os = "macos")]
mod macos;

/// Result of process state detection
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessInputState {
    /// Process is blocked waiting for input on stdin
    WaitingForInput,
    /// Process is actively running (not waiting for input)
    Running,
    /// Process is sleeping but not on stdin (e.g., network I/O)
    SleepingOther,
    /// Could not determine process state
    Unknown,
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

/// Check if a process is waiting for input on stdin
pub fn is_waiting_for_input(pid: u32) -> ProcessInputState {
    #[cfg(target_os = "linux")]
    {
        linux::is_waiting_for_input(pid)
    }

    #[cfg(target_os = "macos")]
    {
        macos::is_waiting_for_input(pid)
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = pid;
        ProcessInputState::Unknown
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_process_input_state_variants() {
        assert_eq!(
            ProcessInputState::WaitingForInput,
            ProcessInputState::WaitingForInput
        );
        assert_ne!(
            ProcessInputState::WaitingForInput,
            ProcessInputState::Running
        );
    }
}
