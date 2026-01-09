//! macOS-specific process state detection using ps and libproc

use super::ProcessInputState;
use std::process::Command;

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

/// Check if a process is waiting for input on stdin
pub fn is_waiting_for_input(pid: u32) -> ProcessInputState {
    // On macOS, we use ps to get process state information
    // ps -o stat=,wchan= -p <pid>
    // stat field meanings:
    //   I - idle (sleeping for longer than 20 seconds)
    //   R - running
    //   S - sleeping for less than 20 seconds
    //   T - stopped
    //   U - waiting on I/O
    //   Z - zombie
    // Additional characters:
    //   + - foreground process group
    //   s - session leader
    //   < - raised priority
    //   N - lowered priority

    let output = match Command::new("ps")
        .args(["-o", "stat=,wchan=", "-p", &pid.to_string()])
        .output()
    {
        Ok(o) => o,
        Err(_) => return ProcessInputState::Unknown,
    };

    if !output.status.success() {
        return ProcessInputState::Unknown;
    }

    let output_str = String::from_utf8_lossy(&output.stdout);
    let parts: Vec<&str> = output_str.trim().split_whitespace().collect();

    if parts.is_empty() {
        return ProcessInputState::Unknown;
    }

    let stat = parts[0];
    let wchan = parts.get(1).copied().unwrap_or("-");

    // Check process state
    let state_char = stat.chars().next().unwrap_or('?');

    match state_char {
        'R' => return ProcessInputState::Running,
        'T' => return ProcessInputState::SleepingOther, // Stopped
        'Z' => return ProcessInputState::SleepingOther, // Zombie
        'U' => return ProcessInputState::SleepingOther, // Uninterruptible I/O wait
        'I' | 'S' => {}                                 // Sleeping - need to check wait channel
        _ => return ProcessInputState::Unknown,
    }

    // Process is sleeping - check wait channel
    // Common macOS wait channels for tty input:
    // - "ttyin" - waiting for tty input
    // - "pause" - paused (could be waiting for signal)
    // - "select" - select() call (could be stdin)
    // - "kevent" - kevent() call (event loop)

    let tty_wait_channels = ["ttyin", "ttyout", "ttyraw"];
    for channel in tty_wait_channels {
        if wchan.contains(channel) {
            return ProcessInputState::WaitingForInput;
        }
    }

    // Check for select/poll that might be stdin
    // Note: TUI apps (like OpenCode, Claude) use event loops (kevent/select) for both
    // user input AND network I/O, so we can't reliably distinguish between waiting
    // for user input vs waiting for API response. Return Unknown to let pattern
    // matching decide based on terminal content.
    let poll_channels = ["select", "poll", "kevent"];
    for channel in poll_channels {
        if wchan.contains(channel) {
            return ProcessInputState::Unknown;
        }
    }

    // Network-related wait channels
    let network_channels = ["netio", "sbwait", "sowait", "sockread", "semwait"];
    for channel in network_channels {
        if wchan.contains(channel) {
            return ProcessInputState::SleepingOther;
        }
    }

    // For any other sleeping state, return Unknown to let pattern matching decide.
    // We can't reliably distinguish between TUI apps waiting for user input vs
    // waiting for background operations (network, timers, etc.)
    ProcessInputState::Unknown
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_current_process_state() {
        let pid = std::process::id();
        let state = is_waiting_for_input(pid);
        // During test execution, process could be Running, SleepingOther, or Unknown
        // (Unknown is returned for ambiguous poll/kevent states)
        assert!(matches!(
            state,
            ProcessInputState::Running
                | ProcessInputState::SleepingOther
                | ProcessInputState::Unknown
        ));
    }
}
