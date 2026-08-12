//! Regression tests for how loudly the Telegram operator asks for attention.
//!
//! On the night of 2026-08-06 one misdetected session produced 27 cards, 19 of
//! them between midnight and 09:33, while the only genuinely open decision had
//! been announced once in June and never repeated. These tests pin the policy
//! that inverts that: quiet overnight, a fading reminder, and silence once a
//! remote keystroke has been shown not to work.

use serde_json::Value;
use std::path::{Path, PathBuf};
use std::process::Command;
use tempfile::tempdir;

fn python_available() -> bool {
    Command::new("python3")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

fn manifest(sub: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join(sub)
}

fn replay(scenario: &str, workdir: &Path) -> Value {
    let output = Command::new("python3")
        .arg(manifest("tests/support/notify_policy_replay.py"))
        .arg(manifest("scripts"))
        .arg(workdir)
        .arg(scenario)
        .output()
        .expect("failed to run replay driver");
    assert!(
        output.status.success(),
        "replay failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("replay returned non-JSON")
}

#[test]
fn overnight_replay_stays_quiet_and_fades() {
    if !python_available() {
        eprintln!("Skipping test: python3 not available");
        return;
    }
    let dir = tempdir().expect("tempdir");

    let legacy = replay("legacy", dir.path());
    let current = replay("default", dir.path());

    // Establishes that the replay reproduces the original blowup, so a pass
    // below means the policy changed rather than the harness going quiet.
    assert!(
        legacy["cards"].as_u64().unwrap() >= 25,
        "legacy policy should reproduce the card storm, got {legacy}"
    );
    assert!(
        legacy["overnight"].as_u64().unwrap() >= 8,
        "legacy policy should fire through the night, got {legacy}"
    );

    assert_eq!(
        current["overnight"].as_u64().unwrap(),
        0,
        "no card may be pushed inside quiet hours: {current}"
    );
    assert!(
        current["cards"].as_u64().unwrap() <= 4,
        "the card cap must hold one waiting prompt to four cards: {current}"
    );
}

#[test]
fn a_dead_prompt_stops_asking_after_one_card() {
    if !python_available() {
        eprintln!("Skipping test: python3 not available");
        return;
    }
    let dir = tempdir().expect("tempdir");

    let tapped = replay("tapped", dir.path());

    // The operator taps once, the keystroke provably does nothing, and the
    // harness stops inviting a retry that cannot work.
    assert_eq!(
        tapped["cards"].as_u64().unwrap(),
        1,
        "a prompt that swallowed the keystroke must not be re-carded: {tapped}"
    );
}

#[test]
fn a_changed_prompt_resets_the_per_prompt_card_cap() {
    if !python_available() {
        eprintln!("Skipping test: python3 not available");
        return;
    }
    let dir = tempdir().expect("tempdir");

    let changed = replay("changed", dir.path());

    assert!(
        changed["cards"].as_u64().unwrap() >= 1,
        "a new prompt must notify even when the previous prompt hit its cap: {changed}"
    );
}
