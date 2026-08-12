//! Background status polling for TUI performance
//!
//! This module provides non-blocking status updates for sessions by running
//! tmux subprocess calls in a background thread.

use std::collections::HashMap;
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use crate::session::{Instance, Status};

/// Result of a status check for a single session
#[derive(Debug)]
pub struct StatusUpdate {
    pub id: String,
    pub status: Status,
    pub last_error: Option<String>,
}

/// Background thread that polls session status without blocking the UI
pub struct StatusPoller {
    request_tx: mpsc::Sender<Vec<Instance>>,
    result_rx: mpsc::Receiver<Vec<StatusUpdate>>,
    in_flight: bool,
    schedule: StatusRefreshSchedule,
    _handle: thread::JoinHandle<()>,
}

const SELECTED_REFRESH_INTERVAL: Duration = Duration::from_secs(1);
const BACKGROUND_REFRESH_INTERVAL: Duration = Duration::from_secs(3);

#[derive(Default)]
struct StatusRefreshSchedule {
    last_requested: HashMap<String, Instant>,
}

impl StatusRefreshSchedule {
    fn due_instances(
        &self,
        instances: &[Instance],
        selected_session: Option<&str>,
        now: Instant,
    ) -> Vec<Instance> {
        instances
            .iter()
            .filter(|instance| {
                let interval = if selected_session == Some(instance.id.as_str()) {
                    SELECTED_REFRESH_INTERVAL
                } else {
                    BACKGROUND_REFRESH_INTERVAL
                };
                self.last_requested
                    .get(&instance.id)
                    .is_none_or(|last| now.duration_since(*last) >= interval)
            })
            .cloned()
            .collect()
    }

    fn mark_requested<'a>(&mut self, ids: impl IntoIterator<Item = &'a str>, now: Instant) {
        for id in ids {
            self.last_requested.insert(id.to_string(), now);
        }
    }
}

impl StatusPoller {
    pub fn new() -> Self {
        let (request_tx, request_rx) = mpsc::channel::<Vec<Instance>>();
        let (result_tx, result_rx) = mpsc::channel::<Vec<StatusUpdate>>();

        let handle = thread::spawn(move || {
            Self::polling_loop(request_rx, result_tx);
        });

        Self {
            request_tx,
            result_rx,
            in_flight: false,
            schedule: StatusRefreshSchedule::default(),
            _handle: handle,
        }
    }

    fn polling_loop(
        request_rx: mpsc::Receiver<Vec<Instance>>,
        result_tx: mpsc::Sender<Vec<StatusUpdate>>,
    ) {
        while let Ok(instances) = request_rx.recv() {
            crate::tmux::refresh_session_cache();

            let updates: Vec<StatusUpdate> = instances
                .into_iter()
                .map(|mut inst| {
                    inst.update_status();

                    StatusUpdate {
                        id: inst.id,
                        status: inst.status,
                        last_error: inst.last_error,
                    }
                })
                .collect();

            if result_tx.send(updates).is_err() {
                // Receiver dropped, exit the loop
                break;
            }
        }
    }

    /// Request a status refresh for all given instances (non-blocking).
    pub fn request_refresh(
        &mut self,
        instances: &[Instance],
        selected_session: Option<&str>,
    ) -> bool {
        if self.in_flight {
            return false;
        }

        let now = Instant::now();
        let due = self
            .schedule
            .due_instances(instances, selected_session, now);
        if due.is_empty() {
            return false;
        }

        let requested_ids: Vec<String> = due.iter().map(|instance| instance.id.clone()).collect();
        if self.request_tx.send(due).is_err() {
            return false;
        }

        self.schedule
            .mark_requested(requested_ids.iter().map(String::as_str), now);
        self.in_flight = true;
        true
    }

    /// Try to receive status updates without blocking.
    /// Returns None if no updates are available yet.
    pub fn try_recv_updates(&mut self) -> Option<Vec<StatusUpdate>> {
        let updates = self.result_rx.try_recv().ok()?;
        self.in_flight = false;
        Some(updates)
    }
}

impl Default for StatusPoller {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selected_session_refreshes_more_often_than_background_sessions() {
        let selected = Instance::new("selected", "/tmp/selected");
        let background = Instance::new("background", "/tmp/background");
        let instances = vec![selected.clone(), background.clone()];
        let mut schedule = StatusRefreshSchedule::default();
        let started = Instant::now();

        let initial = schedule.due_instances(&instances, Some(&selected.id), started);
        assert_eq!(initial.len(), 2);
        schedule.mark_requested(initial.iter().map(|instance| instance.id.as_str()), started);

        assert!(schedule
            .due_instances(
                &instances,
                Some(&selected.id),
                started + Duration::from_millis(999)
            )
            .is_empty());

        let selected_due = schedule.due_instances(
            &instances,
            Some(&selected.id),
            started + SELECTED_REFRESH_INTERVAL,
        );
        assert_eq!(selected_due.len(), 1);
        assert_eq!(selected_due[0].id, selected.id);

        let all_due = schedule.due_instances(
            &instances,
            Some(&selected.id),
            started + BACKGROUND_REFRESH_INTERVAL,
        );
        assert_eq!(all_due.len(), 2);
    }
}
