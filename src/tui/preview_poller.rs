//! Background preview capture for the home TUI.

use std::sync::mpsc;
use std::thread;

use crate::session::Instance;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PreviewKind {
    Agent,
    Terminal,
}

pub struct PreviewRequest {
    pub instance: Instance,
    pub kind: PreviewKind,
    pub width: u16,
    pub height: u16,
}

#[derive(Debug)]
pub struct PreviewUpdate {
    pub session_id: String,
    pub kind: PreviewKind,
    pub content: String,
    pub dimensions: (u16, u16),
    pub terminal_running: bool,
}

pub struct PreviewPoller {
    request_tx: mpsc::Sender<PreviewRequest>,
    result_rx: mpsc::Receiver<PreviewUpdate>,
    _handle: thread::JoinHandle<()>,
}

impl PreviewPoller {
    pub fn new() -> Self {
        let (request_tx, request_rx) = mpsc::channel::<PreviewRequest>();
        let (result_tx, result_rx) = mpsc::channel::<PreviewUpdate>();

        let handle = thread::spawn(move || {
            while let Ok(request) = request_rx.recv() {
                let update = capture_preview(request);
                if result_tx.send(update).is_err() {
                    break;
                }
            }
        });

        Self {
            request_tx,
            result_rx,
            _handle: handle,
        }
    }

    pub fn request_refresh(&self, request: PreviewRequest) -> bool {
        self.request_tx.send(request).is_ok()
    }

    pub fn try_recv_update(&self) -> Option<PreviewUpdate> {
        self.result_rx.try_recv().ok()
    }
}

impl Default for PreviewPoller {
    fn default() -> Self {
        Self::new()
    }
}

fn capture_preview(request: PreviewRequest) -> PreviewUpdate {
    let session_id = request.instance.id.clone();
    let dimensions = (request.width, request.height);

    let (content, terminal_running) = match request.kind {
        PreviewKind::Agent => (
            request
                .instance
                .capture_output_with_size(request.height as usize, request.width, request.height)
                .unwrap_or_default(),
            false,
        ),
        PreviewKind::Terminal => match request.instance.terminal_tmux_session() {
            Ok(session) if session.exists() => (
                session
                    .capture_pane(request.height as usize)
                    .unwrap_or_default(),
                true,
            ),
            _ => (String::new(), false),
        },
    };

    PreviewUpdate {
        session_id,
        kind: request.kind,
        content,
        dimensions,
        terminal_running,
    }
}
