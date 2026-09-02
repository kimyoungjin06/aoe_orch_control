//! Transport-neutral identity for one live supervised agent session.

use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};

use anyhow::{bail, ensure, Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::Instance;

pub const REMOTE_SESSION_IDENTITY_SCHEMA: &str = "remote_session_identity.v1";
const MACHINE_ID_MAX_BYTES: u64 = 4096;
const MACHINE_ID_HASH_DOMAIN: &[u8] = b"forager.remote-session.machine.v1\0";
const PROJECT_ROOT_HASH_DOMAIN: &[u8] = b"forager.remote-session.project-root.v1\0";
const WORKTREE_HASH_DOMAIN: &[u8] = b"forager.remote-session.worktree.v1\0";
const HARNESS_HASH_DOMAIN: &[u8] = b"forager.remote-session.harness.v1\0";
const SESSION_RECORD_HASH_DOMAIN: &[u8] = b"forager.remote-session.record.v1\0";
const TRANSPORT_SESSION_HASH_DOMAIN: &[u8] = b"forager.remote-session.tmux-session.v1\0";
const PANE_HASH_DOMAIN: &[u8] = b"forager.remote-session.tmux-pane.v1\0";
const PROCESS_HASH_DOMAIN: &[u8] = b"forager.remote-session.process.v1\0";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RemoteSessionWorktreeKindV1 {
    DirectProjectRoot,
    ManagedWorktree,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteSessionIdentityV1 {
    pub schema: String,
    pub machine_identity_sha256: String,
    pub project_key: Option<String>,
    pub project_root_identity_sha256: String,
    pub worktree_kind: RemoteSessionWorktreeKindV1,
    pub worktree_identity_sha256: String,
    pub harness_kind: String,
    pub harness_launch_sha256: String,
    pub session_id: String,
    pub session_record_identity_sha256: String,
    pub transport_kind: String,
    pub transport_session_identity_sha256: String,
    pub pane_identity_sha256: String,
    pub process_identity_sha256: String,
    pub binding_sha256: String,
}

impl RemoteSessionIdentityV1 {
    pub fn observe(instance: &Instance, project_key: Option<&str>) -> Result<Self> {
        let harness_kind = instance.tool.trim().to_string();
        ensure!(!harness_kind.is_empty(), "session harness kind is empty");
        let harness_launch_sha256 = hash_parts(
            HARNESS_HASH_DOMAIN,
            &[harness_kind.as_str(), instance.get_tool_command()],
        );
        Self::observe_with_launch_hash(instance, project_key, &harness_launch_sha256)
    }

    fn observe_with_launch_hash(
        instance: &Instance,
        project_key: Option<&str>,
        harness_launch_sha256: &str,
    ) -> Result<Self> {
        ensure!(
            is_sha256(harness_launch_sha256),
            "policy-bound harness launch hash is not a canonical SHA-256"
        );
        let worktree_path = absolute_path(Path::new(&instance.project_path))?;
        let (project_root_path, worktree_kind) = match &instance.worktree_info {
            Some(worktree) => (
                absolute_path(Path::new(&worktree.main_repo_path))?,
                RemoteSessionWorktreeKindV1::ManagedWorktree,
            ),
            None => (
                worktree_path.clone(),
                RemoteSessionWorktreeKindV1::DirectProjectRoot,
            ),
        };
        let project_root = RootBinding::open(&project_root_path, PROJECT_ROOT_HASH_DOMAIN)?;
        let worktree = RootBinding::open(&worktree_path, WORKTREE_HASH_DOMAIN)?;

        let tmux_session = instance.tmux_session()?;
        let runtime_before = tmux_session.runtime_identity()?;
        let runtime_fields = parse_tmux_runtime_identity(&runtime_before)?;
        let pane_pid = runtime_fields[3]
            .parse::<u32>()
            .context("parse tmux pane PID")?;
        let pane_process_identity = crate::process::process_identity(pane_pid)
            .context("observe tmux pane process identity")?;
        let foreground_pid = tmux_session
            .get_foreground_pid()
            .context("observe agent foreground process")?;
        let foreground_identity = crate::process::process_identity(foreground_pid)
            .context("observe agent foreground process identity")?;
        ensure!(
            tmux_session.runtime_identity()? == runtime_before,
            "tmux runtime changed while observing remote session identity"
        );
        project_root.ensure_active()?;
        worktree.ensure_active()?;

        let transport_session_identity_sha256 = hash_parts(
            TRANSPORT_SESSION_HASH_DOMAIN,
            &[runtime_fields[0], runtime_fields[1]],
        );
        let pane_identity_sha256 = hash_parts(
            PANE_HASH_DOMAIN,
            &[runtime_fields[2], &pane_process_identity],
        );
        let process_identity_sha256 =
            hash_parts(PROCESS_HASH_DOMAIN, &[foreground_identity.as_str()]);
        let harness_kind = instance.tool.trim().to_string();
        ensure!(!harness_kind.is_empty(), "session harness kind is empty");
        let created_at = instance.created_at.to_rfc3339();
        let session_record_identity_sha256 = hash_parts(
            SESSION_RECORD_HASH_DOMAIN,
            &[instance.id.as_str(), created_at.as_str()],
        );

        let mut identity = Self {
            schema: REMOTE_SESSION_IDENTITY_SCHEMA.to_string(),
            machine_identity_sha256: machine_identity_sha256()?,
            project_key: project_key.map(str::to_string),
            project_root_identity_sha256: project_root.identity_sha256,
            worktree_kind,
            worktree_identity_sha256: worktree.identity_sha256,
            harness_kind,
            harness_launch_sha256: harness_launch_sha256.to_string(),
            session_id: instance.id.clone(),
            session_record_identity_sha256,
            transport_kind: "tmux".to_string(),
            transport_session_identity_sha256,
            pane_identity_sha256,
            process_identity_sha256,
            binding_sha256: String::new(),
        };
        identity.binding_sha256 = identity.recompute_binding_sha256()?;
        identity.validate()?;
        Ok(identity)
    }

    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.schema == REMOTE_SESSION_IDENTITY_SCHEMA,
            "remote session identity schema is invalid"
        );
        for (label, value) in [
            ("machine_identity_sha256", &self.machine_identity_sha256),
            (
                "project_root_identity_sha256",
                &self.project_root_identity_sha256,
            ),
            ("worktree_identity_sha256", &self.worktree_identity_sha256),
            ("harness_launch_sha256", &self.harness_launch_sha256),
            (
                "session_record_identity_sha256",
                &self.session_record_identity_sha256,
            ),
            (
                "transport_session_identity_sha256",
                &self.transport_session_identity_sha256,
            ),
            ("pane_identity_sha256", &self.pane_identity_sha256),
            ("process_identity_sha256", &self.process_identity_sha256),
            ("binding_sha256", &self.binding_sha256),
        ] {
            ensure!(is_sha256(value), "{label} is not a canonical SHA-256");
        }
        for (label, value) in [
            ("harness_kind", self.harness_kind.as_str()),
            ("session_id", self.session_id.as_str()),
            ("transport_kind", self.transport_kind.as_str()),
        ] {
            ensure!(
                !value.trim().is_empty()
                    && value.len() <= 128
                    && !value.chars().any(char::is_control),
                "remote session {label} is invalid"
            );
        }
        if let Some(project_key) = &self.project_key {
            ensure!(
                !project_key.trim().is_empty()
                    && project_key.len() <= 128
                    && !project_key.chars().any(char::is_control),
                "remote session project_key is invalid"
            );
        }
        ensure!(
            self.transport_kind == "tmux",
            "remote session transport kind is unsupported"
        );
        ensure!(
            self.binding_sha256 == self.recompute_binding_sha256()?,
            "remote session identity binding hash is invalid"
        );
        Ok(())
    }

    fn recompute_binding_sha256(&self) -> Result<String> {
        let mut value = serde_json::to_value(self)?;
        value
            .as_object_mut()
            .context("serialize remote session identity object")?
            .remove("binding_sha256");
        Ok(sha256_hex(&serde_json::to_vec(&value)?))
    }
}

struct RootBinding {
    path: PathBuf,
    file: File,
    identity: (u64, u64),
    identity_sha256: String,
}

impl RootBinding {
    fn open(path: &Path, domain: &[u8]) -> Result<Self> {
        let file = crate::process::open_bounded_root(path)
            .with_context(|| format!("open remote session root {}", path.display()))?;
        let identity = crate::process::filesystem_file_identity(&file.metadata()?)
            .context("filesystem identity is unavailable")?;
        let active = std::fs::symlink_metadata(path)?;
        ensure!(
            active.is_dir() && crate::process::filesystem_file_identity(&active) == Some(identity),
            "remote session root is not the exact active directory"
        );
        let path_text = path.to_string_lossy();
        let device = identity.0.to_string();
        let inode = identity.1.to_string();
        let identity_sha256 = hash_parts(
            domain,
            &[path_text.as_ref(), device.as_str(), inode.as_str()],
        );
        Ok(Self {
            path: path.to_path_buf(),
            file,
            identity,
            identity_sha256,
        })
    }

    fn ensure_active(&self) -> Result<()> {
        let reopened =
            crate::process::open_bounded_root(&self.path).context("reopen remote session root")?;
        let active = std::fs::symlink_metadata(&self.path)?;
        ensure!(
            crate::process::filesystem_file_identity(&self.file.metadata()?) == Some(self.identity)
                && crate::process::filesystem_file_identity(&reopened.metadata()?)
                    == Some(self.identity)
                && crate::process::filesystem_file_identity(&active) == Some(self.identity),
            "remote session root changed while observing identity"
        );
        Ok(())
    }
}

fn parse_tmux_runtime_identity(identity: &str) -> Result<[&str; 4]> {
    let fields = identity.split('|').collect::<Vec<_>>();
    if fields.len() != 4 || fields.iter().any(|field| field.is_empty()) {
        bail!("tmux runtime identity has an invalid shape");
    }
    Ok([fields[0], fields[1], fields[2], fields[3]])
}

fn absolute_path(path: &Path) -> Result<PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };
    Ok(absolute)
}

#[cfg(target_os = "linux")]
fn machine_identity_sha256() -> Result<String> {
    let root = crate::process::open_bounded_root(Path::new("/etc"))
        .context("open machine identity root")?;
    let root_identity = crate::process::filesystem_file_identity(&root.metadata()?)
        .context("machine identity root has no filesystem identity")?;
    let mut file = crate::process::open_bounded_regular_file(&root, Path::new("machine-id"))
        .context("open machine identity")?;
    let before = file.metadata()?;
    let file_identity = crate::process::filesystem_file_identity(&before)
        .context("machine identity file has no filesystem identity")?;
    ensure!(
        before.len() <= MACHINE_ID_MAX_BYTES,
        "machine identity exceeds its byte budget"
    );
    let mut bytes = Vec::new();
    file.by_ref()
        .take(MACHINE_ID_MAX_BYTES + 1)
        .read_to_end(&mut bytes)?;
    ensure!(
        bytes.len() as u64 <= MACHINE_ID_MAX_BYTES,
        "machine identity grew beyond its byte budget"
    );
    let after = file.metadata()?;
    let reopened_file = crate::process::open_bounded_regular_file(&root, Path::new("machine-id"))
        .context("reopen machine identity")?;
    let reopened_root = crate::process::open_bounded_root(Path::new("/etc"))
        .context("reopen machine identity root")?;
    ensure!(
        crate::process::FilesystemObjectSnapshot::from_metadata(&before)
            == crate::process::FilesystemObjectSnapshot::from_metadata(&after)
            && crate::process::filesystem_file_identity(&reopened_file.metadata()?)
                == Some(file_identity)
            && crate::process::filesystem_file_identity(&reopened_root.metadata()?)
                == Some(root_identity)
            && crate::process::filesystem_file_identity(&root.metadata()?) == Some(root_identity),
        "machine identity changed while reading"
    );
    let value = std::str::from_utf8(&bytes)?.trim();
    ensure!(
        value.len() == 32 && value.bytes().all(|byte| byte.is_ascii_hexdigit()),
        "machine identity has an invalid shape"
    );
    Ok(hash_parts(MACHINE_ID_HASH_DOMAIN, &[value]))
}

#[cfg(not(target_os = "linux"))]
fn machine_identity_sha256() -> Result<String> {
    bail!("stable machine identity observation is not implemented on this operating system")
}

fn hash_parts(domain: &[u8], parts: &[&str]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(domain);
    for part in parts {
        hasher.update((part.len() as u64).to_be_bytes());
        hasher.update(part.as_bytes());
    }
    format!("{:x}", hasher.finalize())
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn fixture() -> RemoteSessionIdentityV1 {
        let mut identity = RemoteSessionIdentityV1 {
            schema: REMOTE_SESSION_IDENTITY_SCHEMA.to_string(),
            machine_identity_sha256: "a".repeat(64),
            project_key: Some("forager".to_string()),
            project_root_identity_sha256: "b".repeat(64),
            worktree_kind: RemoteSessionWorktreeKindV1::ManagedWorktree,
            worktree_identity_sha256: "c".repeat(64),
            harness_kind: "codex".to_string(),
            harness_launch_sha256: "d".repeat(64),
            session_id: "session-one".to_string(),
            session_record_identity_sha256: "e".repeat(64),
            transport_kind: "tmux".to_string(),
            transport_session_identity_sha256: "f".repeat(64),
            pane_identity_sha256: "1".repeat(64),
            process_identity_sha256: "2".repeat(64),
            binding_sha256: String::new(),
        };
        identity.binding_sha256 = identity.recompute_binding_sha256().unwrap();
        identity
    }

    #[test]
    fn canonical_identity_rejects_every_unbound_change() {
        let identity = fixture();
        identity.validate().unwrap();
        for mutate in [
            |value: &mut RemoteSessionIdentityV1| value.machine_identity_sha256 = "3".repeat(64),
            |value: &mut RemoteSessionIdentityV1| {
                value.project_root_identity_sha256 = "3".repeat(64)
            },
            |value: &mut RemoteSessionIdentityV1| value.worktree_identity_sha256 = "3".repeat(64),
            |value: &mut RemoteSessionIdentityV1| value.harness_kind = "claude".to_string(),
            |value: &mut RemoteSessionIdentityV1| value.session_id = "session-two".to_string(),
            |value: &mut RemoteSessionIdentityV1| {
                value.transport_session_identity_sha256 = "3".repeat(64)
            },
            |value: &mut RemoteSessionIdentityV1| value.pane_identity_sha256 = "3".repeat(64),
            |value: &mut RemoteSessionIdentityV1| value.process_identity_sha256 = "3".repeat(64),
        ] {
            let mut forged = identity.clone();
            mutate(&mut forged);
            assert!(forged.validate().is_err());
        }
    }

    #[test]
    fn live_tmux_observation_binds_every_identity_plane_when_available() -> Result<()> {
        if std::process::Command::new("tmux")
            .arg("-V")
            .output()
            .is_err()
        {
            return Ok(());
        }
        let temp = tempdir()?;
        let project_root = temp.path().join("project-root");
        let worktree = temp.path().join("worktree");
        std::fs::create_dir(&project_root)?;
        std::fs::create_dir(&worktree)?;
        let mut instance = Instance::new("remote-identity", &worktree.to_string_lossy());
        let launch_profile = crate::offdesk::RemoteSessionLaunchProfileV1 {
            launch_profile_id: "generic_sleep".to_string(),
            session_kind: crate::offdesk::RemoteSessionKindV1::Agent,
            harness: crate::offdesk::RemoteSessionHarnessV1::Generic,
            executable_path: "/bin/sleep".to_string(),
            fixed_argv: vec!["/bin/sleep".to_string(), "30".to_string()],
            fixed_environment: vec![crate::offdesk::RemoteSessionEnvironmentEntryV1 {
                key: "TERM".to_string(),
                value: "forager-policy-term".to_string(),
            }],
            yolo_mode: true,
            automatic_orchestrator: false,
            worktree_policy: crate::offdesk::RemoteSessionWorktreePolicyV1::ManagedWorktreeRequired,
            hook_policy: crate::offdesk::RemoteSessionHookPolicyV1::PretrustedOnly,
        };
        instance.tool = "generic".to_string();
        instance.command = crate::offdesk::remote_session_policy_launch_command(&launch_profile)?;
        instance.worktree_info = Some(crate::session::WorktreeInfo {
            branch: "test".to_string(),
            main_repo_path: project_root.to_string_lossy().to_string(),
            managed_by_forager: true,
            created_at: chrono::Utc::now(),
            cleanup_on_delete: false,
        });
        let session = instance.tmux_session()?;
        session.create(&instance.project_path, Some(&instance.command))?;
        std::thread::sleep(std::time::Duration::from_millis(100));

        let observed = RemoteSessionIdentityV1::observe(&instance, Some("forager-test"));
        session.kill()?;
        let observed = observed?;
        observed.validate()?;
        assert_eq!(observed.session_id, instance.id);
        assert_eq!(observed.project_key.as_deref(), Some("forager-test"));
        assert_eq!(observed.harness_kind, "generic");
        assert_eq!(observed.transport_kind, "tmux");
        assert_eq!(
            observed.worktree_kind,
            RemoteSessionWorktreeKindV1::ManagedWorktree
        );
        assert_ne!(
            observed.project_root_identity_sha256,
            observed.worktree_identity_sha256
        );
        assert_ne!(
            observed.harness_launch_sha256,
            crate::offdesk::remote_session_policy_harness_launch_sha256(&launch_profile)?
        );
        Ok(())
    }
}
