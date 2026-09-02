use anyhow::Result;

use crate::offdesk::{RemoteSessionPolicyInspectionV1, RemoteSessionPolicyResolutionV1};

pub(super) fn present_remote_session_policy_inspection(
    report: &RemoteSessionPolicyInspectionV1,
    json: bool,
) -> Result<()> {
    if json {
        let safe = super::operator_safe_json_value(serde_json::to_value(report)?);
        println!("{}", serde_json::to_string_pretty(&safe)?);
        return Ok(());
    }

    println!("Remote session policy inspection");
    println!(
        "Policy: {}",
        crate::offdesk::operator_safe_text(&report.policy_id)
    );
    println!(
        "Schema: {}",
        crate::offdesk::operator_safe_text(&report.policy_schema)
    );
    println!("Policy SHA-256: {}", report.policy_sha256);
    println!("Enabled: {}", report.enabled);
    println!("Allowed roots: {}", report.roots.len());
    for root in &report.roots {
        println!(
            "  - {} (project {}, path sha256 {}, profiles: {})",
            crate::offdesk::operator_safe_text(&root.root_id),
            crate::offdesk::operator_safe_text(&root.project_key),
            root.canonical_path_sha256,
            root.allowed_launch_profile_ids
                .iter()
                .map(|value| crate::offdesk::operator_safe_text(value))
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
    println!("Launch profiles: {}", report.launch_profiles.len());
    for profile in &report.launch_profiles {
        println!(
            "  - {} ({:?}, {:?}, yolo={}, worktree={:?})",
            crate::offdesk::operator_safe_text(&profile.launch_profile_id),
            profile.session_kind,
            profile.harness,
            profile.yolo_mode,
            profile.worktree_policy
        );
    }
    println!(
        "Capacity: global {}, per root {}",
        report.capacity.max_remote_active_global, report.capacity.max_remote_active_per_root
    );
    println!("Request TTL: {}s", report.request_ttl_seconds);
    println!("Authority: read-only policy inspection");
    println!("Profile state read: no");
    println!("Root or executable resolution: not authorized");
    println!("Request creation or launch: not authorized");
    Ok(())
}

pub(super) fn present_remote_session_policy_resolution(
    report: &RemoteSessionPolicyResolutionV1,
    json: bool,
) -> Result<()> {
    if json {
        let safe = super::operator_safe_json_value(serde_json::to_value(report)?);
        println!("{}", serde_json::to_string_pretty(&safe)?);
        return Ok(());
    }

    println!("Remote session policy resolution");
    println!(
        "Policy: {}",
        crate::offdesk::operator_safe_text(&report.policy_id)
    );
    println!("Policy SHA-256: {}", report.policy_sha256);
    println!(
        "Root: {} (project {}, identity {})",
        crate::offdesk::operator_safe_text(&report.root_id),
        crate::offdesk::operator_safe_text(&report.project_key),
        report.project_root_identity_sha256
    );
    println!(
        "Launch profile: {} ({:?}, yolo={}, worktree={:?})",
        crate::offdesk::operator_safe_text(&report.launch_profile_id),
        report.harness,
        report.yolo_mode,
        report.worktree_policy
    );
    println!(
        "Executable: identity {}, content {}, symlink hops {}",
        report.executable_identity_sha256,
        report.executable_content_sha256,
        report.executable_symlink_hops
    );
    if let (Some(identity), Some(content), Some(size)) = (
        report.executable_runtime_loader_identity_sha256.as_deref(),
        report.executable_runtime_loader_content_sha256.as_deref(),
        report.executable_runtime_loader_size_bytes,
    ) {
        println!(
            "Runtime loader: bound identity {}, content {}, {} bytes",
            identity, content, size
        );
    } else {
        println!("Runtime loader: not present");
    }
    println!(
        "Executable chain budgets: {} bytes, {} ms hashing",
        report.executable_chain_byte_budget, report.executable_chain_hash_budget_millis
    );
    println!("Structured argv entries: {}", report.argv_count);
    println!(
        "Environment keys: {}",
        report
            .environment_keys
            .iter()
            .map(|value| crate::offdesk::operator_safe_text(value))
            .collect::<Vec<_>>()
            .join(", ")
    );
    println!("Authority: read-only exact policy resolution");
    println!("Profile or project registry state read: no");
    println!("Request creation or launch: not authorized");
    Ok(())
}
