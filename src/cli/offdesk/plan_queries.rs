//! Read-only CLI query adapter for registered Offdesk plans.
//!
//! Registry storage owns filesystem reads and typed workflow modules own the
//! read models. This adapter coordinates filtering, ordering, and reference
//! resolution for every CLI presentation surface.

use anyhow::{bail, Result};

use super::plan_registry::{
    find_offdesk_plan_registry_item, load_offdesk_plan_registry_detail,
    load_offdesk_plan_registry_items,
};
use super::{PlansArgs, RemoteOperatorPlansArgs};
use crate::offdesk::{OffdeskPlanRegistryDetail, OffdeskPlanRegistryItem};

pub(super) struct OffdeskPlanListQuery<'a> {
    pub project_key: Option<&'a str>,
    pub task_id: Option<&'a str>,
    pub profile_key: Option<&'a str>,
    pub artifact_kind: Option<&'a str>,
    pub latest: bool,
}

impl<'a> OffdeskPlanListQuery<'a> {
    pub(super) fn from_plans_args(args: &'a PlansArgs) -> Self {
        Self {
            project_key: args.project_key.as_deref(),
            task_id: args.task_id.as_deref(),
            profile_key: args.profile_key.as_deref(),
            artifact_kind: args.artifact_kind.as_deref(),
            latest: args.latest,
        }
    }

    pub(super) fn from_remote_operator_args(args: &'a RemoteOperatorPlansArgs) -> Self {
        Self {
            project_key: args.project_key.as_deref(),
            task_id: args.task_id.as_deref(),
            profile_key: args.profile_key.as_deref(),
            artifact_kind: args.artifact_kind.as_deref(),
            latest: args.latest,
        }
    }
}

pub(super) fn query_offdesk_plans(
    profile: &str,
    query: &OffdeskPlanListQuery<'_>,
) -> Result<Vec<OffdeskPlanRegistryItem>> {
    let items = load_offdesk_plan_registry_items(profile)?;
    Ok(apply_offdesk_plan_query(items, query))
}

pub(super) fn query_offdesk_plan_detail(
    profile: &str,
    plan_ref: &str,
) -> Result<OffdeskPlanRegistryDetail> {
    let item = resolve_offdesk_plan_item(profile, plan_ref)?;
    load_offdesk_plan_registry_detail(item)
}

pub(super) fn resolve_offdesk_plan_item(
    profile: &str,
    plan_ref: &str,
) -> Result<OffdeskPlanRegistryItem> {
    let items = load_offdesk_plan_registry_items(profile)?;
    let Some(item) = find_offdesk_plan_registry_item(items, plan_ref) else {
        bail!("Registered Offdesk plan not found: {plan_ref}");
    };
    Ok(item)
}

fn apply_offdesk_plan_query(
    mut items: Vec<OffdeskPlanRegistryItem>,
    query: &OffdeskPlanListQuery<'_>,
) -> Vec<OffdeskPlanRegistryItem> {
    items.retain(|item| offdesk_plan_matches_query(item, query));
    items.sort_by(|left, right| {
        (left.registration.registered_at, left.plan_id.as_str())
            .cmp(&(right.registration.registered_at, right.plan_id.as_str()))
    });
    if query.latest {
        return items.pop().into_iter().collect();
    }
    items
}

fn offdesk_plan_matches_query(
    item: &OffdeskPlanRegistryItem,
    query: &OffdeskPlanListQuery<'_>,
) -> bool {
    query
        .project_key
        .is_none_or(|expected| item.registration.project_key.as_deref() == Some(expected))
        && query
            .task_id
            .is_none_or(|expected| item.registration.task_id.as_deref() == Some(expected))
        && query
            .profile_key
            .is_none_or(|expected| item.registration.profile_key.as_deref() == Some(expected))
        && query
            .artifact_kind
            .is_none_or(|expected| item.registration.artifact_kind == expected)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::offdesk::{
        build_offdesk_plan_registry_item, offdesk_plan_registration_denials,
        OffdeskPlanRegistration, OffdeskPlanRegistrationArtifacts,
        OFFDESK_PLAN_REGISTRATION_SCHEMA,
    };

    fn item(
        plan_id: &str,
        registered_at: &str,
        project_key: &str,
        task_id: &str,
        profile_key: &str,
        artifact_kind: &str,
    ) -> OffdeskPlanRegistryItem {
        let registration_path = format!("/tmp/{plan_id}/registration.json");
        build_offdesk_plan_registry_item(
            plan_id.to_string(),
            registration_path.clone(),
            OffdeskPlanRegistration {
                schema: OFFDESK_PLAN_REGISTRATION_SCHEMA.to_string(),
                registered_at: registered_at.parse().expect("valid timestamp"),
                forager_profile: "forager-ops".to_string(),
                source_path: format!("/tmp/{plan_id}/source.json"),
                source_sha256: format!("hash_{plan_id}"),
                artifact_kind: artifact_kind.to_string(),
                plan_schema: format!("{artifact_kind}.v1"),
                profile_key: Some(profile_key.to_string()),
                profile_name: None,
                project_key: Some(project_key.to_string()),
                request_id: None,
                task_id: Some(task_id.to_string()),
                ready_for_operator_review: true,
                ready_for_launch_preparation: false,
                ready_for_enqueue: false,
                validation_failures: Vec::new(),
                decision: None,
                consensus: None,
                selected_plan_path: None,
                dry_run: false,
                artifacts: OffdeskPlanRegistrationArtifacts {
                    registry_dir: Some(format!("/tmp/{plan_id}")),
                    registration_json: Some(registration_path),
                    copied_source_json: Some(format!("/tmp/{plan_id}/source.json")),
                },
                does_not_authorize: offdesk_plan_registration_denials(),
            },
            &[],
            &[],
        )
    }

    #[test]
    fn filters_every_supported_field_exactly() {
        let query = OffdeskPlanListQuery {
            project_key: Some("project"),
            task_id: Some("task"),
            profile_key: Some("generic"),
            artifact_kind: Some("offdesk_multiturn_plan"),
            latest: false,
        };
        let items = vec![
            item(
                "match",
                "2026-08-13T01:00:00Z",
                "project",
                "task",
                "generic",
                "offdesk_multiturn_plan",
            ),
            item(
                "wrong_project",
                "2026-08-13T02:00:00Z",
                "other",
                "task",
                "generic",
                "offdesk_multiturn_plan",
            ),
            item(
                "wrong_task",
                "2026-08-13T03:00:00Z",
                "project",
                "other",
                "generic",
                "offdesk_multiturn_plan",
            ),
            item(
                "wrong_profile",
                "2026-08-13T04:00:00Z",
                "project",
                "task",
                "other",
                "offdesk_multiturn_plan",
            ),
            item(
                "wrong_kind",
                "2026-08-13T05:00:00Z",
                "project",
                "task",
                "generic",
                "offdesk_planner_council",
            ),
        ];

        let selected = apply_offdesk_plan_query(items, &query);

        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].plan_id, "match");
    }

    #[test]
    fn orders_deterministically_and_latest_returns_only_newest_match() {
        let items = vec![
            item(
                "newest",
                "2026-08-13T03:00:00Z",
                "project",
                "task",
                "generic",
                "offdesk_multiturn_plan",
            ),
            item(
                "oldest",
                "2026-08-13T01:00:00Z",
                "project",
                "task",
                "generic",
                "offdesk_multiturn_plan",
            ),
            item(
                "middle",
                "2026-08-13T02:00:00Z",
                "project",
                "task",
                "generic",
                "offdesk_multiturn_plan",
            ),
            item(
                "newest_001",
                "2026-08-13T03:00:00Z",
                "project",
                "task",
                "generic",
                "offdesk_multiturn_plan",
            ),
        ];
        let all_query = OffdeskPlanListQuery {
            project_key: Some("project"),
            task_id: None,
            profile_key: None,
            artifact_kind: None,
            latest: false,
        };

        let ordered = apply_offdesk_plan_query(items.clone(), &all_query);
        let latest = apply_offdesk_plan_query(
            items,
            &OffdeskPlanListQuery {
                latest: true,
                ..all_query
            },
        );

        assert_eq!(
            ordered
                .iter()
                .map(|item| item.plan_id.as_str())
                .collect::<Vec<_>>(),
            vec!["oldest", "middle", "newest", "newest_001"]
        );
        assert_eq!(latest.len(), 1);
        assert_eq!(latest[0].plan_id, "newest_001");
    }
}
