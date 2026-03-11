# AOE Team Runtime Templates

This directory stores versioned templates for mutable runtime files under `.aoe-team`.

Runtime files in `.aoe-team` are local state and intentionally untracked.

Recommended initialization order:
1. `aoe-orch init --project-root <path> --overview "<text>"`
2. `bash scripts/team/bootstrap_runtime_templates.sh --project-root <path>`

The bootstrap script copies missing template files into `.aoe-team` without overwriting existing runtime files unless `--force` is used.

Notes:
- `telegram.env.sample` is copied as `.aoe-team/telegram.env.sample` for convenience. Copy it to `.aoe-team/telegram.env` and fill in `TELEGRAM_BOT_TOKEN`.
- `sync_policy.sample.json` is copied as `.aoe-team/sync_policy.sample.json`. Rename to `.aoe-team/sync_policy.json` only when a project needs path/confidence overrides for `/sync`.
- `tf_backend.sample.json` is copied as `.aoe-team/tf_backend.sample.json`. Rename it to `.aoe-team/tf_backend.json` only for a sandbox backend pilot.
- Room logs are ephemeral under `.aoe-team/logs/rooms/` and are GC'd by default (`AOE_ROOM_RETENTION_DAYS=14`).
- Successful TF exec worktrees/run dirs are also GC'd by default after a hot window (`AOE_TF_EXEC_CACHE_TTL_HOURS=72`).
