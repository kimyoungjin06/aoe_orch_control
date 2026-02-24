# AOE Team Runtime Templates

This directory stores versioned templates for mutable runtime files under `.aoe-team`.

Runtime files in `.aoe-team` are local state and intentionally untracked.

Recommended initialization order:
1. `aoe-orch init --project-root <path> --overview "<text>"`
2. `bash scripts/team/bootstrap_runtime_templates.sh --project-root <path>`

The bootstrap script copies missing template files into `.aoe-team` without overwriting existing runtime files unless `--force` is used.
