# Multi-Agent TF Operations Template

## Purpose
- Provide a non-destructive documentation scaffold for Mother-Orch -> Project-Orch -> TF execution.
- Keep low-context startup while supporting multi-project and multi-TF handoff.

## Placeholders
- `__MODULE_NAME__`
- `__ACTIVE_PROJECT__`
- `__ACTIVE_TF__`
- `__TAG__`

## Apply
```bash
./apply_template.sh <target_project_root> <module_name> <active_project> <active_tf> <tag>
```

Example:
```bash
./apply_template.sh ~/Desktop/Workspace/aoe_orch_control aoe_orch_control O1 TF-001 2026-02-26
```

## Safety
- Default mode is non-destructive: existing files are kept.
- Use `--force` only when intentionally overwriting template-managed files.
