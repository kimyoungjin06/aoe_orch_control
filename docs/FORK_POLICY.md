# FORK_POLICY

## 1. Upstream
- Upstream project: `njbrake/agent-of-empires`
- Upstream URL: `https://github.com/njbrake/agent-of-empires`
- This repository is an operational fork/workspace with local orchestration extensions.

## 2. Attribution and License
- Upstream source attribution must remain visible in `README.md` and project metadata.
- Upstream license text and copyright notices must be preserved.
- Third-party additions must include their own license references when required.

## 3. Local Change Boundaries
- Allowed local changes:
- Telegram control plane, orchestration policy, task monitor UX, runtime automation scripts
- Avoid broad rewrites of upstream core unless required for clear operational value.

## 4. Sync and Rebase Policy
- Track upstream security and critical bugfix changes on a regular cadence.
- Before major local release or rollout, review upstream delta from baseline commit.
- Record merge/rebase decision and conflict notes in project docs.

## 5. Security and Secrets
- Runtime secrets (e.g., bot token) must never be committed.
- `.aoe-team/telegram.env` and runtime state/log/message artifacts are excluded by `.gitignore`.
- If secret leakage occurs, rotate token immediately and document incident handling.

## 6. Traceability
- Every local milestone should reference:
- upstream baseline commit
- changed components
- validation evidence

## 7. Contribution Strategy
- If local improvement is generally useful and not environment-specific, consider upstream PR.
- Environment-specific operational glue can remain local with clear docs.
