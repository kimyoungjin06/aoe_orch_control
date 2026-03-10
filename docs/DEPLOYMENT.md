# DEPLOYMENT

## 1. Goal

This document defines how to deploy `aoe_orch_control` as a package-managed
control plane while keeping project runtime state local.

The deployment rule is simple:

- deploy versioned package assets from the repository
- generate `.aoe-team/` runtime state per target project after deployment

## 2. What Is Deployed

Deploy these versioned paths:

- `scripts/`
- `templates/`
- `docs/`
- `tests/`
- `systemd/`
- `.github/` if CI is part of the deployment target

Important runtime-facing entrypoints:

- `scripts/team/aoe-team-stack.sh`
- `scripts/team/runtime/telegram_tmux.sh`
- `scripts/team/runtime/worker_codex_handler.sh`
- `scripts/gateway/aoe-telegram-gateway.py`
- `scripts/team/bootstrap_runtime_templates.sh`

Do not treat `.aoe-team/` as package source.

## 3. What Must Stay Local

The following are local runtime artifacts and must not be shipped as source of
truth:

- `.aoe-team/orch_manager_state.json`
- `.aoe-team/auto_scheduler.json`
- `.aoe-team/tf_exec_map.json`
- `.aoe-team/telegram_gateway_state.json`
- `.aoe-team/logs/`
- `.aoe-team/messages/`
- `.aoe-team/tf_runs/`
- `.aoe-team/telegram.env`
- `.aoe-team/team.json`
- `.aoe-team/orchestrator.json`
- `.aoe-team/workers/*.json`
- `.aoe-team/agents/*/AGENTS.md`

Generated compatibility shims inside `.aoe-team/` are also runtime-local:

- `.aoe-team/telegram_tmux.sh`
- `.aoe-team/telegram_stack.sh`
- `.aoe-team/worker_codex_handler.sh`

## 4. Pre-Deployment Checks

From the repository root:

```bash
git status --short
python3 -m py_compile scripts/gateway/*.py
bash -n scripts/team/aoe-team-stack.sh \
  scripts/team/runtime/telegram_tmux.sh \
  scripts/team/runtime/worker_codex_handler.sh \
  scripts/team/bootstrap_runtime_templates.sh
```

If gateway tests are available in the environment:

```bash
scripts/gateway_pytest.sh
```

Minimum release check:

- no unintended runtime files are staged
- gateway modules compile
- team/runtime shell entrypoints pass syntax check

## 5. Fresh Install

### 5.1 Clone or copy the package tree

Example:

```bash
git clone <repo-url> /opt/aoe_orch_control
cd /opt/aoe_orch_control
```

### 5.2 Install global CLI

```bash
bash scripts/team/install_global_cli.sh
```

This exposes:

- `aoe-team-stack`
- `aoe-team-tmux`

### 5.3 Initialize runtime for a target project

For each managed project root:

```bash
aoe-team-stack --project-root /path/to/project init
```

Equivalent lower-level bootstrap:

```bash
bash scripts/team/bootstrap_runtime_templates.sh --project-root /path/to/project
```

This creates or refreshes missing runtime files in:

- `/path/to/project/.aoe-team/`

### 5.4 Configure Telegram/runtime environment

Edit local runtime env:

- `/path/to/project/.aoe-team/telegram.env`

At minimum, configure:

- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_OWNER_CHAT_ID`
- ACL / owner-only settings as needed

### 5.5 Start the stack

```bash
aoe-team-stack --project-root /path/to/project start
aoe-team-stack --project-root /path/to/project status
aoe-team-stack --project-root /path/to/project health --wait=10
```

## 6. Multi-Project Operation Model

The same package tree can manage multiple project runtimes.

Pattern:

- one package install
- many project roots
- one `.aoe-team/` per project root

Example:

```bash
aoe-team-stack --project-root /srv/projects/O3 start
aoe-team-stack --project-root /srv/projects/O4 start
```

Operator-side discovery and orchestration continue through Telegram and the
runtime registry stored in each project's `.aoe-team/`.

## 7. Upgrade Procedure

### 7.1 Package upgrade

Update only the versioned package tree:

```bash
cd /opt/aoe_orch_control
git pull --ff-only
```

### 7.2 Refresh runtime files

For each target project:

```bash
aoe-team-stack --project-root /path/to/project init
```

This is the safe way to refresh generated compatibility shims and missing
template-backed runtime files without treating runtime state as versioned source.

### 7.3 Restart and validate

```bash
aoe-team-stack --project-root /path/to/project restart
aoe-team-stack --project-root /path/to/project health --wait=30
```

Then verify from Telegram:

- `/whoami`
- `/map`
- `/offdesk prepare`

## 8. Systemd User Deployment

Install user services from the package root:

```bash
bash scripts/systemd/install_user_services.sh
```

Useful commands:

```bash
systemctl --user status aoe-telegram-stack.service
systemctl --user status aoe-telegram-heal.timer
systemctl --user restart aoe-telegram-stack.service
systemctl --user start aoe-telegram-heal.service
```

To keep services alive after logout:

```bash
sudo loginctl enable-linger <user>
```

## 9. Deployment Smoke Checklist

For each deployed project:

```bash
aoe-team-stack --project-root /path/to/project status
aoe-team-stack --project-root /path/to/project overview
aoe-team-stack --project-root /path/to/project health --wait=10
```

Telegram smoke:

- `/whoami`
- `/help`
- `/map`
- `/offdesk prepare`

If worker/runtime issues are suspected:

```bash
aoe-team-stack --project-root /path/to/project logs
```

## 10. Rollback Principle

Rollback the package tree, not the runtime directory.

That means:

- revert or redeploy the repository version
- keep `.aoe-team/` as local operational state unless a specific recovery step
  requires restoration from backup

If runtime regeneration is needed after rollback:

```bash
aoe-team-stack --project-root /path/to/project init
aoe-team-stack --project-root /path/to/project restart
```

## 11. Related Docs

- `docs/ARCHITECTURE.md`
- `docs/RUNBOOK.md`
- `docs/SYSTEMD_USER_SETUP.md`
- `docs/COMMANDS.md`
