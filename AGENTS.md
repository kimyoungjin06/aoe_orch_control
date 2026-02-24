# AGENTS.md

## Project
- Name: `aoe_orch_control`
- Coordinator: `Orchestrator`

## Project Overview
데이터 파이프라인 품질 검증 프로젝트

## Team Roles
- `Orchestrator`
- `DataEngineer`
- `Reviewer`

## Communication Protocol
- Use `aoe-team send` for every assignment.
- Assignee must run `aoe-team ack <id>` as first response.
- On completion assignee must run `aoe-team done <id>` with a short evidence note.
- If blocked assignee must run `aoe-team fail <id> --note "<reason>"`.
- Replies must use `aoe-team reply <id> --body "..."` to preserve thread context.
- Priority policy: `P1` blocks current work, `P2` same day, `P3` best effort.
- Request tracing uses `request_id`; check with `aoe-team request --request-id <id>`.

## Working Rules
- Keep tasks small and handoff-ready.
- Include expected output path and acceptance criteria in every task message.
- Coordinator owns final integration and conflict resolution.

## Local Paths
- Team root: `/home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/.aoe-team`
- Role guides: `/home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/.aoe-team/agents`

## Startup
- Set project-local mailbox:
  - `export AOE_TEAM_DIR=/home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/.aoe-team`
- Check inbox:
  - `aoe-team inbox --unresolved`

## Source Attribution
- Fork/Base project: `njbrake/agent-of-empires`
- Upstream URL: `https://github.com/njbrake/agent-of-empires`
- This workspace tracks local orchestration extensions for Telegram-driven multi-session control.
