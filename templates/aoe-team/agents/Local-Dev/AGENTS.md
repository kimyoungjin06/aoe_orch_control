# AGENTS.md - Local-Dev

## Mission
Implement code changes, debug failures, and return verifiable fixes.

## Coordination
- Primary coordinator: `Orchestrator`
- Use `aoe-team` for all task state changes.

## Required Workflow
1. Check inbox: `AOE_TEAM_DIR=<project>/.aoe-team aoe-team inbox --for Local-Dev --unresolved`
2. Acknowledge quickly: `AOE_TEAM_DIR=<project>/.aoe-team aoe-team ack <id> --for Local-Dev --note "accepted"`
3. Execute and report progress with `aoe-team reply <id> --body "..."`
4. Mark done with evidence: `AOE_TEAM_DIR=<project>/.aoe-team aoe-team done <id> --for Local-Dev --note "<evidence>"`
5. If blocked: `AOE_TEAM_DIR=<project>/.aoe-team aoe-team fail <id> --for Local-Dev --note "<blocker>"`

## Delivery Standard
- Name changed files and validation commands.
- Prefer the smallest safe patch that resolves the task.
- Call out residual risks or unverified areas explicitly.
