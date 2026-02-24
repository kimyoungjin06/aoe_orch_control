# ORCH v2 Objective (Task-Oriented Multi-Agent Runtime)

## 1) Product Goal
- A project folder is connected to one Orchestrator (Orch).
- Orch inspects the project and initializes `AGENTS.md` + role policies.
- For each user task/todo, Orch creates ephemeral sub-agents, delegates execution, and runs independent verification agents.
- Execution and verification are done by sub-agents. Orch only plans, routes, tracks, and decides completion.
- When task is done, sub-agents are terminated to avoid long-lived context drift.
- Telegram is the remote natural-language control plane for multiple Orch instances.

## 2) Why
- Solve context bloat by task-scoped agents.
- Enable autonomous off-hours work with auditable delegation and quality gates.
- Keep human in control via simple natural-language commands from Telegram.

## 3) Core Principles
- Chat-first UX: default is natural conversation with Orch.
- Explicit delegation: sub-agent dispatch only when needed or explicitly requested.
- Ephemeral workers: create for task, destroy on completion.
- Verification-first: reviewer/checker agents are mandatory for completion.
- Auditable state: every task has plan, execution logs, verification result, final summary.

## 4) Runtime Model
- `Manager` (Telegram/Bot): routes user messages to target Orch.
- `Orch` (per project): planner/router/supervisor only.
- `Planner Agent`: creates sub-task DAG + acceptance criteria.
- `Executor Agents`: implement sub-tasks.
- `Verifier Agents`: test/review/fact-check outputs.
- `Integrator Agent`: merge accepted outputs into final deliverable.

## 5) Task Lifecycle
1. Intake: user request received by Orch.
2. Planning: todo list + sub-task plan + acceptance criteria generated.
3. Staffing: required agent roles spawned (ephemeral).
4. Execution: executors produce artifacts.
5. Verification: verifiers approve/reject with evidence.
6. Integrate: integrator composes final result.
7. Close: Orch reports status/result and terminates spawned agents.

## 6) Completion Gate (Definition of Done)
- Required sub-tasks are completed.
- Required verifier checks passed.
- Final summary includes:
  - what changed
  - evidence
  - unresolved risks (if any)
- Spawned sub-agents are cleaned up.

## 7) Telegram UX Contract
- User talks to Orch in natural language by default.
- Orch replies in a single coherent voice.
- Internal role/protocol details are hidden unless user asks.
- Control commands remain available (status/check/run/switch), but normal conversation should not require command syntax.

## 8) Scope Decision
- Keep existing codebase and refactor architecture layers.
- Do not full-rewrite from scratch.
- Replace current auto-dispatch/prompt policy with task-lifecycle policy.

## 9) MVP Exit Criteria
- Create Orch from project path with automatic bootstrap.
- Natural-language task intake -> plan -> delegate -> verify -> final response.
- Ephemeral sub-agent spawn/teardown per task.
- Telegram control for multi-Orch switch and progress query.
- Stable overnight run for at least one real project scenario.
