# Project Flow & Branching (Multi-Agent TF)

## Macro Flow
1. select project
2. create/select TF
3. plan
4. execute
5. critic
6. integrate
7. close or retry/escalate

## Branching
| condition | branch | action |
|---|---|---|
| gate green + critic pass | continue | next todo |
| critic fail (recoverable) | retry | rerun in same/new TF |
| evidence insufficient | pending | keep open, request artifacts |
| external dependency | blocked_external | escalate with unblock condition |
| policy conflict | blocked_policy | halt and request owner decision |
| objective met | handoff_ready | create handoff, close TF |

## Switch Order
1. lock update
2. registry update
3. project ongoing update
4. TF ongoing update
5. runlog update
