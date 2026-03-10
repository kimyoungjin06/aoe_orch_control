# AOE_TODO.md

Project scenario (per-project, runtime file).

This file is imported into the Mother-Orch todo queue via `/sync`.
Only task lines are parsed; everything else is ignored.

## Tasks

# Optional: keep your canonical todo in `<project_root>/TODO.md` and include it here.
@include ../TODO.md

# Supported formats (parsed by /sync):
# - [ ] summary        (open; default priority: P2)
# - summary            (open; default priority: P2)
# - 1. summary         (open; default priority: P2)
# - [ ] P1: summary
# - [x] P2: summary
# - P3: summary
#
# Notes:
# - You can include an explicit TODO id to update an existing item:
#   - [ ] TODO-123 P2: Adjust thresholds
# - Done lines ([x]) only mark done when the matching TODO already exists.

# - [ ] P2: (write your task here)

## Examples (ignored by /sync)

```text
- [ ] P1: Fix failing unit tests
- [ ] P2: Refactor scheduler handler
- [ ] P3: Update docs and runbook
```
