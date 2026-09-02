# Remote Session Creation And Isolation Plan

Updated: 2026-09-02

Status: S2-A verified; S2-B Linux local gates and independent review passed,
macOS CI pending; S2-C closed

Current authority: strict local S2-A contracts and migration-free read-only
policy inspection and exact target resolution only. This plan does not authorize
request creation, a session launch, a Telegram deployment, a new listener
permission, arbitrary remote commands, automatic hook trust, project
registration, worktree deletion, or a benchmark score change.

## 1. Goal

Close scenario S2 in `remote-agent-control-benchmark.md` with one deterministic
end-to-end path that can start an interactive Forager session remotely while
proving all of the following together:

1. the project root came from an owner-controlled allowlist;
2. the harness command and YOLO behavior came from a local launch profile, not
   Telegram text;
3. the selected isolation policy was applied and observed;
4. the remote concurrency cap was reserved before launch;
5. new, resumed, already-running, duplicate, and conflicting requests are
   different durable outcomes;
6. the launched tmux process is bound to one `remote_session_identity.v1`;
7. crash recovery cannot create a second session or worktree;
8. cleanup removes only exact pristine resources created by the failed
   transaction and never deletes dirty or replaced state.

S2 is not complete when Telegram merely queues a shell command, when
`forager go --no-attach` happens to start a process, or when separate tests
independently cover queueing, launch, and identity. One scenario must prove the
whole authority chain.

## 2. Existing Surfaces To Reuse

- Telegram single-owner and optional exact multi-owner identity checks;
- control-generation binding, confirmation TTL, update journal, effect receipt,
  and reply outbox;
- `offdesk tick` as the only unattended launcher;
- scheduler approval, global operator pause, and task-scoped tick filtering;
- the native `Instance` and tmux session implementation;
- automatic-Orchestrator and repository startup preflights;
- `remote_session_identity.v1` for post-launch machine, root, worktree,
  harness, durable-session, tmux, pane, and process binding;
- OS-specific bounded root and process identity helpers in `src/process/`;
- managed Git worktree creation and removal primitives after they satisfy this
  plan's exact-identity and cleanup requirements.

The project registry remains useful for labels, wiki routing, and project
discovery. It is not S2 launch authority. Its current substring matching and
read-failure-to-empty behavior are intentionally presentation-friendly and are
not strict enough for a remote execution allowlist.

## 3. Explicit Non-Goals

The first S2 implementation does not:

- accept a raw filesystem path from Telegram;
- accept a raw shell command, tool argument, environment variable, branch, or
  YOLO override from Telegram;
- register a new project remotely;
- trust repository hooks or Codex project trust remotely;
- provide a general remote terminal;
- start Docker or restore the retired sandbox feature;
- delete a worktree that has changes, unpushed commits, an identity mismatch,
  or an uncertain ownership record;
- infer provider-native session creation or cloud relay behavior;
- resolve hard gate 5, which still requires separate live network storage and
  deletion evidence.

## 4. Current Gaps And Rejected Shortcuts

### 4.1 Project registry is not an allowlist

`resolve_unique_project_for_path` rejects ambiguity, but registry patterns are
substring matches and `load_registry` degrades an unreadable or invalid file to
an empty registry. S2 requires a separate strict policy whose failure blocks the
request before a confirmation is stored.

### 4.2 `forager go` is not a remote transaction

`forager go --no-attach` has useful local UX, but it:

- accepts an arbitrary local path and optional tool arguments;
- can run or prompt for repository hooks;
- uses human text rather than a strict launch receipt;
- performs load, create, start, and save without an S2 request ledger;
- cannot prove a capacity reservation or crash-safe replay.

Wrapping it in `local_background` or `local_tmux` would create two unrelated
runtime identities and could mark the wrapper complete while the interactive
session remains alive. S2 needs a native tick handler.

### 4.3 Session storage is not transaction authority

The current `Storage::save` backup and path write are sufficient for the local
UI path but do not provide an exclusive mutation session, atomic no-follow
replacement, or exact crash reconciliation. The S2 launch path must use a
bounded session mutation primitive and must not claim safety from the current
path write alone.

### 4.4 Tick limit is not a concurrency cap

`offdesk tick --limit 1` limits work examined in one invocation. It does not
count already-running remote sessions or pending launch reservations. S2 needs
a durable remote-session capacity reservation checked under the tick lock.

### 4.5 Existing hook behavior is too permissive for remote launch

Interactive hook prompts and warning-only hook failures are not valid in an
unattended launch. A remote launch must require an already trusted, exact hook
configuration. It must run any authorized launch hook through a strict
preflight and then call the session start primitive without rerunning it.

## 5. Authority Model

The remote transport selects local identifiers. It never supplies executable
launch material.

```text
Telegram intent
  -> strict local policy resolution
  -> read-only launch preview
  -> generation-bound operator confirmation
  -> durable remote-session request
  -> Offdesk task and scheduler approval
  -> tick-held capacity and launch reservation
  -> exact root and worktree revalidation
  -> native Forager session create or resume
  -> live remote_session_identity.v1 observation
  -> durable launch receipt
  -> operator-safe result card
```

The confirmation authorizes creation of the request, not direct process
launch. The scheduler approval and tick remain separate launch authority.
Operator pause must hold a confirmed request without consuming its launch
reservation.

The task continues to use the existing `dispatch.runtime` capability and
scheduler approval. It gains a typed `interactive_session` launch payload that
references the request ID. A raw task command cannot substitute for that
payload, and this plan does not create a parallel approval capability.

## 6. Strict Local Policy

Add a standalone owner-controlled `remote_session_policy.v1` file. Keep it
separate from `SessionConfig` in the first slice so it cannot silently inherit
profile merges or require a partial settings-TUI implementation.

Minimum logical fields:

```text
schema
enabled
policy_id
allowed_roots[]
  root_id
  canonical_path
  project_key
  allowed_launch_profile_ids[]
launch_profiles[]
  launch_profile_id
  session_kind
  harness
  executable_path
  fixed_argv[]
  fixed_environment[]
  yolo_mode
  automatic_orchestrator
  worktree_policy
  hook_policy
capacity
  max_remote_active_global
  max_remote_active_per_root
request_ttl_seconds
```

Policy requirements:

- [ ] Default to absent or `enabled=false`.
- [ ] Reject duplicate root IDs, duplicate launch-profile IDs, unknown profile
  references, empty fixed argv, relative executables, uncontrolled environment
  keys, and zero capacity.
- [ ] Reject `/`, a home directory, a workspace umbrella directory, relative
  roots, symlinks, non-directories, and roots outside the explicitly supplied
  installation boundary.
- [ ] Read through a bounded, stable, owner-only, single-link, no-follow
  descriptor snapshot.
- [ ] Compute one canonical policy SHA-256 and bind it into every preview,
  confirmation, request, reservation, and receipt.
- [ ] Treat the project registry key as presentation metadata after the exact
  root is resolved. It cannot broaden the root.
- [ ] Permit only fixed local harness profiles. Telegram may select a profile
  ID but cannot append arguments or override session kind, YOLO, automatic
  Orchestrator behavior, hooks, environment, branch, model, or provider.
- [ ] Resolve and bind the executable and allowed symlink chain during preview,
  then revalidate the same executable identity immediately before tmux launch.
  Do not perform a fresh `PATH` lookup at the execution boundary.
- [ ] Convert structured argv to the tmux launch form without interpolating any
  Telegram-derived text.
- [ ] Expose a read-only inspection command that redacts command details not
  needed by the remote card.

Recommended first policy defaults:

- one global remote session;
- one remote session per root;
- `managed_worktree_required` for the benchmark launch profile;
- `pretrusted_only` hook policy;
- YOLO decided by the local launch profile, never by remote input;
- no implicit automatic-Orchestrator child. A launch profile may explicitly
  create one Orchestrator as the requested session, and it counts against
  remote capacity;
- no free-form fallback profile.

## 7. Versioned Contracts

### 7.1 `remote_session_launch_preview.v1`

Required evidence:

```text
preview_id
created_at
expires_at
profile
operator_identity_sha256
machine_identity_sha256
control_generation_sha256
policy_id
policy_sha256
root_id
project_key
initial_project_root_identity_sha256
launch_profile_id
harness
harness_launch_sha256
yolo_mode
worktree_policy
requested_disposition
resolved_disposition
matching_session_id
matching_worktree_identity_sha256
capacity_observation
observed_state_sha256
```

`requested_disposition` is `auto`, `new`, or `resume`. A preview does not
reserve capacity and does not create a session, task, branch, or worktree.
`harness_launch_sha256` is canonically derived from the selected policy
profile's harness, executable, fixed argv, fixed environment, and YOLO mode. A
preview cannot supply an independent launch hash.

### 7.2 `remote_session_launch_request.v1`

The request binds the exact preview, one random application ID, one
idempotency key, the operator confirmation, policy hash, root identity, launch
profile, resolved disposition, expiry, and observed state hash. It contains no
Telegram token and no raw operator ID.

### 7.3 `remote_session_launch_reservation.v1`

The reservation is written under the tick lock before any session row,
worktree, hook, or tmux mutation. It binds:

- request and attempt identity;
- global and per-root capacity counts;
- intended session ID;
- intended worktree path and branch when managed isolation is selected;
- exact worktree identity once the reservation reaches a state that requires
  an existing worktree;
- initial root and policy identities;
- reservation state and expiry.

Reservation states:

- `reserved`
- `session_row_committed`
- `tmux_started`
- `identity_bound`
- `completed`
- `recovery_required`
- `released`

### 7.4 `remote_session_launch_receipt.v1`

Terminal result values:

- `created`
- `resumed`
- `already_running`
- `duplicate_replay`
- `held_capacity`
- `held_pause`
- `blocked_policy`
- `blocked_stale_root`
- `blocked_hook_trust`
- `blocked_conflict`
- `recovery_required`
- `failed_clean`

A successful receipt includes the full validated
`remote_session_identity.v1` and its canonical hash. `duplicate_replay`
returns the original receipt identity and cannot launch again.

### 7.5 `remote_session_cleanup_receipt.v1`

`failed_clean` requires a separate typed cleanup receipt. It binds the exact
request, reservation, intended session, absent session row, absent transport,
worktree outcome, and a canonical cleanup observation hash. Its ID must differ
from the terminal launch receipt, and the launch receipt must carry the exact
canonical cleanup receipt hash. Successful and non-clean-failure results reject
supplied cleanup evidence.

This contract validator proves byte-level pairing with supplied evidence only.
It does not prove that an original replay receipt or cleanup receipt came from
the durable ledger. S2-C must load those exact bytes from the protected ledger
before calling the validator. Caller-supplied self-consistent evidence is not
launch authority.

## 8. Identity And Disposition Rules

Use one stable launch key derived from:

```text
profile + policy_sha256 + root_id + initial root identity
+ launch_profile_id + worktree policy and scope
```

Required behavior:

- [ ] `auto` returns `already_running` when exactly one compatible live session
  exists.
- [ ] `auto` returns `resumed` when exactly one compatible durable stopped
  session and its exact worktree exist.
- [ ] `auto` returns `created` when no compatible durable session exists and
  capacity is available.
- [ ] Explicit `new` conflicts with a compatible active request unless the
  local policy explicitly permits parallel isolated worktrees.
- [ ] Explicit `resume` requires an exact session ID. Titles, group names, and
  harness labels are not authority.
- [ ] A repeated idempotency key with identical bytes returns the original
  request or receipt. The same key with different bytes is an integrity error.
- [ ] Multiple compatible sessions, a replaced root, a replaced worktree, a
  changed policy, or a changed launch profile fail closed.
- [ ] A running session is never restarted merely because a new request asks
  for resume.

## 9. Isolation And Worktree Policy

The S2 benchmark path uses a temporary Git repository and
`managed_worktree_required`.

- [ ] Create the branch and worktree only after durable reservation.
- [ ] Generate both names from the request identity, not Telegram text.
- [ ] Require the worktree parent to be fixed by local policy and outside the
  project working tree.
- [ ] Reject symlinked parents, cross-device replacement where unsupported,
  an existing destination, and a destination that escapes the bounded parent.
- [ ] Record main repository identity, worktree identity, branch, initial HEAD,
  and ownership marker.
- [ ] Revalidate project root, worktree parent, policy, and active path before
  the session row commit, before tmux start, and before the final receipt.
- [ ] Keep `direct_existing_root` as a later explicit policy option. It must be
  reported as shared-root execution and cannot satisfy the strong isolation
  benchmark by itself.

## 10. Capacity And Reservation Rules

The first cap applies to remote-managed sessions and reservations in one
Forager profile. Manually started on-desk sessions are reported separately and
do not silently consume or enlarge remote capacity.

- [ ] Count `reserved`, `session_row_committed`, `tmux_started`, and live
  completed remote sessions.
- [ ] Exclude terminal released reservations only after exact liveness
  reconciliation.
- [ ] Check global and per-root limits under the existing tick lock.
- [ ] Persist the reservation before starting work.
- [ ] Recheck capacity immediately before tmux creation.
- [ ] Two concurrent ticks or replayed updates must produce at most one launch.
- [ ] A held request remains queued and does not churn confirmations,
  approvals, branches, or worktrees.
- [ ] Operator pause wins over available capacity.

## 11. Native Launch And Recovery

Do not invoke `forager go` as a child process. Extract or add a native session
creation service used by both a future local JSON command and the Offdesk tick
handler.

Implementation requirements:

- [ ] Add one exclusive, bounded session mutation session shared by every
  writer of `sessions.json` and `groups.json`, including `add`, `go`, session
  start, restart, rename, remove, automatic-Orchestrator creation, and the S2
  tick path. Every writer performs a post-lock reload.
- [ ] Use atomic, owner-only, no-follow persistence for sessions, groups, and
  launch ledgers.
- [ ] Precompute the session ID before mutation and store it in the
  reservation.
- [ ] Run strict pretrusted hook verification before session creation.
- [ ] Run allowed hooks once, fail on error, and call the lower-level start
  primitive with hook replay disabled.
- [ ] Bind automatic-Orchestrator startup preflight results into the receipt.
- [ ] Reject any unrequested implicit child-session creation and count an
  explicitly requested Orchestrator session against the same capacity budget.
- [ ] Observe the created tmux session until a bounded prompt/startup state or
  timeout.
- [ ] Require the live session envelope to match the reserved machine, root,
  worktree, harness, durable session ID, tmux session, pane, and process.
- [ ] Never mark `created` or `resumed` from process existence alone.

After a new session is identity-bound, the existing wiki-context bridge may
prepare and present the exact project brief. That presentation is a separate
post-launch receipt. Its failure cannot fabricate a launch failure or accepted
knowledge. A resumed or already-running session receives no automatic input;
it continues to require explicit `context-sync`.

Crash matrix:

| Durable boundary | Retry behavior |
| --- | --- |
| Request only | Reserve and continue once after current policy/root recheck |
| Reservation only | Reuse the exact intended session/worktree IDs |
| Worktree exists, no session row | Adopt only an exact pristine owned worktree; otherwise require recovery |
| Session row exists, no tmux | Start the exact row once after full revalidation |
| tmux exists, no identity receipt | Observe and bind only the exact reserved runtime; never create another |
| Identity receipt exists, task not finalized | Return the same receipt and finalize task state |
| Conflicting row, worktree, tmux, or receipt | Fail closed with `recovery_required` |

## 12. Cleanup Contract

Cleanup has two distinct scopes.

### Failed pre-use cleanup

Automatic cleanup may run only when all of the following are exact:

- the request owns the reservation;
- the session never reached a usable prompt;
- the tmux identity is absent or was stopped by the same recovery session;
- the session row is still byte-identical to the created row;
- the managed worktree is pristine, on the recorded branch and HEAD, and still
  owned by the request;
- root and parent identities are unchanged.

### Post-use retirement

Stopping a live session does not delete its worktree. A separate local review
must decide whether to retain, archive, or remove it. Dirty state, unpushed
commits, identity drift, ambiguous ownership, or failed liveness checks always
leave the worktree in place and emit `cleanup_review_required`.

Required cleanup evidence:

- [ ] zero orphan tmux sessions after a clean pre-start failure;
- [ ] zero orphan reservations after exact recovery;
- [ ] zero deleted dirty worktrees;
- [ ] an operator-visible retained path and reason for every refused cleanup;
- [ ] idempotent cleanup receipts for retries and crashes.

## 13. Telegram Surface

Add Telegram only after the local contract and transaction pass independently.

Proposed compact commands:

```text
/start-work
/start-work <root-id> <launch-profile-id> [auto|new]
/resume-work <session-id>
```

Behavior:

- [ ] `/start-work` lists only operator-safe local root and launch-profile IDs.
- [ ] Natural language may resolve to these IDs, but unresolved or ambiguous
  text asks a question and creates no confirmation.
- [ ] The preview card shows project, harness, YOLO state, worktree policy,
  requested versus resolved disposition, capacity, and expiry.
- [ ] `/confirm` stores the durable request. It does not directly invoke tmux.
- [ ] A later card distinguishes queued, approval-pending, held-capacity,
  created, resumed, already-running, recovery-required, and failed-clean.
- [ ] Every callback revalidates operator pair, control generation, policy
  hash, observed state hash, and confirmation TTL.
- [ ] Token rotation terminally revokes an uncommitted launch confirmation.
- [ ] Reply failure after the local effect uses the existing outbox and cannot
  repeat the request or launch.
- [ ] No card contains a raw command, secret environment value, absolute
  private path, or unredacted Telegram identity.

## 14. Implementation Slices

### S2-A: Contract fixtures and validators

- [x] Add strict Rust types for policy-derived preview, request, reservation,
  and receipt.
- [x] Freeze canonical JSON and SHA-256 fixtures.
- [x] Reject duplicate keys, unknown enums, unsafe text, invalid timestamps,
  invalid state transitions, hash drift, and cross-contract identity mismatch.
- [x] Add a read-only policy inspection command with no profile creation or
  migration side effect.

Gate: `GO_S2_STRICT_POLICY_RESOLUTION` only after contract tests, formatting,
strict Clippy, and an independent adversarial review pass.

Local implementation evidence:

- `src/offdesk/remote_session.rs` owns the deny-unknown-field schemas,
  domain-separated canonical hashes, exact contract-chain validator, complete
  reservation transition graph, receipt-to-original replay validator, typed
  cleanup receipt validator, policy-derived harness launch hash, and
  inspection-only bounded policy reader.
- `tests/fixtures/remote_session_creation/` freezes policy, preview, request,
  reservation, receipt, cleanup receipt, live-session identity reference, and
  expected hashes.
- `tests/remote_session_creation_contract.rs` covers canonical round trips,
  drift and malformed-input rejection, every reservation state pair, safe
  source-file and parent authority, stale request and successful receipt
  expiry rejection, exact resume and live identity binding, typed cleanup and
  duplicate replay evidence, redacted output, a genuinely empty profile root,
  and profile-independent behavior for both binaries.
- `forager offdesk remote-session policy-inspect --policy <FILE> --json` reads
  no Forager profile state and returns explicit false authority for root or
  executable resolution, request creation, and launch.
- The first independent review returned `REVISE_S2_A` after reproducing stale
  reservation, live identity, secret identifier, config-read, self-replay, and
  writable-parent counterexamples. Those counterexamples are now explicit
  negative regressions.
- The second independent review returned `REVISE_S2_A` after reproducing a
  success receipt after reservation expiry, a resumed session that differed
  from the preview match, an untyped self-referential cleanup receipt, and
  coordinated drift of the preview and live harness hashes. The revised
  validator now derives a single policy launch hash, binds exact resume IDs,
  limits successful receipts to the reservation window, and requires typed
  cleanup evidence. The focused suite now has 13 tests. A fresh full-tree
  verification and independent re-review are still required before the gate
  can open.
- The third review confirmed those direct cases were closed but reproduced
  three adjacent gaps: recovery and cleanup outcomes did not bind a Resume
  match, a completed reservation could be presented as a clean pre-use
  failure, and the generic live observer used a different launch hash. The
  reservation now binds every Resume match before result handling and records
  its exact release origin. `failed_clean` rejects a completed origin and
  preserves an existing Resume worktree. The policy launch hash remains an
  expected contract value, while the generic observer remains available only
  for presentation identity. A canonical cleanup JSON fixture and golden hash
  are now frozen.
- The fourth review proved that a proposed policy-bound helper could accept a
  tmux session whose actual environment differed from policy because it
  injected the expected hash after checking only the session record. That
  helper has been removed. S2-A exposes no production policy-bound observer or
  successful launch authority. S2-C must add typed executable, argv,
  environment, and concrete YOLO evidence and validate it against the live
  process before it may create a policy-bound identity. A fresh full-tree
  verification and independent re-review were required before the gate could
  open.
- The final independent re-review returned
  `GO_S2_STRICT_POLICY_RESOLUTION`. It found no blocking issue after replaying
  the prior environment counterexample and confirmed that the public surface
  remains policy inspection only. The exact tree passed the 13 focused
  contracts, live tmux observer regression, full repository tests, formatting,
  strict all-target Clippy, mdBook, and diff checks.

The S2-A gate is now satisfied. S2-B may implement bounded strict policy
loading, canonical hashing, and exact descriptor-bound root and executable
resolution. This does not open request or reservation ledgers, capacity
reservation, session or worktree creation, tmux mutation, policy-bound live
identity generation, or Telegram integration.

### S2-B: Strict policy and exact root resolver

- [x] Implement bounded policy loading and canonical hashing.
- [x] Resolve exact allowed roots with descriptor identity.
- [x] Keep project registry metadata presentation-only.
- [x] Add unsafe policy, symlink, hardlink, replacement, ambiguity, oversized
  file, and workspace-umbrella rejection tests.

Gate: `GO_S2_LOCAL_LAUNCH_TRANSACTION`.

Local implementation evidence:

- `forager offdesk remote-session policy-resolve` is profile-independent,
  migration-free, and read-only. It requires one explicit absolute
  installation boundary plus exact root and launch-profile IDs.
- The resolver keeps the project registry out of authority, rejects the
  installation boundary itself as a workspace umbrella, and opens the selected
  root descriptor-relative beneath that boundary without following symlinks or
  crossing a filesystem boundary.
- The executable resolver performs no `PATH` lookup. It bounds and snapshots an
  owner-controlled symlink and directory chain, opens the resolved regular file
  without following a final symlink, checks executable ownership, link count,
  effective owner mode, native ELF or Mach-O format, byte and elapsed hashing
  budgets, and content hash, and retains root, component, and executable
  descriptors for later revalidation. Script entrypoints are rejected so a
  shebang cannot reintroduce `/usr/bin/env` or `PATH` resolution.
- The operator report redacts raw roots, executable paths, argv values, and
  environment values. It exposes only selector metadata, hashes, counts,
  environment key names, and explicit false request and launch authority.
- Twenty-two focused contract tests pass, including policy and root replacement,
  allowed executable symlink-chain replacement, hardlinked executable,
  disabled policy, unsafe policy shapes, oversized source, root symlink,
  non-directory root, outside-boundary root, workspace-umbrella rejection,
  unsafe intermediate directories, ineffective execute mode, script
  entrypoints, executable byte budget, and executable-parent replacement.
- The first independent S2-B review returned `REVISE_S2_B`. It reproduced
  writable intermediate root and executable directories, a current-owner
  `0401` executable, a Codex shebang that reintroduced `env` and `PATH`, and a
  prefix-ambiguous root identity hash. The corrected resolver validates and
  retains every directory component, admits only a native binary executable,
  applies effective-user execute rules, uses length-prefixed root identity
  hashing, and exposes explicit byte and elapsed hash budgets.
- The second independent review also returned `REVISE_S2_B`. It reproduced a
  four-byte ELF-magic file that passed resolution but could not execute, and
  noted that a blocking read from a remote or userspace filesystem could exceed
  the elapsed budget before control returned to the process. The corrected
  resolver now validates architecture-specific ELF or Mach-O headers, bounded
  program or load-command tables, file-backed segment ranges, executable
  segments, and entrypoint evidence. It also rejects known network and
  userspace executable filesystems before reading executable content.
- The third independent review returned `REVISE_S2_B` after showing that modern
  CIFS and SMB2, Ceph, 9p, and unknown filesystems escaped the denylist, an ELF
  `PT_INTERP` path was syntactically checked but not identity-bound, and macOS
  accepted an empty `LC_UNIXTHREAD` or an `LC_MAIN` entry outside its executable
  segment. The corrected resolver now uses a narrow local-filesystem allowlist,
  resolves and recursively binds the ELF interpreter or Mach-O dynamic loader
  with the same descriptor, directory, symlink, identity, content, and active
  snapshot checks, rejects nested runtime loaders, rejects legacy Mach-O thread
  entry commands, and requires `LC_MAIN.entryoff` inside a file-backed
  executable segment.
- The twenty-two focused contracts now also reject truncated native magic,
  wrong-host ELF architecture, an overflowing ELF program table, a missing ELF
  interpreter, and runtime-loader identity replacement. Three internal
  regressions freeze the prefix-free root hash, fail-closed filesystem
  allowlist, and Mach-O entrypoint rules.
- The fourth independent review returned `REVISE_S2_B` for an ELF `PT_LOAD`
  file-size and memory-size inversion, applying the program schema to a macOS
  `MH_DYLINKER`, and missing Mach-O command alignment and section-table size
  checks. The resolver now validates every ELF load segment's file and memory
  sizes, power-of-two alignment, offset and address congruence, and file-backed
  entrypoint. Mach-O validation separates program and runtime-loader roles,
  parses architecture-specific x86_64 or arm64 thread-state program counters,
  binds them to executable virtual segments, and checks command alignment and
  exact `LC_SEGMENT_64` section-table size.
- Public root and executable descriptor accessors remain closed until S2-C.
  Reports distinguish the main executable's budget from the loader-inclusive
  chain maximum and expose only loader hashes, size, and binding status. The
  elapsed budget covers content validation and hashing after a descriptor is
  open; this implementation does not claim a wall-clock deadline for an
  initial `open` or `lstat` on a stalled mount.
- The fifth independent review returned `REVISE_S2_B` because active
  revalidation reopened a macOS `MH_DYLINKER` with the program role and hashed
  the loader once on its own and again as part of the program chain. Active
  revalidation now checks every held descriptor, directory, and symlink in the
  bound chain, then reopens the top-level program chain exactly once. The
  recursive reopen supplies the runtime-loader role explicitly, so one active
  revalidation stays within the reported loader-inclusive maximum. A
  macOS-only regression exercises a real program-to-loader chain and repeats
  active revalidation.
- The corrected tree passes the twenty-two focused contracts, three internal
  adversarial regressions, full repository test suite, formatting, strict
  all-target and all-feature Clippy, mdBook, and diff checks. The final
  independent code re-review returned `GO_S2_B` with no remaining blocking code
  finding. This Linux host cannot execute the macOS-only full-chain regression,
  so cross-platform completion remains conditional on a real `macos-latest`
  `cargo test --locked --all-targets --all-features` PASS.

The S2-B implementation does not open S2-C. Independent code review is GO, but
`GO_S2_LOCAL_LAUNCH_TRANSACTION` remains closed until the existing macOS CI
matrix produces the required PASS evidence. A macOS failure closes the gate and
becomes a new correctness finding.

### S2-C: Native launch transaction and capacity

- [ ] Add request and reservation ledgers.
- [ ] Add typed runtime launch evidence for the exact executable, argv,
  environment, and concrete YOLO flags, then validate it against live process
  and tmux evidence before producing a policy-bound identity.
- [ ] Migrate every session writer to the common lock and atomic persistence
  path, with concurrent `add`, `go`, start, and remote-request regressions.
- [ ] Add the typed `dispatch.runtime` interactive-session task payload and
  native tick handler.
- [ ] Add remote-only global and per-root capacity checks.
- [ ] Add new, resume, already-running, duplicate, conflict, pause, and held
  outcomes.
- [ ] Bind successful outcomes to `remote_session_identity.v1`.
- [ ] Cover every crash boundary in Section 11.

Gate: `GO_S2_MANAGED_WORKTREE_ISOLATION`.

### S2-D: Managed worktree isolation and cleanup

- [ ] Implement exact managed worktree creation from reservation identity.
- [ ] Add root, parent, branch, HEAD, destination, and ownership checks.
- [ ] Implement failed-pre-use cleanup and reviewed post-use retention.
- [ ] Add dirty, replaced, unpushed, symlinked, cross-device, and concurrent
  cleanup adversarial tests.

Gate: `GO_S2_TELEGRAM_ADAPTER`.

### S2-E: Telegram adapter

- [ ] Add routing, rendering, confirmation, and result cards.
- [ ] Reuse control generation, update journal, effect receipt, and outbox.
- [ ] Keep natural language resolution selector-only.
- [ ] Add fake Telegram replay tests for stale policy, stale root, owner/token
  rotation, callback replay, reply failure, and ambiguous intent.

Gate: `GO_S2_END_TO_END_BENCHMARK`.

### S2-F: Deterministic benchmark and deployment decision

- [ ] Run one Linux and real tmux scenario with two allowed temporary Git
  projects, one denied sibling, and a fixed fake interactive harness.
- [ ] Set global and per-root remote capacity to one.
- [ ] Create one isolated session and validate every identity plane.
- [ ] Race a second request and prove it is held without a second worktree or
  tmux session.
- [ ] Replay the first request and return the original receipt.
- [ ] Stop the exact tmux session, issue an exact resume, and prove the same
  durable session and worktree identities are reused.
- [ ] Replace the allowed root before launch and prove zero mutation.
- [ ] Trigger a pre-use failure and prove exact pristine cleanup.
- [ ] Dirty a worktree and prove cleanup refusal and operator-visible residue.
- [ ] Record ordered inputs, hashes, receipts, logs, process identities,
  duplicate-effect count, maximum active count, and cleanup outcomes.
- [ ] Rerun full repository tests, strict Clippy, formatting, mdBook, and diff
  checks.
- [ ] Deploy only after a separate operator decision and a recoverable service
  backup.

Gate: mark S2 `VERIFIED` only when the complete scenario passes on the same
tree. A score change is recorded only after that run. Hard gate 5 remains
separate.

## 15. Deterministic Acceptance Matrix

| Case | Required result | Forbidden effect |
| --- | --- | --- |
| Allowed root, free capacity, no match | `created` with full live identity | second session or shared-root fallback |
| Exact stopped match | `resumed` with same session/worktree IDs | new session ID |
| Exact live match | `already_running` | restart or prompt injection |
| Exact request replay | original receipt or `duplicate_replay` | duplicate task, worktree, hook, or tmux |
| Same idempotency key, changed bytes | integrity error | adoption of changed request |
| Denied or ambiguous root | `blocked_policy` before confirmation | request, approval, task, branch, or worktree |
| Root replaced after confirmation | `blocked_stale_root` | session row or tmux creation |
| Capacity exhausted | `held_capacity` | capacity oversubscription or retry churn |
| Operator paused | `held_pause` | reservation consumption or launch |
| Hook trust absent or changed | `blocked_hook_trust` | prompt, auto-trust, or hook execution |
| Crash after tmux start | exact reconciliation | second tmux session |
| Clean failed worktree | `failed_clean` plus cleanup receipt | orphan owned state |
| Dirty or replaced worktree | `cleanup_review_required` | deletion or path overwrite |
| Control generation changed | revoked confirmation | action under old token/owner |

## 16. Completion Metrics

The S2 evidence bundle must report exact counts rather than qualitative claims:

- duplicate process launches: `0`;
- cross-root mutations: `0`;
- maximum observed remote-active sessions with cap one: `1`;
- identity mismatches accepted: `0`;
- stale confirmations executed: `0`;
- pristine failed-launch orphans: `0`;
- dirty or replaced worktrees deleted: `0`;
- unclassified terminal requests: `0`.

If the scenario passes, the isolation axis may be reconsidered from 2 to 4,
which would raise the Candidate 0 coverage score from 86.3 to 91.3. That number
must not be published before the evidence run and remains coverage, not a
security certification. The overall verdict remains `PARTIAL` while hard gate
5 is unresolved.

## 17. Recommended Immediate Order

1. [Complete] Implement S2-A contract fixtures and strict validators
   without Telegram or tmux mutation.
2. [Complete] Complete full-tree verification and obtain an adversarial
   re-review of the corrected S2-A tree before opening S2-B.
3. [macOS CI pending] Verify the strict policy loader and exact root and
   executable resolver, then obtain `GO_S2_LOCAL_LAUNCH_TRANSACTION`.
4. [Closed] Build and test the native local transaction through Offdesk tick.
5. Add worktree isolation and cleanup recovery.
6. Add Telegram as the final adapter.
7. Run the single complete S2 benchmark before changing the score or deploying.

The immediate next unit is the S2-B macOS CI run. The Linux full-tree gates and
independent adversarial review passed on this candidate. S2-C remains closed:
no request, reservation, session, worktree, tmux process, policy-bound live
identity, or Telegram action may be created.
