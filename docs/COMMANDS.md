# COMMANDS

이 레포(`aoe_orch_control`)는 upstream AOE(`agent-of-empires`) 위에 **로컬 tmux 스택 제어 + Telegram 원격 콘솔**을 얹은 형태다.

명령어 표면(command surface)은 아래 4개로 분리되어 있다.

1. Upstream 실행 엔진(로컬 CLI): `aoe-orch`
2. Upstream 팀 프로토콜(로컬 CLI): `aoe-team`
3. 이 레포의 로컬 스택 제어(tel/tmux): `aoe-team-stack` (=`aoe-team-tmux`, 내부적으로 `scripts/team/runtime/telegram_tmux.sh`)
4. 이 레포의 Telegram 원격 콘솔: `/...` 슬래시 명령, 그리고 옵션으로 `aoe ...` CLI-스타일(봇이 파싱하는 입력 형식, 로컬 바이너리가 아님)

---

## 1) Upstream: `aoe-orch` (실행 엔진)

의미: 실제로 TF/Orch 작업을 “실행”하는 엔진이다. Telegram 게이트웨이/스케줄러는 최종적으로 `aoe-orch`를 호출한다.

이 레포에서 최소 전제로 사용하는 서브커맨드(대표):

- `aoe-orch init --project-root <path> --overview <text>`: 프로젝트(Orch) 초기화
- `aoe-orch spawn ...`: Orchestrator 세션(또는 작업) 생성
- `aoe-orch status ...`: 현재 상태 조회
- `aoe-orch worker --for <Role> ...`: 역할별 워커 루프 실행
- `aoe-orch add-role ...`: 워커 역할 추가
- `aoe add-claude <Role|--name Name> [--spawn]`: Claude 세션을 role로 추가
- `aoe add-codex <Role|--name Name> [--spawn]`: Codex 세션을 role로 추가

정확한 옵션/서브커맨드 목록은 upstream 문서를 기준으로 한다.

---

## 2) Upstream: `aoe-team` (팀 메시징 프로토콜)

의미: 팀(역할 세션) 간에 “업무 지시/응답”을 주고받는 메일박스/프로토콜 CLI다.

이 레포의 운영 규약(핵심):

- 지시는 항상 `aoe-team send`로 보낸다.
- 수신자는 첫 응답으로 `aoe-team ack <id>`를 실행한다.
- 진행은 `aoe-team reply <id> ...`, 완료는 `aoe-team done <id> ...`, 막힘은 `aoe-team fail <id> ...`.

대표 커맨드:

- `aoe-team inbox --unresolved`
- `aoe-team send --to <Role> --priority <P1|P2|P3> --title <...> --body <...>`
- `aoe-team ack|reply|done|fail <message_id> ...`
- `aoe-team request --request-id <id>`: request 추적

---

## 3) 이 레포: `aoe-team-stack` (Mother-Orch tmux 스택 제어)

의미: Mother-Orch(게이트웨이 + 워커 + UI + 스케줄러)를 tmux 세션들로 띄우고, **세션 전환 UX(Alt+1..9 / 페이지)**까지 포함해 관리한다. (세션 힌트는 tmux status bar 상단 라인으로 표시)

설치:

- `bash scripts/team/install_global_cli.sh`
- 설치 후 전역 커맨드: `aoe-team-stack`, `aoe-team-tmux` (동일 동작)

구현 위치:

- 포워더: `scripts/team/aoe-team-stack.sh`
- package-managed tmux 제어: `scripts/team/runtime/telegram_tmux.sh`
- `.aoe-team/telegram_tmux.sh` 는 runtime에 생성되는 compatibility shim

주요 서브커맨드(요약):

- `aoe-team-stack init`: 런타임 파일 템플릿 부트스트랩(비파괴)
- `aoe-team-stack start|stop|restart`
- `aoe-team-stack status|health|logs`
- `aoe-team-stack ui`: tmux 키바인딩/시각화 적용(Alt+1..9 등)
- `aoe-team-stack overview [--watch] [--compact]`: 숫자 매핑/요약
- `aoe-team-stack switch <idx|session>`: 즉시 전환/attach
- `aoe-team-stack page next|prev|set <N>|status|reset`
- `aoe-team-stack auto on|off|status`: tmux 백그라운드 스케줄러 세션 제어

worker runtime 권한 정책:

- 이 스택의 worker plane은 role의 `provider`를 보고 실행기를 고른다.
  - `provider=codex` -> `codex exec`
  - `provider=claude` -> `claude -p`
- Codex 권한 env:
  - `AOE_CODEX_PERMISSION_MODE=full|danger-full-access|workspace-write|read-only`
  - `AOE_CODEX_RUN_AS_ROOT=1`
- Claude 권한 env:
  - `AOE_CLAUDE_PERMISSION_MODE=full|workspace-write|read-only|auto|default`
  - `AOE_CLAUDE_RUN_AS_ROOT=1`
- `full` 계열은 worker runtime에서 사실상 YOLO/full-access로 해석된다.
  - Codex: `--dangerously-bypass-approvals-and-sandbox`
  - Claude: `--dangerously-skip-permissions --permission-mode bypassPermissions`
- Phase1 planning의 Claude provider도 같은 env를 읽는다.
  - 이제 `run_claude_exec()`는 `--add-dir <project_root>`와 provider별 permission mode를 사용한다.
  - 이전처럼 `--tools ""`로 막혀 있지 않아서 코드/문서 접근 기반 planning이 가능하다.
- 진단용:
  - `AOE_WORKER_DRY_RUN=1 bash scripts/team/runtime/worker_codex_handler.sh`
  - 실제 실행 없이 provider/launch/permission 플래그만 출력한다.

---

## 4) 이 레포: Telegram 원격 콘솔(슬래시 명령)

의미: 원격에서 Mother-Orch를 운영하기 위한 “콘솔”이다.

권장 입력:

- 기본은 **prefix-first**: `/status`, `!todo ...` 같은 형태.
- 일부 기능은 CLI-스타일 텍스트도 지원: `aoe status`, `aoe todo list` 같은 형태.
- 단, 운영 설정에 따라 `slash-only`가 켜져 있으면 슬래시만 허용된다.
- prefix는 환경변수 `AOE_TG_COMMAND_PREFIXES`로 바꿀 수 있다.
  - 권장: `AOE_TG_COMMAND_PREFIXES=!/` (표시는 `!`로, 입력은 `!`/`/` 둘 다 허용)

### A. 안전/권한/환경

- `/whoami`: 내 chat 권한/설정 확인
- `/tutorial`: 빠른 시작(온보딩) 가이드
- `/lockme`: allowlist를 “현재 chat 1개”로 잠금
- `/onlyme`: `/lockme` + owner-only(1:1 private DM gate)
- `/panic [status]`: (긴급) 자동 실행 즉시 중지. `auto/offdesk off` + pending/confirm 정리 + routing off
- `/clear [pending|routing|room|queue]`: 상태/로그/큐 정리(기본은 안전하게 동작)
- `/acl`: ACL 요약
- `/grant <allow|admin|readonly> <chat_id|alias>`
- `/revoke <allow|admin|readonly|all> <chat_id|alias>`

### B. 입력 라우팅(평문 처리)

- `/mode [on|off|direct|dispatch]` (단축: `/on`, `/off`)
- `/dispatch [요청]`: 팀 실행(배정)으로 라우팅. 인자 없이 쓰면 “다음 1회 평문 허용”
- `/direct [질문]`: 오케스트레이터 직접응답으로 라우팅. 인자 없이 쓰면 “다음 1회 평문 허용”
- `/cancel`: one-shot 대기 모드/확인 요청 해제
- `/ok`: 고위험 자동실행 확인(있을 때만)

### C. 모니터링(상태/태스크)

- `/status`: 게이트웨이/큐/TF 개수 등 상태 요약
- `/map`: 프로젝트(O1..) 매핑
  - 옵션: `AOE_ORCH_AUTO_DISCOVER=1`이면 `aoe list --json --all`을 스캔해(Workspace root 하위) 미등록 프로젝트를 자동 등록한다. (추가로 `AOE_ORCH_AUTO_INIT=1`이면 `<project_root>/.aoe-team/AOE_TODO.md` 템플릿도 자동 생성)
- `/use <O#|name>`: active 프로젝트(Orch) 전환
- `/orch pause <O#|name> [reason]`: 프로젝트를 일시정지(글로벌 스케줄러에서 제외)
- `/orch resume <O#|name>`: 일시정지 해제
- `/orch hide <O#|name> [reason]`: 프로젝트를 운영 기본 범위에서 숨김(`/map`, `/queue`, `/next`, `/fanout`, `/offdesk`, `/auto`에서 제외)
- `/orch unhide <O#|name>`: 숨김 해제
- `/monitor [N|O#]`: 최근 태스크 목록
- `/pick [번호|T-###|request_id]`: 태스크 포커스 선택
- `/check [T-###|request_id]`: 진행 요약
- `/task [T-###|request_id]`: 라이프사이클 상세
- `/kpi [hours]`: 최근 KPI/이벤트 요약
- `/request <T-###|request_id>`: request 원문/상태 조회(선택 태스크도 갱신)

### C2. TF Recipes(증거 기반 점검)

목적: “스모크/추정”이 아니라 **기존 산출물(JSON/YAML 등)을 실제로 읽어** proof 수준의 상태를 빠르게 확인한다.

- `/tf` 또는 `/tf list`: 사용 가능한 레시피 목록
- `/tf mod2-proof [tag]`: TwinPaper Module02(골든셋) 파이프라인 proof 점검(읽기 전용, **로컬 결정적 검사**)
  - 예(베이스라인): `/use O2` 후 `/tf mod2-proof phase-1`
  - 예(태그 미리보기): `/tf mod2-proof tags`
  - 예(최신 자동 선택): `/tf mod2-proof latest`
  - 예(phase1-dedup 결과 예시): `/tf mod2-proof phase-1keyrev4` (autolabel `n_auto_pending=83` 같은 지표 확인)
  - 하는 일:
    - `active_stream_lock.yaml`에서 `must_check`를 읽고 존재 여부(OK/MISSING) 점검
    - `contract-ci` 게이트(`status.overall_ok`)와 핵심 counts를 JSON에서 파싱해 요약
    - proof 기준에 따라 `proof_success|proof_retry|proof_fail` verdict 산출
    - 결과를 보고서로 저장: `docs/investigations_mo/projects/<O#>/tfs/TF-M2PROOF-<tag>/report.md`

### D. Todo/스케줄링(Mother-Orch)

- `/todo`: 프로젝트 todo 조회(서브명령 포함)
- `/todo proposals`: TF 실행 결과에서 올라온 후속 todo proposal inbox 조회
- `/todo accept <PROP-xxx|number>`: proposal을 main todo queue로 승격
- `/todo reject <PROP-xxx|number> [reason]`: proposal 폐기
- `/todo syncback [preview]`: runtime todo 상태를 canonical `TODO.md`에 반영한다. `done`은 체크박스 완료로 표시하고, runtime에서 새로 생긴 accepted proposal/manual todo는 append하며, blocked/manual_followup은 문서 하단 notes block으로 남긴다. `preview`를 붙이면 파일을 바꾸지 않고 계획만 보여준다.
- `/sync [replace] [all|O#|name] [since 3h] [quiet|-q|--quiet]`: 각 프로젝트의 `<project_root>/.aoe-team/AOE_TODO.md`를 읽어서 todo 큐에 반영(추가/업데이트/완료). 시나리오 파일에서는 체크박스(`- [ ] ...`)뿐 아니라 `- ...`, `1. ...` 같은 일반 리스트도 허용하며(우선순위 없으면 `P2`), 보통 `## Tasks` 섹션 아래에 모아두는 것을 권장한다. `since`를 주면 해당 시간 내에 수정된 시나리오만 반영.
  - 예외: `AOE_TODO.md`가 비어 있거나 템플릿 상태면 자동으로 프로젝트의 todo-ish 파일(`TODO.md` 등)과 최근 문서를 순서대로 스캔해 폴백한다.
  - `replace`를 붙이면 이번 sync source에 더 이상 없는 `sync-managed open todo`를 `canceled(sync_prune_missing)` 처리한다. 안전을 위해 `recent` 모드나 `since`가 붙은 부분 스캔에서는 차단된다.
  - 선택형 override: `<project>/.aoe-team/sync_policy.json`이 있으면 `exclude_globs`, `include_globs`, `class_confidence`, `doc_type_confidence`, `group_overrides`, `min_confidence`로 source 분류/신뢰도를 조정할 수 있다. 샘플: `templates/aoe-team/sync_policy.sample.json`
  - 편의 기능: 인자를 생략하면(`/sync`만 입력) **이전 `/sync ...`의 인자**를 재사용한다(채팅별).
  - shorthand: `since 1h` 대신 마지막에 `1h`처럼 써도 된다. 예: `/sync all 1h`
- `/sync preview [replace] [all|O#|name] [since 3h]`: queue를 바꾸지 않고 source 파일, candidate source class/confidence/doc-type, `would_add/update/done/prune`를 보여준다. plain `/sync` fallback은 최근 md 문서 + salvage 섹션 + TODO 파일을 합쳐 bootstrap 한다.
- `/sync bootstrap [all|O#|name] [since 24h]`: canonical `TODO.md`가 없거나 신뢰하기 어려울 때 recent docs + salvage를 우선으로 큐를 다시 시드(seed)한다. off-desk에서 backlog 복구용으로 쓰고, 필요하면 이후 `/sync preview` 또는 `/sync replace`로 canonical 경로를 다시 점검한다.
- `/sync recent [all|O#|name] [N] [since 3h] [quiet|-q|--quiet]`: 프로젝트 루트에서 **최근 문서 N개(기본 3)** 를 스캔해 todo 후보를 추출 후 큐에 반영. `since`를 주면 해당 시간 내에 수정된 문서만 후보로 본다.
- `/sync salvage [all|O#|name] [N] [since 3h] [quiet|-q|--quiet]`: 최근 문서를 더 넓게 훑어 `Next steps`, `남은 일`, `follow-up` 같은 섹션에서도 todo 후보를 복구한다. formal `AOE_TODO.md`나 TODO 파일을 못 만든 퇴근 후 bootstrap 용도. runnable 수준이면 main queue에 넣고, 너무 loose한 follow-up은 `/todo proposals` inbox로 보낸다.
- `/sync files [all|O#|name] [N] [since 3h] [quiet|-q|--quiet]`: 프로젝트 루트에서 파일명에 `todo|tasks|할일` 힌트가 있는 문서들만 스캔해 todo를 추출 후 큐에 반영. (프로젝트마다 TODO 파일 위치/형식이 제각각일 때 “제로 설정”으로 쓰기 좋다)
- `/queue`: (global) 전체 프로젝트의 todo 큐 요약
- `/queue followup`: `manual_followup` backlog가 있는 프로젝트만 요약
- `/next [force]`: (global) 다음 실행 가능한 todo를 선택해 dispatch 실행
- `/fanout [N] [force]`: (global) **프로젝트별로 1개씩** `/todo next`를 실행(순차 wave). 기본은 all(상한 50).
- `/drain [N] [force]`: `/next` 반복 실행(블로킹)
- `/auto [on|off|status] [fanout|next] [recent|no-recent] [since 12h] [maxfail=3]`: tmux 백그라운드 스케줄러로 `/next` 또는 `/fanout`을 주기 실행(논블로킹)
  - 상태 뷰: `/auto status short` 또는 `/auto status long`
  - 예: `/auto on` (기본: next), `/auto on fanout`
  - off-desk 권장: `/auto on fanout recent since 12h maxfail=3`
  - full-scope prune까지 원하면: `/auto on fanout recent replace-sync`
  - `recent`(idle prefetch): 큐가 비었을 때(=실행 가능한 open todo가 없을 때) 한 번씩 다음을 실행해 큐를 시드(seed)한 뒤 스케줄링을 계속한다. (best-effort, rate-limited)
    - `/sync files all since <since> quiet`
    - `/sync salvage all since <since> quiet`
  - `replace-sync`: idle prefetch를 `/sync replace all quiet`로 바꾼다. `replace`는 full-scope only라서 `since`는 무시된다.
  - 안전장치(알람 스팸 방지): 아래 상황에서는 auto가 자동으로 꺼진다.
    - confirm pending: `/ok` 또는 `/cancel`이 필요한 상태
    - stuck: `/next` 실행 후에도 동일 todo가 진전 없이 반복될 때(무한 루프 방지)
    - too many failures: 같은 todo가 연속으로 `blocked/failed`(또는 실행했는데 변화가 없음) 상태가 `maxfail`을 넘겼을 때
    - 재개: 원인 해소 후 `/auto on`으로 다시 켠다.
- `/offdesk [on|off|status|prepare|review]`: off-desk 프리셋(운영자 입력 최소화)
  - 상태 뷰: `/offdesk status short` 또는 `/offdesk status long`
  - `prepare`: 퇴근 전 preflight. 프로젝트별 runtime, canonical `TODO.md`, `AOE_TODO.md` include, queue(open/running/blocked/followup/proposals), last sync를 한 번에 점검한다.
  - `review`: `prepare` 결과 중 `warn/blocked` 프로젝트만 좁혀서 `syncback preview`, `todo proposals`, `todo followup`, `sync preview` 같은 즉시 조치 명령을 제안한다.
  - on: `report=short` + `routing=off` + `room=global` + `auto=fanout recent` (+ prefetch_since 기본 `12h`, env `AOE_OFFDESK_PREFETCH_SINCE`로 변경)
  - `replace-sync`를 붙이면 idle prefetch를 `/sync replace all quiet`로 바꾼다. 예: `/offdesk on replace-sync`
  - off: `auto off` + (가능하면) 이전 chat 설정 복원

pause/resume 동작 규칙:

- `/orch pause`된 프로젝트는 `/next`, `/fanout`, `/auto`, `/offdesk`에서 기본적으로 스킵된다.
- `force`를 주면(예: `/next force`, `/fanout force`) pause를 무시하고 포함한다.

시나리오 파일 포맷(프로젝트별, 경로 고정):

- 파일: `<project_root>/.aoe-team/AOE_TODO.md`
- (권장) 실제 Todo는 프로젝트 루트의 `TODO.md`에 쓰고, 시나리오 파일에서 포함(include) 시킬 수 있다:
  - `@include ../TODO.md`
- 예시:

```md
# AOE_TODO.md (scenario)

## Tasks

@include ../TODO.md

- [ ] 급한 이슈 처리
- P1: 오늘 안에 끝내기
- 리팩토링
1. 문서 업데이트
- [x] 완료된 항목(기존 TODO가 있을 때만 완료 처리됨)
```

### E. Room(에페메럴 게시판)

의미: 문서(`docs/`)나 todo discovery를 오염시키지 않는 **임시 채팅방/게시판 로그**. Discord 채널처럼 쓰되, 오래 쌓이지 않게 GC된다.

- 저장 위치: `.aoe-team/logs/rooms/<room>/<YYYY-MM-DD(.N)?>.jsonl`
- 기본 보관: 14일(`AOE_ROOM_RETENTION_DAYS=14`, `0`이면 GC 비활성)

명령:

- `/room` : 현재 room 상태
- `/room list` : room 목록
- `/room use <name>` : room 전환 (예: `global`, `O1`, `O1/TF-ALPHA`)
- `/room post <text>` : 현재 room에 글 남기기
- `/room tail [N]` : 최근 N개 보기(기본 20)

주의:

- room 로그는 **영구 문서가 아니다**. 장기 맥락은 TF `report.md`/registry로 남긴다.
- `AOE_ROOM_AUTOPUBLISH=1`이면 주요 TF 이벤트가 자동으로 room 로그에 남는다.
- room이 `global`일 때 자동 라우팅은 `AOE_ROOM_AUTOPUBLISH_ROUTE`로 제어한다(기본: `project`).

### F. Maintenance (GC)

- `/gc` : room 로그(기본 14일) + TF 실행 캐시(기본 72시간 TTL)를 정책에 따라 정리
- `/gc force` : room GC를 강제로 재실행(하루 1회 마커 무시)

### G. 복구/재실행

- `/retry <T-###|request_id> [lane <L#|R#,...>]`: 같은 입력으로 재실행. lane을 주면 critic이 허용한 실행/review lane만 다시 돎
- `/replan <T-###|request_id> [lane <L#|R#,...>]`: 플래너/크리틱을 다시 붙여 재계획 후 실행. lane을 주면 해당 lane만 범위를 좁힘
- `/replay [list|latest|<idx>|<id>|show <...>|purge]`: 핸들러 오류 입력 큐 조회/재실행/정리

---

## Telegram CLI-스타일 입력(`aoe ...`)은 무엇인가?

이 레포에는 로컬 바이너리 `aoe`가 추가로 생기는 것이 아니라, Telegram 메시지 파서가 아래 같은 형태를 “명령”으로 인식하는 옵션이 있다.

- 예: `aoe status`, `aoe queue`, `aoe todo list`, `aoe auto on`, `aoe room tail 20`

프로젝트(Orch) 레지스트리/다중 프로젝트 운영은 CLI-스타일에서 더 풍부하게 지원한다.

- `aoe orch list` (=`aoe map`)
- `aoe orch use <O#|name>`
- `aoe orch add <name> --path <project_root> [--overview <text>] [--init|--no-init] [--spawn|--no-spawn] [--set-active|--no-set-active]`
- `aoe orch pause <O#|name> [reason]`
- `aoe orch resume <O#|name>`
- `aoe orch status [--orch <O#|name>]`
- `aoe orch run [--orch <O#|name>] [--direct|--dispatch] [--roles <csv>] [--priority P1|P2|P3] [--timeout-sec N] [--no-wait] <prompt>`

구현 위치:

- 파서: `scripts/gateway/aoe_tg_parse.py` (`parse_cli_message`)

추가 동작:

- `aoe orch add ...` 실행 시 대상 프로젝트의 `<project_root>/.aoe-team/AOE_TODO.md`가 없으면 자동으로 생성한다.
- `/todo followup`: 현재 프로젝트의 `manual_followup` backlog만 표시
- `/todo proposals`: 현재 프로젝트의 TF follow-up proposal inbox 표시
- `/todo accept <PROP-xxx|number>`: proposal을 backlog에 승격하고 lineage(`proposal_id`, `created_from_request_id`, `created_from_todo_id`)를 남긴다.
- `/todo reject <PROP-xxx|number> [reason]`: proposal을 거절하고 inbox에서 닫는다.
- `/todo ack <TODO-xxx|number>`: blocked todo를 사람이 확인한 뒤 다시 `open`으로 되돌린다. `manual_followup`/blocked 메타도 함께 정리된다.
- `/todo ackrun <TODO-xxx|number>`: blocked todo를 확인 후 즉시 `open -> dispatch`로 이어서 다시 실행한다. 기본적으로 현재 프로젝트의 pending/running 충돌은 막고, 필요하면 `force`를 붙인다.
