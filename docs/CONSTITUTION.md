# CONSTITUTION

## 0. Status
- Version: `v1.0.3`
- Ratified: `2026-02-26`
- Last amended: `2026-03-03`
- This document is the highest-level product and operating constitution for `aoe_orch_control`.
- Priority order: `CONSTITUTION` > `PROJECT_CHARTER` > `RUNBOOK` > implementation details.
- Scope: 이 문서는 "Mother-Orch(컨트롤 플레인)"와 "Project Orch(프로젝트 단위 오케스트레이터)"를 포함한 전체 운영체계를 규정한다.

## 1. Mission
- 목표는 `로컬 멀티에이전트 운영 자동화`와 `원격 운영 제어`를 하나의 일관된 시스템으로 구현하는 것이다.
- 사용자는 최소 입력으로 다수 에이전트를 통제하고, 시스템은 태스크 단위로 팀을 만들고 해체하며 Todo를 연속 처리해야 한다.

## 2. Core Principles (Immutable)
- `Task-first`: 모든 실행 단위는 태스크 기준으로 정의하고 추적한다.
- `TF isolation`: 실행 컨텍스트는 TF 단위로 격리하고 완료 시 반출/종료한다.
- `Human-readable state`: 식별자는 사람 친화적 별칭(`O1`, `T-###`, role label)으로 우선 노출한다.
- `Operator-minimal`: 운영자는 상위 오케스트레이터와만 상호작용하고 세부 fan-out은 자동화한다.
- `Remote parity`: 로컬에서 가능한 핵심 제어/모니터링은 Telegram에서도 동등하게 가능해야 한다.
- `Safety before speed`: 자동화는 빠르되 승인 경계와 정책 위반 방지 장치를 반드시 가진다.
- `Evidence-driven`: 모든 판단은 상태/로그/검증 결과로 추적 가능해야 한다.
- `Ephemeral vs Canonical`: 에이전트 간 잡담/중간 메모는 에페메럴 로그(예: room)로 분리하고, 장기 맥락은 TF `report.md`/registry 같은 canonical evidence로만 남긴다.

## 3. System Model
- `Mother-Orch` (control plane):
  - 다수 프로젝트 Orch를 관리하는 상위 스케줄러/감독자.
  - 큐 선택, 우선순위 변경, 승인, 재시도 정책, 보고 정책을 관리.
- `Project Orch` (project unit):
  - 하나의 프로젝트 컨텍스트와 Todo backlog를 소유.
  - 태스크별 TF 생성/종료, 결과 수합, 다음 Todo 진행.
- `TF (Task Force)` (task unit):
  - 태스크 전용 임시 팀.
  - Planner/Worker/Critic/Integrator 등 역할 세션으로 구성.
  - 완료 시 자동 종료하고 결과/증거만 상위로 귀속.
- (Non-normative) Runtime mapping (현재 구현 기준, 변경 가능):
  - Mother-Orch = `aoe-team-stack`/tmux 스택(게이트웨이+워커+오퍼레이터 세션)
  - Project Orch = Mother-Orch registry에 등록된 프로젝트 엔트리(`O1..`)
  - TF = 프로젝트별 태스크(`T-###`)를 수행하기 위해 생성되는 임시 세션/프로세스 묶음

## 4. Required Execution Lifecycle
- `Todo selected` -> `TF provisioned` -> `plan` -> `execute` -> `critic` -> `integrate` -> `close`.
- Critic이 목표 미달 판정을 내리면 TF는 종료하지 않고 재실행 루프로 진입한다.
- 기본 재실행 정책은 3회이며, Critic이 상향 에스컬레이션을 요청하면 Orch 또는 Mother-Orch가 개입한다.
- 태스크 완료 후에만 다음 Todo를 자동 시작한다.
- TF 종결 판정(verdict)은 최소 3종을 갖는다:
  - `success`: 목표 충족(완료)
  - `retry`: 목표 미달(재실행 필요)
  - `fail`: 운영자 개입이 필요한 실패(에스컬레이션)
- `success`는 "형식"이 아니라 **목표(Definition of Done) 충족**을 의미한다.
  - 예: 단순 구조 확인/리스크 트리아지(smoke) 목표라면 success는 "트리아지 완료"를 의미할 뿐, 파이프라인 통과(proof)까지를 증명하지 않는다.
  - proof 목표(예: contract-ci, phase1-dedup 산출물/게이트 확인)는 반드시 **근거 파일/요약 지표/게이트 결과**를 포함해야 하며, 없으면 success로 판정하지 않는다.

## 5. Local UX Contract (tmux/CLI)
- 빠른 세션 전환은 필수 기능이다.
- 우선 계약:
  - `Alt+1..9` 직접 전환.
  - 9개 초과 시 page(`next/prev`)로 이동.
  - 현재 세션/팀 상태를 상시 확인 가능한 보조 뷰 제공.
- 목적은 "전환 비용 최소화"이며, operator가 prefix 체인을 반복 입력하지 않게 한다.

## 6. Remote Control Contract (Telegram)
- Telegram은 단순 알림 채널이 아니라 원격 운영 콘솔이다.
- 최소 제공 기능:
  - Orch/TF 상태 조회, 실행/중단/재시도/재계획, 승인/거부, 우선순위 조정, 큐 진행 확인.
  - 예외/실패 입력에 대한 복구 경로(`/replay` 계열) 제공.
- 보고는 이벤트 기반이 기본이며 정책에 따라 요약 레벨 변경 가능해야 한다.
- 원격 제어는 혼동 방지를 위해 slash-first(예: `/status`)를 기본 UX로 삼되, 운영 편의상 평문 입력 라우팅도 지원한다.

## 7. Governance and Safety
- 권한 모델은 owner/admin/allow/readonly를 명시적으로 분리한다.
- 고위험 동작은 확인 단계를 요구한다.
- 정책 소스는 `AGENTS.md` 및 프로젝트 운영 정책 파일이며, 자동화는 이를 위반하지 않는다.
- 로컬 전체 권한 실행이 가능하더라도 감사 가능한 로그와 최소 필요 권한 원칙을 유지한다.
- (Break-glass) "오너 1인 원격 루트/무확인 실행"은 가능하더라도 기본값으로 전제하지 않는다:
  - owner-only + private DM 고정 + 감사 로그를 전제로 한다.
  - 토큰 노출/계정 탈취 시 피해가 RDP급임을 문서와 운영 절차로 상시 상기한다.
- (Data retention) 런타임 로그/캐시 정책은 디폴트로 “무한 축적”을 허용하지 않는다:
  - room 로그: 기본 14일 보관 후 GC (`AOE_ROOM_RETENTION_DAYS=14`)
  - TF 실행 캐시(worktree/run_dir): 성공 후 기본 72시간 핫 윈도우 후 GC (`AOE_TF_EXEC_CACHE_TTL_HOURS=72`)

## 8. Definition of Success
- 로컬:
  - 다중 세션 운영 시 전환/모니터링이 단축키 중심으로 즉시 동작.
- 자동 실행:
  - Todo 리스트를 태스크 단위 TF 생성/종료로 연속 처리.
- 원격:
  - Telegram에서 현재 상태, 실패 원인, 다음 액션을 즉시 파악/제어.
- 신뢰성:
  - 장애 발생 시 복구 경로와 상태 무결성 검증이 재현 가능.

## 9. Amendment Rule
- 본 문서 수정은 다음 3가지를 함께 남겨야 한다.
  - 변경 이유(문제 정의)
  - 기대 효과와 트레이드오프
  - 검증 방법(운영/테스트/관측 지표)
- 사소한 문구 수정이 아닌 동작 의미 변경은 `PROJECT_CHARTER`와 `RUNBOOK`의 관련 항목을 함께 갱신한다.

## 10. Amendment History
- `v1.0.0` (2026-02-26):
  - 최초 헌법 제정.
  - 멀티에이전트 전환 UX, TF 단위 운영, Mother-Orch/Telegram 원격 제어, 안전/거버넌스 원칙을 상위 규범으로 고정.
- `v1.0.1` (2026-02-27):
  - 변경 이유: 용어(Orch/TF/tmux) 해석이 혼동될 여지가 있어 문서 가독성과 정의를 강화.
  - 기대 효과/트레이드오프: 운영/설계 논의가 빨라짐. 대신 "현재 구현 기준" 매핑은 향후 변경 시 갱신 필요.
  - 검증 방법: RUNBOOK의 실제 실행 흐름(aoe-team-stack/tmux)과 문서의 용어 매핑이 일치하는지 점검.
- `v1.0.2` (2026-03-02):
  - 변경 이유: 에이전트 간 통신/메모가 장기 문서와 섞이면서 검색/동기화 오염과 저장소 팽창 위험이 발생.
  - 기대 효과/트레이드오프: 에페메럴 로그(room)와 canonical evidence(TF report/registry)를 분리해 장기 맥락 품질과 디스크 안정성을 확보. 대신 room은 기본적으로 “영구 보관”을 목표로 하지 않음.
  - 검증 방법: Telegram `/room` 사용 시 `.aoe-team/logs/rooms`에 jsonl이 쌓이고, 기본 14일 정책이 적용되는지 확인. TF 성공 실행 캐시가 72시간 이후 정리되는지 확인.
- `v1.0.3` (2026-03-03):
  - 변경 이유: TF verdict `success`가 "형식/스모크 통과"로 오해되어, 파이프라인 통과(proof) 수준의 목표 달성 여부가 혼동될 수 있음.
  - 기대 효과/트레이드오프: 목표(DoD) 기반 success 판정 원칙을 명확히 해 false success를 줄임. 대신 proof 목표는 더 많은 evidence 요구로 실행 비용이 증가할 수 있음.
  - 검증 방법: proof 목표 예시(TwinPaper Module02 contract-ci + phase1-dedup 산출물 확인)를 수행했을 때, evidence가 없으면 retry/fail로 떨어지고 evidence가 있으면 success로 판정되는지 확인.
