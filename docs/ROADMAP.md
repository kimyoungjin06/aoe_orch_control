# ROADMAP

## 1. Purpose
- 본 문서는 `docs/CONSTITUTION.md` 조항을 구현 항목으로 분해한 실행 체크리스트다.
- 운영 우선순위는 `안정성 -> 자동화 -> 자율성` 순서로 적용한다.

## 2. Status Legend
- `DONE`: 운영에 사용 중이며 기본 검증 완료
- `IN_PROGRESS`: 부분 구현 완료, 정책/자동화 추가 필요
- `PLANNED`: 설계 합의만 완료, 구현 미착수

## 3. Constitution Trace Matrix
| Clause | Requirement | Status | Evidence | Next Action |
|---|---|---|---|---|
| C2-Task-first | 태스크 단위 추적/상태 확인 | DONE | `/monitor`, `/check`, `/task`, `T-xxx` alias | task 의존성(선행/후행) 추가 |
| C2-TF-isolation | 태스크별 TF 생성/종료 + 캐시/반출 정책 | DONE | `AOE_TF_EXEC_MODE=worktree`, `.aoe-team/tf_exec_map.json`, `.aoe-team/tf_runs/`, 성공만 보존(실패 시 자동 정리) + TTL GC(`AOE_TF_EXEC_CACHE_TTL_HOURS=72`) | TTL 정책 운영값 튜닝(디스크/속도) |
| C2-Human-readable | 사람 친화 별칭 우선 | DONE | `O1..` project alias, `T-xxx` task alias | 별칭 충돌 자동 복구 정책 추가 |
| C2-Operator-minimal | 상위 Orch 중심 운영 | IN_PROGRESS | Telegram slash 중심 제어 | Mother-Orch가 다중 Orch 큐 직접 스케줄링 |
| C2-Remote-parity | 로컬/원격 제어 동등성 | IN_PROGRESS | Telegram 운영 명령군 + replay | tmux page/hintbar 상태를 Telegram 요약에 반영 |
| C2-Safety | 고위험 확인/권한 경계 | DONE | `/ok`, ACL, owner mode, lockme, replay read/write auth split | owner/admin 정책 감사 자동화 |
| C2-Evidence | 로그/증거 기반 추적 | IN_PROGRESS | `gateway_events.jsonl`, `/kpi`, poll_state | TF 결과물 링크 표준 스키마 확정 |
| C2-Ephemeral-vs-Canonical | 에페메럴 로그(room)와 canonical evidence 분리 | DONE | `/room` + `.aoe-team/logs/rooms` (jsonl) + 기본 보관 14일(`AOE_ROOM_RETENTION_DAYS=14`) | TF 주요 이벤트 auto-post(옵션) 검토 |
| C4-Lifecycle | plan->execute->critic->integrate 루프 | IN_PROGRESS | retry/replan/cancel 기반 루프 | Critic fail 시 재실행/에스컬레이션 자동 전이 |
| C5-Local-UX | Alt 숫자 즉시 전환/페이지 | DONE | `telegram_tmux.sh ui/page/refresh` + 2-line status hint bar + tmux hooks(auto refresh) | refresh 속도 최적화(aoe meta cache/디바운스) |
| C6-Remote-control | Telegram 운영 콘솔화 | IN_PROGRESS | `/status` `/monitor` `/map` `/replay` | 승인/거부/우선순위 변경 명령을 워크플로에 결합 |
| C7-Governance | 정책 준수 + 감사 가능성 | IN_PROGRESS | `CONSTITUTION`, `CHARTER`, `RUNBOOK` | 정책 위반 시 자동 차단/감사 이벤트 강화 |
| C8-Success-metrics | 성공 기준 수치화 | PLANNED | charter 성공 기준 정의 | KPI 대시보드 텍스트 리포트 정규화 |

## 4. Execution Phases
### Phase A: Operational Hardening
- [x] replay 권한 분리(list/show vs run/purge)
- [x] 실패 큐 TTL 도입(`AOE_GATEWAY_FAILED_TTL_HOURS`)
- [x] 상태 요약 강화(`failed_queue_total`, `last_failed_at`, `active_tf_count`)
- [x] 헬스체크 실패 원인 코드화(`telegram_tmux.sh health` -> `E_HEALTH_*`)
- [x] tmux side panel 제거(복사/붙여넣기 방해 요소 제거) + 2-line status hint bar 고정

### Phase B: TF Automation
- [x] 멀티에이전트 TF 문서 템플릿/스캐폴드 구축(`docs/templates/multi_agent_tf_ops_template`, `docs/investigations_mo`)
- [x] 태스크 접수 시 TF 템플릿 자동 생성 (multi-project scaffold sync)
- TF 종료 조건(성공/실패/재시도) 상태머신 고정
- [x] TF 종료 시 반출/아카이브(기본) 자동화 (`tf_close_index.csv`, handoff placeholder fill, `archive/close_summary.md`)
- 결과/근거 반출 포맷 표준화
- [x] TF 실행 캐시(worktree/run_dir) TTL GC (`AOE_TF_EXEC_CACHE_TTL_HOURS=72`, `0` disables)

### Phase B2: Ephemeral Agent Comms
- [x] room(에페메럴 게시판) `/room` + jsonl 저장 + 기본 14일 GC (`AOE_ROOM_RETENTION_DAYS=14`, `0` disables)

### Phase C: Mother-Orch Scheduling
- 다중 Orch todo 큐 스케줄러
- 우선순위/윈도우 기반 실행 정책
- 에스컬레이션 정책(자동 vs 승인지점) 분리
- [x] 프로젝트 Todo 큐 최소 구현 (`/todo add|done|list|next`)
- [x] Mother-Orch global next (`/next`)

### Phase D: Autonomous Recovery
- 예측 불가 상황 분류(환경/권한/의존성/모델오류)
- 분류별 자동 대응 플레이북
- 대응 실패 시 즉시 operator 보고/승인 루프

## 5. Near-term Sprint (Next 2 cycles)
- [x] replay 권한 분리 완성
- [x] failed queue TTL + purge 정책 자동화
- [x] Critic fail 상태를 기준으로 재실행 3회 + 에스컬레이션 훅 구현(`--exec-critic`, `--exec-critic-retry-max`)
- [x] Telegram 보고 레벨(짧게/보통/상세) 스위치 추가(`/report`)

## 6. Current Orch Bug Bundle
- [x] planning 시작 직후 provisional task에 `phase1_*` 메타와 candidate roles를 즉시 반영
- [x] `planning_planner` / `planning_critic` 이벤트에 `project_key`, `request_id`, `task_short_id`를 항상 싣기
- [x] writer/reporting 성격 평문을 `orch-monitor`가 아니라 Mother-Orch `dispatch_task`로 우선 분류

## 7. Current Provider Capacity Handling
- [x] Claude rate limit 시 Codex fallback
- [x] Codex rate limit 시 Claude fallback
- [x] 양쪽 provider 모두 rate limit이면 task를 `tf_phase=rate_limited`로 유지
- [x] `retry_after_sec`와 함께 absolute `retry_at` 저장
- [x] `rate_limited` task가 전체 queue를 영구 busy로 막지 않도록 parked 상태로 분리
- [ ] `retry_at` 도래 시 scheduler/offdesk 자동 resume
