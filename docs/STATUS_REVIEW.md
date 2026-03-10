# STATUS_REVIEW

본 문서는 `docs/CONSTITUTION.md` 관점에서 현재 개발 상태를 비판적으로 점검하고, 다음 스프린트 우선순위를 정리한다.

## 1. 방향성(Alignment)
- 목표(로컬 멀티에이전트 운영 자동화 + 원격 운영 제어)는 일관적이다.
- "Operator는 Orch만 상대"하고, 실행/검증은 하위 역할 세션이 담당한다는 모델도 유지되고 있다.
- Slash-first 원칙(혼동 방지)과 평문 라우팅(/mode, /dispatch, /direct)은 실제 운영 UX 요구와 부합한다.

## 2. 장점(Pros)
- **운영 난이도 감소**: `/pick`(최근 목록 버튼), `/todo`(백로그), `/monitor` 등으로 최소 입력 흐름이 만들어졌다.
- **사람 친화적 매핑**: `O1..` 프로젝트 별칭, 최근 태스크/선택 태스크 흐름으로 raw id 의존이 줄었다.
- **안전장치 기반 원격제어**: owner-only/deny-by-default/ACL, replay read/write 분리 등 "원격 콘솔"로서 최소한의 거버넌스가 있다.
- **실행 품질 루프**: exec-critic 기반 `success/retry/fail` 분기와 재시도 상한(기본 3회)이 자동화에 적합하다.

## 3. 단점/리스크(Cons & Risks)
- **보안 리스크(가장 큼)**: Telegram 토큰/계정 탈취는 사실상 "원격 쉘(RDP급)" 사고로 직결된다.
- **구성 복잡도**: env/systemd/tmux/상태파일이 얽혀 있어 초보자 온보딩이 어렵다(문서/자동화로 완화 필요).
- **상태 스키마 일관성**: `todo`와 `task`의 linkage/메타 필드가 늘어나는 중이며, 재시작 시 정규화 과정에서 일부 메타가 손실될 여지가 있다(필드 보존 정책 필요).
- **단일 파일 비대화**: 게이트웨이 본체(`aoe-telegram-gateway.py`)가 커지고 있어 회귀 위험/수정 비용이 증가한다(모듈 분리/테스트 강화 필요).

## 4. 보안 결론(Owner-only 1:1 전제)
- 이 봇은 "나만 쓰는 1:1 운영 콘솔"로 정의하는 것이 맞다.
- 운영 기본값 권장:
  - `AOE_DENY_BY_DEFAULT=1`, `AOE_OWNER_ONLY=1`, `TELEGRAM_OWNER_CHAT_ID` 고정
  - Telegram은 **private DM만 사용**(그룹/채널 금지)
  - 토큰은 파일 권한(600)으로 보호하고, 노출 시 즉시 회전
  - 감사 로그(`gateway_events.jsonl`)는 항상 남기고, 원격 실행은 로그 기반으로 추적 가능해야 함
- 주의:
  - "무확인(full auto) + root 권한"은 편의성은 높지만 사고 영향이 매우 커서 기본 전제로 삼지 않는다.
  - 필요하다면 break-glass(수동으로 잠깐 켜고 끄는) 형태로만 제한적으로 고려한다.

## 5. 현재 개발 완료도(요약)
- 완료(DONE):
  - `/pick` no-arg 메뉴 + 1..9 빠른 선택
  - `/todo` CRUD 최소 기능 + `/todo next`(open todo를 dispatch 실행으로 연결)
  - TF close 산출물 반출(인덱스/placeholder fill/close summary)
  - tmux 전환 단축키/패널(상시 세션 리스트) 기본 UX
- 진행 중(IN_PROGRESS):
  - Mother-Orch 스케줄링(다중 Orch의 todo를 자동 순회 실행)
  - TF 템플릿 자동 생성(태스크 접수 시 생성/압축/아카이브 정책)
  - 상태 스키마 안정화(재시작 시 메타 보존/마이그레이션)

## 6. 다음 우선순위(제안)
1. Mother-Orch "전체 프로젝트 todo next" 최소 스케줄러: 한 번의 명령으로 가장 우선인 todo를 실행하고 이벤트로 보고.
2. TF 템플릿 자동 생성: 태스크 생성 시 `docs/investigations_mo` 템플릿을 자동으로 만들고 종료 시 압축/아카이브.
3. 코드 분리 + 테스트: 게이트웨이/핸들러/상태 정규화 모듈 분리, 핵심 경로(권한/모드/투두/전환)에 스모크 테스트 추가.

