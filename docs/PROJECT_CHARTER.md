# PROJECT_CHARTER

## 1. Purpose
- 이 프로젝트의 목적은 `AOE 기반 멀티 에이전트 팀 운영`을 실무형 워크플로로 고도화하는 것이다.
- 사용자는 오직 Orchestrator와만 대화하고, 실행/검증은 역할별 Sub-session이 분담한다.
- 핵심 문제는 장기 대화에서 컨텍스트가 비대해지는 점이며, 이를 태스크 단위 세션 분리로 해결한다.

## 2. Objectives
- Telegram 자연어 인터페이스로 프로젝트별 Orch를 원격 제어한다.
- Orch는 계획, 배정, 수합, 완료 판단만 수행한다.
- DataEngineer/Reviewer 등 역할 세션은 실행과 검증을 분리 수행한다.
- 태스크 완료 시 서브세션은 종료하고 산출물/증거만 Orch에 귀속한다.

## 3. Non-Goals
- 단일 장수명 세션에 모든 작업을 누적하는 방식은 목표가 아니다.
- 메시징 편의보다 재현 가능성과 검증 가능성을 우선한다.
- 특정 벤더나 단일 LLM 제공자 종속 구조를 고정하지 않는다.

## 4. Success Criteria
- Telegram 명령 성공률 95% 이상을 유지한다.
- 태스크별 3단계(접수/실행/완료) 상태 추적 정확도 100%를 유지한다.
- verifier gate 활성 태스크는 검증 역할 완료 없이 `close=done`으로 끝나지 않는다.
- 장애 발생 시 10분 내 재기동 및 `/status`, `/monitor` 확인까지 복구한다.

## 5. Scope
- In scope:
- Telegram gateway, Orch task lifecycle, role-based worker dispatch, 모니터링/별칭 UX
- Out of scope:
- GUI 대시보드 대체, 외부 SaaS 의존 강제, 업스트림 전체 기능 재설계

## 6. Operating Model
- 사용자 -> Telegram -> Orch
- Orch -> Sub-session dispatch (필요시에만 fan-out)
- Sub-session -> 결과/근거 회신
- Orch -> 통합 응답 + 상태 종료

## 7. Decision Records
- 주요 구조 변경은 `docs/` 아래 ADR 또는 changelog 형태로 남긴다.
- 포크 추적/라이선스/업스트림 기준은 `FORK_POLICY`, `UPSTREAM_BASELINE`에서 관리한다.
