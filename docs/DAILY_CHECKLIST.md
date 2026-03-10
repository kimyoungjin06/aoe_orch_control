# DAILY_CHECKLIST

## Daily Start (5 min)
1. (최초 1회) `scripts/team/install_global_cli.sh` 실행
1. `telegram_tmux.sh status` 확인
1. `telegram_tmux.sh ui` 실행 (Alt+1..9 또는 Prefix(C-b)+1..9 전환, Alt+,/Alt+. 페이지 이동, 상단 세션맵 갱신)
2. `ps -ef | rg 'aoe-telegram-gateway|aoe-orch worker'` 확인
3. `systemctl --user status aoe-telegram-stack.service` 확인
4. Telegram에서 `/whoami`, `/status`, `/map`, `/monitor 5` 실행
5. 전일 실패 task 존재 시 `/task <label>` 확인
6. `docs/investigations_mo/registry/project_lock.yaml`에서 `active_project`, `active_tf`가 현재 운용 대상과 일치하는지 확인

## During Operation
1. 신규 요청은 `/dispatch <요청>`로만 시작
2. 확인은 `/check <번호|label>` 또는 `/task <번호|label>` 사용
3. raw request_id 대신 `T-xxx` 또는 alias 우선 사용
4. `/monitor` 후 `/pick <번호>`로 포커스 지정
5. 재실행은 `/retry <번호|label>` 또는 `/replan <번호|label>` 사용
6. 게이트 차단 시 `E_GATE` 원인 먼저 해결
7. lineage 발생(`retry/replan`) 시 `docs/investigations_mo/registry/handoff_index.csv`에 행이 추가되는지 확인

## Daily End (5 min)
1. 미완료 task 목록(`/monitor 10`) 정리
2. 실패 코드(`E_*`) 발생 건수 확인
3. 다음날 우선순위 task alias 정리
4. 운영 메모(이슈/복구 방법)를 docs에 반영
5. `docs/investigations_mo/registry/tf_registry.md`의 status가 실제 작업 상태와 일치하는지 확인

## Weekly Security Check (10 min)
1. 토큰 노출 의심 이벤트 점검
2. 필요 시 BotFather 토큰 회전
3. 회전 후 `/help`, `/kpi 24`, `/monitor 3` 재검증
4. `.aoe-team/telegram.env`에서 `TELEGRAM_OWNER_CHAT_ID`, `AOE_OWNER_ONLY=1` 유지 확인
