# DAILY_CHECKLIST

## Daily Start (5 min)
1. `telegram_tmux.sh status` 확인
2. `ps -ef | rg 'aoe-telegram-gateway|aoe-orch worker'` 확인
3. `systemctl --user status aoe-telegram-stack.service` 확인
4. Telegram에서 `/whoami`, `/status`, `/monitor 5` 실행
5. 전일 실패 task 존재 시 `/task <label>` 확인

## During Operation
1. 신규 요청은 `/dispatch <요청>`로만 시작
2. 확인은 `/check <번호|label>` 또는 `/task <번호|label>` 사용
3. raw request_id 대신 `T-xxx` 또는 alias 우선 사용
4. `/monitor` 후 `/pick <번호>`로 포커스 지정
5. 재실행은 `/retry <번호|label>` 또는 `/replan <번호|label>` 사용
6. 게이트 차단 시 `E_GATE` 원인 먼저 해결

## Daily End (5 min)
1. 미완료 task 목록(`/monitor 10`) 정리
2. 실패 코드(`E_*`) 발생 건수 확인
3. 다음날 우선순위 task alias 정리
4. 운영 메모(이슈/복구 방법)를 docs에 반영

## Weekly Security Check (10 min)
1. 토큰 노출 의심 이벤트 점검
2. 필요 시 BotFather 토큰 회전
3. 회전 후 `/help`, `/kpi 24`, `/monitor 3` 재검증
