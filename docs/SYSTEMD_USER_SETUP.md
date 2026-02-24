# SYSTEMD_USER_SETUP

## Goal
- Reboot/login 이후에도 AOE Telegram stack를 자동 복구/유지한다.

## 1. Install
1. `bash /home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/scripts/systemd/install_user_services.sh`
2. 상태 확인:
- `systemctl --user status aoe-telegram-stack.service`
- `systemctl --user status aoe-telegram-heal.timer`

## 2. Keep Alive After Logout
- 사용자 로그아웃 이후에도 유지하려면 linger를 켠다:
- `sudo loginctl enable-linger kimyoungjin06`

## 3. Daily Ops
- 재시작: `systemctl --user restart aoe-telegram-stack.service`
- 정지: `systemctl --user stop aoe-telegram-stack.service`
- 시작: `systemctl --user start aoe-telegram-stack.service`
- 헬스 즉시 실행: `systemctl --user start aoe-telegram-heal.service`

## 4. Uninstall
- `bash /home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/scripts/systemd/uninstall_user_services.sh`

## 5. Unit Files
- `~/.config/systemd/user/aoe-telegram-stack.service`
- `~/.config/systemd/user/aoe-telegram-heal.service`
- `~/.config/systemd/user/aoe-telegram-heal.timer`

## 6. Notes
- stack 서비스는 `tmux` 세션(`aoe_tg_gateway`, `aoe_tg_worker_*`)을 관리한다.
- heal timer는 60초 주기로 health check를 수행하고, 비정상 시 `aoe-telegram-stack.service`를 재시작한다.
