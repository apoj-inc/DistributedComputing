#!/usr/bin/env bash
set -euo pipefail

# ====== НАСТРОЙКИ (поменяйте под себя) ======
SERVICE_NAME="DC_master"
APP_USER="distribtedcomputing"
APP_GROUP="distribtedcomputing"

# Откуда взять бинарник (исходный путь) и куда установить
SRC_BINARY="${1:-}"                 # можно передать первым аргументом скрипта
DEST_DIR="/opt/distribtedcomputing"
DEST_BINARY="${DEST_DIR}/bin"

# Параметры запуска (поменяйте под себя)
APP_ARGS=("-e ./master.env")

# Рабочая директория
WORKDIR="${DEST_DIR}"

# Unit-файл
UNIT_PATH="/etc/systemd/system/${SERVICE_NAME}.service"
# ===========================================

if [[ $EUID -ne 0 ]]; then
  echo "Запускайте через sudo/root."
  exit 1
fi

if [[ -z "${SRC_BINARY}" ]]; then
  echo "Использование: sudo $0 /path/to/my_binary"
  exit 1
fi

if [[ ! -f "${SRC_BINARY}" ]]; then
  echo "Файл не найден: ${SRC_BINARY}"
  exit 1
fi

echo "[1/6] Создаю пользователя/группу (если нужно): ${APP_USER}"
if ! id -u "${APP_USER}" >/dev/null 2>&1; then
  useradd -r -s /usr/sbin/nologin -d "${DEST_DIR}" "${APP_USER}"
fi
# группа обычно создаётся вместе с пользователем; на всякий:
getent group "${APP_GROUP}" >/dev/null 2>&1 || groupadd -r "${APP_GROUP}" || true

echo "[2/6] Создаю директорию установки: ${DEST_DIR}"
mkdir -p "${DEST_DIR}"

echo "[3/6] Копирую бинарник и env -> ${DEST_BINARY}"
install -m 0755 "${SRC_BINARY}" "${DEST_BINARY}"
install -m 0755 "${SRC_BINARY}/configs/master.env  " "${DEST_BINARY}"

echo "[4/6] Выставляю владельца директорий"
chown -R "${APP_USER}:${APP_GROUP}" "${DEST_DIR}"

# Соберём строку ExecStart безопасно (экранируем аргументы)
EXEC_START="${DEST_BINARY}"
for a in "${APP_ARGS[@]}"; do
  # printf %q делает shell-экранирование
  EXEC_START+=" $(printf '%q' "$a")"
done

echo "[5/6] Пишу unit-файл: ${UNIT_PATH}"
cat > "${UNIT_PATH}" <<EOF
[Unit]
Description=${SERVICE_NAME} service (keep alive)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=${APP_USER}
Group=${APP_GROUP}
WorkingDirectory=${WORKDIR}
ExecStart=${EXEC_START}
Restart=always
RestartSec=3
KillMode=control-group
TimeoutStopSec=15
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF

echo "[6/6] Перезагружаю systemd, включаю и запускаю службу"
systemctl daemon-reload
systemctl enable --now "${SERVICE_NAME}.service"

echo
echo "Готово."
echo "Статус: systemctl status ${SERVICE_NAME}.service"
echo "Логи:   journalctl -u ${SERVICE_NAME}.service -f"
