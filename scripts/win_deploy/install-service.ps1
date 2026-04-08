# =======================
# Настройки (меняй тут)
# =======================

$TaskName = "dc_worker"

# Путь к твоему скомпилированному файлу
$ExePath  = "C:/HDLNoCGEN/service.ps1"

# =======================
# Установка/обновление службы
# =======================

# Проверки
if (-not (Test-Path $ExePath)) { throw "EXE не найден: $ExePath" }

# Аргументы приложению
$appArgs = ""

# Удалим старую задачу, если была
schtasks /Query /TN $TaskName 2>$null
if ($LASTEXITCODE -eq 0) {
  schtasks /Delete /TN $TaskName /F
}

# Создадим задачу от SYSTEM, запуск при старте, highest
$TR = "powershell $ExePath $appArgs"
Write-Host "TR: $TR"
schtasks /Create /TN $TaskName /SC ONSTART /RU "SYSTEM" /RL HIGHEST /TR "$TR" /F

schtasks /Run /TN $TaskName


Write-Host "OK: Задача '$TaskName' создана."
