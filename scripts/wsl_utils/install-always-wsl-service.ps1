# =======================
# Настройки (меняй тут)
# =======================

$TaskName = "wsl_dont_go_down"

# Путь к твоему скомпилированному файлу
$ExePath  = "C:/Users/HDLNoCGen/StartWSL.vbs"

# =======================
# Установка/обновление службы
# =======================

# Аргументы приложению
$appArgs = ""

# Удалим старую задачу, если была
schtasks /Query /TN $TaskName 2>$null
if ($LASTEXITCODE -eq 0) {
  schtasks /Delete /TN $TaskName /F
}

# Создадим задачу от SYSTEM, запуск при старте, highest
$TR = "$ExePath $appArgs"
Write-Host "TR: $TR"
schtasks /Create /TN $TaskName /SC ONSTART /RU "HDLNoCGen" /RP "HDLNoCGen" /TR "$TR" /F

schtasks /Run /TN $TaskName


Write-Host "OK: Задача '$TaskName' создана."
