$TaskName = "dc_worker"

# Путь к твоему скомпилированному файлу
$ExePath  = "./dc_worker.exe"

# Рабочая папка (обычно папка, где лежит exe)
$WorkDir  = "C:/HDLNoCGEN"

# Аргументы приложению
$appArgs = "-e worker.env"

$script = "cd $WorkDir; $ExePath $appArgs;"

powershell "$script"

