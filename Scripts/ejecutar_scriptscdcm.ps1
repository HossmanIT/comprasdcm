# Ruta de los scripts Python
$transferScript = "C:\Mondayapp\comprasdcm\transfercdcm.py"
$syncScript = "C:\Mondayapp\comprasdcm\sync_scriptcdcm.py"
$logFile = "C:\Logs\comprascdcm.log"
$transferOut = "C:\Logs\transfercdcm_salida.log"
$transferErr = "C:\Logs\transfercdcm_error.log"
$syncOut = "C:\Logs\synccdcm_salida.log"
$syncErr = "C:\Logs\synccdcm_error.log"

# Ruta completa a python.exe (ajusta si usas entorno virtual)
$pythonPath = "python.exe"

# Funcion para escribir logs
function Write-Log {
    param ([string]$message)
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    "$timestamp - $message" | Out-File -FilePath $logFile -Append
    Write-Host "$timestamp - $message"
}

# Iniciar registro
Write-Log "==== Inicio de la ejecucion automatica ===="

# 1. Ejecutar transfercdcm.py
try {
    Write-Log "Ejecutando transfercdcm.py..."
    $transferProcess = Start-Process -FilePath $pythonPath `
        -ArgumentList $transferScript `
        -RedirectStandardOutput $transferOut `
        -RedirectStandardError $transferErr `
        -Wait -PassThru -NoNewWindow

    if ($transferProcess.ExitCode -eq 0) {
        Write-Log "transfercdcm.py se ejecuto correctamente (ExitCode: 0)."
    } else {
        Write-Log "ERROR: transfercdcm.py fallo (ExitCode: $($transferProcess.ExitCode))."
        exit 1
    }
} catch {
    Write-Log "ERROR al ejecutar transfercdcm.py: $_"
    exit 1
}

# 2. Esperar 60 segundos antes de ejecutar sync_scriptcdcm.py
Write-Log "Esperando 60 segundos antes de ejecutar sync_scriptcdcm.py..."
Start-Sleep -Seconds 60

# 3. Ejecutar sync_scriptcdcm.py
try {
    Write-Log "Ejecutando sync_scriptcdcm.py..."
    $syncProcess = Start-Process -FilePath $pythonPath `
        -ArgumentList $syncScript `
        -RedirectStandardOutput $syncOut `
        -RedirectStandardError $syncErr `
        -Wait -PassThru -NoNewWindow

    if ($syncProcess.ExitCode -eq 0) {
        Write-Log "sync_scriptcdcm.py se ejecuto correctamente (ExitCode: 0)."
    } else {
        Write-Log "ERROR: sync_scriptcdcm.py fallo (ExitCode: $($syncProcess.ExitCode))."
        exit 1
    }
} catch {
    Write-Log "ERROR al ejecutar sync_scriptcdcm.py: $_"
    exit 1
}

Write-Log "==== Ejecucion completada ===="
