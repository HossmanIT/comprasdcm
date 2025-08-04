# Ruta de los scripts Python
$transferScript = "C:\Mondayapp\myenv\transfer.py"
$syncScript = "C:\Mondayapp\myenv\sync_script.py"
$logFile = "C:\Logs\ejecucion.log"

# Función para escribir logs
function Write-Log {
    param ([string]$message)
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    "$timestamp - $message" | Out-File -FilePath $logFile -Append
    Write-Host "$timestamp - $message"
}

# Iniciar registro
Write-Log "==== Inicio de la ejecucion automatica ===="

# 1. Ejecutar transfer.py
try {
    Write-Log "Ejecutando transfer.py..."
    $transferProcess = Start-Process -FilePath "python.exe" -ArgumentList $transferScript -Wait -PassThru -NoNewWindow
    if ($transferProcess.ExitCode -eq 0) {
        Write-Log "transfer.py se ejecuto correctamente (ExitCode: 0)."
    } else {
        Write-Log "ERROR: transfer.py fallo (ExitCode: $($transferProcess.ExitCode))."
        exit 1
    }
} catch {
    Write-Log "ERROR al ejecutar transfer.py: $_"
    exit 1
}

# 2. Esperar 60 segundos antes de ejecutar sync_script.py
Write-Log "Esperando 60 segundos antes de ejecutar sync_script.py..."
Start-Sleep -Seconds 60

# 3. Ejecutar sync_script.py
try {
    Write-Log "Ejecutando sync_script.py..."
    $syncProcess = Start-Process -FilePath "python.exe" -ArgumentList $syncScript -Wait -PassThru -NoNewWindow
    if ($syncProcess.ExitCode -eq 0) {
        Write-Log "sync_script.py se ejecuto correctamente (ExitCode: 0)."
    } else {
        Write-Log "ERROR: sync_script.py fallo (ExitCode: $($syncProcess.ExitCode))."
        exit 1
    }
} catch {
    Write-Log "ERROR al ejecutar sync_script.py: $_"
    exit 1
}

Write-Log "==== Ejecución completada ===="