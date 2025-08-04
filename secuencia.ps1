# sync_script.ps1
Start-Process -FilePath "python.exe" -ArgumentList ""C:\ruta\app2.py"" -NoNewWindow

# Esperar 10 segundos (o hasta que el puerto 8000 esté disponible)
Start-Sleep -Seconds 60

# Ejecutar App2 (segundo programa)
Start-Process -FilePath "python.exe" -ArgumentList "C:\ruta\app2.py" -NoNewWindow