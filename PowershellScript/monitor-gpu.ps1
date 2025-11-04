while ($true) {
    Clear-Host
    Write-Host "=== GPU Monitoring ===" -ForegroundColor Green
    nvidia-smi --query-gpu=timestamp,utilization.gpu,utilization.memory,memory.used,memory.total --format=csv
    Start-Sleep -Seconds 2
}