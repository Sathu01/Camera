# PowerShell Script: Stop All Streams and Clean Up
# Stops all FFmpeg processes and cleans HLS output

param(
    [int]$StreamCount = 50,
    [string]$ApiBaseUrl = "http://localhost:8080"
)

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Stop All Streams & Cleanup" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

# Stop FFmpeg processes
Write-Host "`nStopping FFmpeg processes..." -ForegroundColor Yellow
$ffmpegProcs = Get-Process -Name ffmpeg -ErrorAction SilentlyContinue
if ($ffmpegProcs) {
    $ffmpegProcs | Stop-Process -Force
    Write-Host "✓ Stopped $($ffmpegProcs.Count) FFmpeg processes" -ForegroundColor Green
} else {
    Write-Host "✓ No FFmpeg processes found" -ForegroundColor Green
}

# Stop HLS streams via API
Write-Host "`nStopping HLS streams via API..." -ForegroundColor Yellow
$stopCount = 0
$failCount = 0

for ($i = 1; $i -le $StreamCount; $i++) {
    $streamName = "cam_$i"
    
    try {
        $response = Invoke-RestMethod `
            -Uri "$ApiBaseUrl/api/stream/hls/stop/$streamName" `
            -Method POST `
            -TimeoutSec 5 `
            -ErrorAction Stop
        
        $stopCount++
        Write-Host "  ✓ Stopped: $streamName" -ForegroundColor Gray
    } catch {
        $failCount++
        # Don't show errors for streams that weren't running
    }
    
    Start-Sleep -Milliseconds 50
}

Write-Host "✓ API stop commands sent: $stopCount succeeded, $failCount not running" -ForegroundColor Green

# Clean up HLS output directory
Write-Host "`nCleaning HLS output directory..." -ForegroundColor Yellow
$hlsDir = ".\hls"
if (Test-Path $hlsDir) {
    Remove-Item -Path $hlsDir -Recurse -Force
    Write-Host "✓ HLS directory cleaned" -ForegroundColor Green
} else {
    Write-Host "✓ HLS directory not found (already clean)" -ForegroundColor Green
}

# Clean up logs
Write-Host "`nCleaning stream logs..." -ForegroundColor Yellow
$logDir = ".\stream_logs"
if (Test-Path $logDir) {
    Remove-Item -Path $logDir -Recurse -Force
    Write-Host "✓ Stream logs cleaned" -ForegroundColor Green
} else {
    Write-Host "✓ No stream logs found" -ForegroundColor Green
}

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "Cleanup Complete!" -ForegroundColor Green
Write-Host "========================================`n" -ForegroundColor Cyan