# PowerShell Script: Publish 50 Concurrent RTSP Streams to MediaMTX
# This script creates 50 FFmpeg processes publishing to rtsp://127.0.0.1:8554/v1 to v50

param(
    [int]$StreamCount = 50,
    [string]$VideoPath = "C:\Users\pc\Videos\Captures\VALORANT   2025-04-30 23-27-59.mp4",
    [string]$MediaMTXHost = "127.0.0.1",
    [int]$MediaMTXPort = 8554
)

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "RTSP Stream Publisher" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Stream Count: $StreamCount" -ForegroundColor Yellow
Write-Host "Video File: $VideoPath" -ForegroundColor Yellow
Write-Host "MediaMTX: rtsp://${MediaMTXHost}:${MediaMTXPort}" -ForegroundColor Yellow
Write-Host "========================================`n" -ForegroundColor Cyan

# Validate video file exists
if (-not (Test-Path $VideoPath)) {
    Write-Host "ERROR: Video file not found: $VideoPath" -ForegroundColor Red
    exit 1
}

# Clean up any existing ffmpeg processes
Write-Host "Cleaning up existing FFmpeg processes..." -ForegroundColor Yellow
Get-Process ffmpeg -ErrorAction SilentlyContinue | Stop-Process -Force
Start-Sleep -Seconds 2

# Create logs directory
$logDir = ".\stream_logs"
if (-not (Test-Path $logDir)) {
    New-Item -ItemType Directory -Path $logDir | Out-Null
}

Write-Host "Starting $StreamCount streams..." -ForegroundColor Green
Write-Host "This will take a few moments...`n" -ForegroundColor Gray

# Array to store jobs
$jobs = @()

# Start publishing streams
for ($i = 1; $i -le $StreamCount; $i++) {
    $streamName = "v$i"
    $rtspUrl = "rtsp://${MediaMTXHost}:${MediaMTXPort}/${streamName}"
    $logFile = "$logDir\stream_$i.log"
    
    Write-Host "[$i/$StreamCount] Starting stream: $streamName" -ForegroundColor Cyan
    
    # Create FFmpeg command with optimized settings
    $ffmpegArgs = @(
        "-stream_loop", "-1",
        "-re",
        "-i", "`"$VideoPath`"",
        "-f", "lavfi",
        "-i", "anullsrc=channel_layout=stereo:sample_rate=44100",
        "-map", "0:v",
        "-map", "1:a",
        "-vf", "fps=25,scale=960:540",
        "-pix_fmt", "yuv420p",
        "-c:v", "libx264",
        "-profile:v", "baseline",
        "-level", "3.0",
        "-g", "50",
        "-keyint_min", "50",
        "-sc_threshold", "0",
        "-b:v", "800k",
        "-maxrate", "800k",
        "-bufsize", "1600k",
        "-c:a", "aac",
        "-b:a", "128k",
        "-ar", "44100",
        "-ac", "2",
        "-rtsp_transport", "tcp",
        "-fflags", "+genpts",
        "-use_wallclock_as_timestamps", "1",
        "-f", "rtsp",
        "$rtspUrl"
    )
    
    # Start FFmpeg as background job
    $job = Start-Job -ScriptBlock {
        param($ffmpegArgs, $logFile)
        $processInfo = New-Object System.Diagnostics.ProcessStartInfo
        $processInfo.FileName = "ffmpeg"
        $processInfo.Arguments = $ffmpegArgs -join " "
        $processInfo.RedirectStandardOutput = $true
        $processInfo.RedirectStandardError = $true
        $processInfo.UseShellExecute = $false
        $processInfo.CreateNoWindow = $true
        
        $process = New-Object System.Diagnostics.Process
        $process.StartInfo = $processInfo
        $process.Start() | Out-Null
        
        # Log output
        $output = $process.StandardError.ReadToEnd()
        $output | Out-File -FilePath $logFile
        
        $process.WaitForExit()
    } -ArgumentList ($ffmpegArgs -join " "), $logFile
    
    $jobs += $job
    
    # Small delay to avoid overwhelming the system
    Start-Sleep -Milliseconds 200
}

Write-Host "`n========================================" -ForegroundColor Green
Write-Host "All $StreamCount streams started!" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green
Write-Host ""
Write-Host "Stream URLs:" -ForegroundColor Yellow
Write-Host "  rtsp://${MediaMTXHost}:${MediaMTXPort}/v1" -ForegroundColor Gray
Write-Host "  rtsp://${MediaMTXHost}:${MediaMTXPort}/v2" -ForegroundColor Gray
Write-Host "  ..." -ForegroundColor Gray
Write-Host "  rtsp://${MediaMTXHost}:${MediaMTXPort}/v${StreamCount}" -ForegroundColor Gray
Write-Host ""
Write-Host "Logs directory: $logDir" -ForegroundColor Yellow
Write-Host ""
Write-Host "Press Ctrl+C to stop all streams" -ForegroundColor Red
Write-Host ""

# Wait for user interrupt
try {
    Write-Host "Streams are running... (monitoring)" -ForegroundColor Cyan
    while ($true) {
        Start-Sleep -Seconds 5
        $runningJobs = ($jobs | Where-Object { $_.State -eq 'Running' }).Count
        Write-Host "[$(Get-Date -Format 'HH:mm:ss')] Active streams: $runningJobs/$StreamCount" -ForegroundColor Gray
        
        if ($runningJobs -eq 0) {
            Write-Host "All streams have stopped!" -ForegroundColor Red
            break
        }
    }
}
finally {
    Write-Host "`nStopping all streams..." -ForegroundColor Yellow
    $jobs | Stop-Job
    $jobs | Remove-Job -Force
    Get-Process ffmpeg -ErrorAction SilentlyContinue | Stop-Process -Force
    Write-Host "All streams stopped." -ForegroundColor Green
}