# PowerShell Script: Monitor System Resources During Load Test
# Monitors CPU, Memory, and Thread count during stream processing

param(
    [int]$IntervalSeconds = 5,
    [string]$OutputFile = ".\test_results\system_monitor_$(Get-Date -Format 'yyyyMMdd_HHmmss').csv"
)

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "System Resource Monitor" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Interval: $IntervalSeconds seconds" -ForegroundColor Yellow
Write-Host "Output: $OutputFile" -ForegroundColor Yellow
Write-Host "Press Ctrl+C to stop monitoring" -ForegroundColor Red
Write-Host "========================================`n" -ForegroundColor Cyan

# Create output directory if needed
$outputDir = Split-Path $OutputFile -Parent
if (-not (Test-Path $outputDir)) {
    New-Item -ItemType Directory -Path $outputDir | Out-Null
}

# Initialize CSV
$headers = "Timestamp,CPU_Percent,Memory_MB,Memory_Percent,FFmpeg_Processes,Java_Threads,Java_Memory_MB"
$headers | Out-File -FilePath $OutputFile -Encoding UTF8

# Display header
Write-Host "Time                CPU%    Memory(MB)  Mem%    FFmpeg  Java Threads  Java Mem(MB)" -ForegroundColor White
Write-Host "===================================================================================" -ForegroundColor Gray

try {
    while ($true) {
        $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
        
        # Get CPU usage
        $cpu = (Get-Counter '\Processor(_Total)\% Processor Time').CounterSamples.CookedValue
        
        # Get Memory usage
        $os = Get-CimInstance Win32_OperatingSystem
        $totalMemory = $os.TotalVisibleMemorySize / 1MB
        $freeMemory = $os.FreePhysicalMemory / 1MB
        $usedMemory = $totalMemory - $freeMemory
        $memoryPercent = ($usedMemory / $totalMemory) * 100
        
        # Count FFmpeg processes
        $ffmpegCount = (Get-Process -Name ffmpeg -ErrorAction SilentlyContinue).Count
        
        # Get Java process info
        $javaProc = Get-Process -Name java -ErrorAction SilentlyContinue | Select-Object -First 1
        $javaThreads = 0
        $javaMemory = 0
        
        if ($javaProc) {
            $javaThreads = $javaProc.Threads.Count
            $javaMemory = [Math]::Round($javaProc.WorkingSet64 / 1MB, 0)
        }
        
        # Format output
        $cpuStr = "{0,6:N1}" -f $cpu
        $memStr = "{0,10:N0}" -f $usedMemory
        $memPctStr = "{0,6:N1}" -f $memoryPercent
        $ffmpegStr = "{0,7}" -f $ffmpegCount
        $javaThreadStr = "{0,12}" -f $javaThreads
        $javaMemStr = "{0,13:N0}" -f $javaMemory
        
        # Color coding based on thresholds
        $cpuColor = if ($cpu -gt 80) { "Red" } elseif ($cpu -gt 60) { "Yellow" } else { "Green" }
        $memColor = if ($memoryPercent -gt 80) { "Red" } elseif ($memoryPercent -gt 60) { "Yellow" } else { "Green" }
        
        # Display
        Write-Host "$timestamp  " -NoNewline
        Write-Host "$cpuStr  " -NoNewline -ForegroundColor $cpuColor
        Write-Host "$memStr  " -NoNewline
        Write-Host "$memPctStr  " -NoNewline -ForegroundColor $memColor
        Write-Host "$ffmpegStr  " -NoNewline -ForegroundColor Cyan
        Write-Host "$javaThreadStr  " -NoNewline -ForegroundColor Magenta
        Write-Host "$javaMemStr" -ForegroundColor Blue
        
        # Save to CSV
        $csvLine = "$timestamp,$cpu,$usedMemory,$memoryPercent,$ffmpegCount,$javaThreads,$javaMemory"
        $csvLine | Out-File -FilePath $OutputFile -Append -Encoding UTF8
        
        Start-Sleep -Seconds $IntervalSeconds
    }
}
catch {
    Write-Host "`nMonitoring stopped." -ForegroundColor Yellow
}
finally {
    Write-Host "`nMonitoring data saved to: $OutputFile" -ForegroundColor Green
}