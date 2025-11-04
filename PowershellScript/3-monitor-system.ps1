param(
    [int]$IntervalSeconds = 5,
    [string]$OutputFile = ""
)

# If $PSScriptRoot is empty (running in interactive console), fall back to current directory
if (-not $PSScriptRoot) { $scriptRoot = (Get-Location).Path } else { $scriptRoot = $PSScriptRoot }

# Set default output file
if ([string]::IsNullOrEmpty($OutputFile)) {
    $timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
    $resultsDir = Join-Path -Path $scriptRoot -ChildPath "test_results"
    if (-not (Test-Path $resultsDir)) {
        New-Item -ItemType Directory -Path $resultsDir | Out-Null
    }
    $OutputFile = Join-Path -Path $resultsDir -ChildPath "monitor_$timestamp.csv"
}

Write-Host "======================================" -ForegroundColor Cyan
Write-Host "System Monitor - Press Ctrl+C to stop" -ForegroundColor Cyan
Write-Host "======================================" -ForegroundColor Cyan
Write-Host "Interval: $IntervalSeconds seconds" -ForegroundColor Yellow
Write-Host "Output: $OutputFile`n" -ForegroundColor Yellow

# Create CSV file (overwrite if existing)
$headers = "Time,CPU_%,RAM_MB,RAM_%,Java_Threads,FFmpeg_Procs,FFmpeg_Threads,Total_Threads,GPU_%,GPU_Mem_MB,Encoder"
$headers | Out-File -FilePath $OutputFile -Encoding UTF8

# Header for console
Write-Host "Time                CPU%   RAM_MB  RAM%   Java_T  FFmpeg  FF_T   Total_T  GPU%  GPU_MB  Encoder" -ForegroundColor White
Write-Host "====================================================================================================" -ForegroundColor Gray

try {
    while ($true) {
        $time = Get-Date -Format "yyyy-MM-dd HH:mm:ss"

        # CPU - use the counter and guard if null
        $cpuSample = (Get-Counter '\Processor(_Total)\% Processor Time' -ErrorAction SilentlyContinue).CounterSamples
        $cpu = if ($cpuSample) { [math]::Round($cpuSample.CookedValue, 1) } else { 0.0 }

        # Memory - Win32_OperatingSystem returns KB for TotalVisibleMemorySize / FreePhysicalMemory
        $os = Get-CimInstance Win32_OperatingSystem -ErrorAction SilentlyContinue
        if ($os) {
            $totalMemMB = [math]::Round($os.TotalVisibleMemorySize / 1024, 0)
            $freeMemMB = [math]::Round($os.FreePhysicalMemory / 1024, 0)
            $usedMemMB = $totalMemMB - $freeMemMB
            $memPct = if ($totalMemMB -gt 0) { [math]::Round(($usedMemMB / $totalMemMB) * 100, 1) } else { 0.0 }
        } else {
            $totalMemMB = 0
            $freeMemMB = 0
            $usedMemMB = 0
            $memPct = 0.0
        }

        # Java threads
        $javaProcs = Get-Process -Name java -ErrorAction SilentlyContinue
        $javaThreads = 0
        if ($javaProcs) {
            foreach ($proc in $javaProcs) {
                $javaThreads += ($proc.Threads.Count | Measure-Object -Sum).Sum
            }
        }

        # FFmpeg
        $ffmpegProcs = Get-Process -Name ffmpeg -ErrorAction SilentlyContinue
        $ffmpegCount = 0
        $ffmpegThreads = 0
        if ($ffmpegProcs) {
            $ffmpegCount = $ffmpegProcs.Count
            foreach ($proc in $ffmpegProcs) {
                $ffmpegThreads += ($proc.Threads.Count | Measure-Object -Sum).Sum
            }
        }

        # Total system threads (Measure-Object returns Sum property)
        $totalThreadsObj = Get-Process | Measure-Object -Property Threads -Sum
        $totalThreads = if ($totalThreadsObj.Sum) { [int]$totalThreadsObj.Sum } else { 0 }

        # GPU: try nvidia-smi; support multiple GPUs (take first) and handle missing nvidia-smi
        $gpuPct = 0
        $gpuMem = 0
        $encoder = "NONE"
        $nvidiaOutput = $null

        try {
            $nvidiaOutput = & nvidia-smi --query-gpu=utilization.gpu,memory.used --format=csv,noheader,nounits 2>$null
        } catch {
            # nvidia-smi not available or failed
            $nvidiaOutput = $null
        }

        if ($nvidiaOutput) {
            # nvidia-smi can return multiple lines; take the first non-empty
            $firstLine = $nvidiaOutput | Where-Object { -not [string]::IsNullOrWhiteSpace($_) } | Select-Object -First 1
            if ($firstLine) {
                $parts = $firstLine -split ','
                if ($parts.Count -ge 2) {
                    $gpuPct = try { [int]($parts[0].Trim()) } catch { 0 }
                    $gpuMem = try { [int]($parts[1].Trim()) } catch { 0 }
                }
            }
        }

        # Determine encoder state if ffmpeg processes exist
        if ($ffmpegCount -gt 0) {
            if ($gpuPct -gt 5) { $encoder = "NVENC" } else { $encoder = "SOFTWARE" }
        }

        # Format for display (strings)
        $cpuStr = "{0,5}" -f $cpu
        $ramStr = "{0,7}" -f $usedMemMB
        $ramPctStr = "{0,5}" -f $memPct
        $javaTStr = "{0,7}" -f $javaThreads
        $ffmpegCStr = "{0,7}" -f $ffmpegCount
        $ffmpegTStr = "{0,6}" -f $ffmpegThreads
        $totalTStr = "{0,8}" -f $totalThreads
        $gpuPctStr = "{0,5}" -f $gpuPct
        $gpuMemStr = "{0,7}" -f $gpuMem
        $encoderStr = "{0,8}" -f $encoder

        # Colors
        $cpuColor = if ($cpu -gt 80) { "Red" } elseif ($cpu -gt 60) { "Yellow" } else { "Green" }
        $memColor = if ($memPct -gt 80) { "Red" } elseif ($memPct -gt 60) { "Yellow" } else { "Green" }
        $threadColor = if ($javaThreads -gt 150) { "Red" } elseif ($javaThreads -gt 100) { "Yellow" } else { "Cyan" }
        $encoderColor = if ($encoder -eq "NVENC") { "Green" } elseif ($encoder -eq "SOFTWARE") { "Red" } else { "Gray" }
        $gpuColor = if ($gpuPct -gt 60) { "Yellow" } else { "White" }

        # Display
        Write-Host "$time  " -NoNewline
        Write-Host "$cpuStr  " -NoNewline -ForegroundColor $cpuColor
        Write-Host "$ramStr  " -NoNewline -ForegroundColor White
        Write-Host "$ramPctStr  " -NoNewline -ForegroundColor $memColor
        Write-Host "$javaTStr  " -NoNewline -ForegroundColor $threadColor
        Write-Host "$ffmpegCStr  " -NoNewline -ForegroundColor Cyan
        Write-Host "$ffmpegTStr  " -NoNewline -ForegroundColor Blue
        Write-Host "$totalTStr  " -NoNewline -ForegroundColor White
        Write-Host "$gpuPctStr  " -NoNewline -ForegroundColor $gpuColor
        Write-Host "$gpuMemStr  " -NoNewline -ForegroundColor Magenta
        Write-Host "$encoderStr" -ForegroundColor $encoderColor

        # Save to CSV
        $csvLine = "$time,$cpu,$usedMemMB,$memPct,$javaThreads,$ffmpegCount,$ffmpegThreads,$totalThreads,$gpuPct,$gpuMem,$encoder"
        $csvLine | Out-File -FilePath $OutputFile -Append -Encoding UTF8

        # Warnings
        if ($encoder -eq "SOFTWARE" -and $ffmpegCount -gt 0) {
            Write-Host "  >> WARNING: Using SOFTWARE encoding (not GPU NVENC)!" -ForegroundColor Yellow
        }

        if ($javaThreads -gt 150) {
            Write-Host "  >> WARNING: High Java thread count: $javaThreads" -ForegroundColor Red
        }

        if ($cpu -gt 90 -and $gpuPct -lt 20 -and $ffmpegCount -gt 0) {
            Write-Host "  >> WARNING: High CPU + Low GPU = NVENC not working!" -ForegroundColor Red
        }

        Start-Sleep -Seconds $IntervalSeconds
    }
}
catch {
    Write-Host "`nStopped." -ForegroundColor Yellow
}
finally {
    Write-Host "`nData saved to: $OutputFile" -ForegroundColor Green
    Write-Host "`nWhat to check in CSV:" -ForegroundColor Cyan
    Write-Host "  - Encoder column: Should show 'NVENC' not 'SOFTWARE'" -ForegroundColor White
    Write-Host "  - Java_Threads: Should stay under 150 (you have 100 streams)" -ForegroundColor White
    Write-Host "  - GPU_%: Should be 40-80% when streams are running" -ForegroundColor White
    Write-Host "  - CPU_%: Should be 20-40% with NVENC, 80%+ with SOFTWARE" -ForegroundColor White
}
