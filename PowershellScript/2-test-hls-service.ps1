# Simple 80 Stream Test Script
$StreamCount = 20
$ApiBaseUrl = "http://localhost:8080"
$RtspUrl = "rtsp://127.0.0.1:8554/v1"
$BatchSize = 25

Write-Host "========================================"
Write-Host "Starting $StreamCount Streams Test"
Write-Host "========================================"

$results = @{
    Started = 0
    Failed  = 0
    Errors  = @()
}

$batchCount = [Math]::Ceiling($StreamCount / $BatchSize)

for ($batch = 0; $batch -lt $batchCount; $batch++) {
    $start = ($batch * $BatchSize) + 1
    $end   = [Math]::Min((($batch + 1) * $BatchSize), $StreamCount)
    
    Write-Host "`nBatch $($batch + 1)/$batchCount : Streams $start-$end"

    $jobs = @()
    for ($i = $start; $i -le $end; $i++) {
        $streamName = "cam_$i"

        $job = Start-Job -ScriptBlock {
            param($api, $rtsp, $name)
            try {
                $body = @{
                    rtspUrl    = $rtsp
                    streamName = $name
                } | ConvertTo-Json

                $response = Invoke-RestMethod `
                    -Uri "$api/api/stream/hls/start" `
                    -Method POST `
                    -ContentType "application/json" `
                    -Body $body `
                    -TimeoutSec 15

                return @{ Success = $true; Name = $name }
            }
            catch {
                return @{ Success = $false; Name = $name; Error = $_.Exception.Message }
            }
        } -ArgumentList $ApiBaseUrl, $RtspUrl, $streamName

        $jobs += $job
    }

    $jobs | Wait-Job | Out-Null

    foreach ($job in $jobs) {
        $result = Receive-Job -Job $job
        if ($result.Success) {
            $results.Started++
            Write-Host "  V $($result.Name)" -ForegroundColor Green
        } else {
            $results.Failed++
            $results.Errors += "$($result.Name): $($result.Error)"
            Write-Host "  X $($result.Name): $($result.Error)" -ForegroundColor Red
        }
    }

    $jobs | Remove-Job -Force
    
    if ($batch -lt $batchCount - 1) {
        Write-Host "  Waiting 2 seconds..."
        Start-Sleep -Seconds 2
    }
}

Write-Host "`n========================================"
Write-Host "FINAL RESULTS"
Write-Host "========================================"
Write-Host "Started: $($results.Started)" -ForegroundColor Green
Write-Host "Failed: $($results.Failed)" -ForegroundColor Red

if ($results.Errors.Count -gt 0) {
    Write-Host "`nErrors:"
    $results.Errors | ForEach-Object { Write-Host "  $_" -ForegroundColor Red }
}


Write-Host "`nTest Complete!"