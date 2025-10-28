# PowerShell Script: Test HLS Service with Multiple Concurrent Streams
# This script calls your Spring Boot API to convert RTSP to HLS

param(
    [int]$StreamCount = 50,
    [string]$ApiBaseUrl = "http://localhost:8080",
    [string]$MediaMTXHost = "127.0.0.1",
    [int]$MediaMTXPort = 8554,
    [int]$BatchSize = 10,
    [int]$BatchDelay = 2
)

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "HLS Service Load Test" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Stream Count: $StreamCount" -ForegroundColor Yellow
Write-Host "API URL: $ApiBaseUrl" -ForegroundColor Yellow
Write-Host "Batch Size: $BatchSize streams" -ForegroundColor Yellow
Write-Host "Batch Delay: $BatchDelay seconds" -ForegroundColor Yellow
Write-Host "========================================`n" -ForegroundColor Cyan

# Test API connectivity
Write-Host "Testing API connectivity..." -ForegroundColor Yellow
try {
    $response = Invoke-RestMethod -Uri "$ApiBaseUrl/actuator/health" -Method GET -TimeoutSec 5 -ErrorAction Stop
    Write-Host "✓ API is reachable" -ForegroundColor Green
} catch {
    Write-Host "✗ API is not reachable at $ApiBaseUrl" -ForegroundColor Red
    Write-Host "Error: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}

# Create results directory
$resultsDir = ".\test_results"
if (-not (Test-Path $resultsDir)) {
    New-Item -ItemType Directory -Path $resultsDir | Out-Null
}

# Results tracking
$results = @()
$successCount = 0
$failCount = 0
$jobs = @()

Write-Host "`nStarting HLS stream conversion test..." -ForegroundColor Green
Write-Host "Processing in batches of $BatchSize streams`n" -ForegroundColor Gray

# Process streams in batches
for ($batch = 0; $batch -lt [Math]::Ceiling($StreamCount / $BatchSize); $batch++) {
    $batchStart = ($batch * $BatchSize) + 1
    $batchEnd = [Math]::Min(($batch + 1) * $BatchSize, $StreamCount)
    
    Write-Host "Batch $($batch + 1): Processing streams $batchStart to $batchEnd" -ForegroundColor Cyan
    
    for ($i = $batchStart; $i -le $batchEnd; $i++) {
        $streamName = "cam_$i"
        $rtspUrl = "rtsp://${MediaMTXHost}:${MediaMTXPort}/v$i"
        
        Write-Host "  [$i/$StreamCount] Starting: $streamName" -ForegroundColor Gray
        
        # Create job to call API
        $job = Start-Job -ScriptBlock {
            param($apiUrl, $rtspUrl, $streamName, $index)
            
            $startTime = Get-Date
            $result = @{
                Index = $index
                StreamName = $streamName
                RtspUrl = $rtspUrl
                Success = $false
                ResponseTime = 0
                HlsUrl = $null
                Error = $null
            }
            
            try {
                $body = @{
                    rtspUrl = $rtspUrl
                    streamName = $streamName
                } | ConvertTo-Json
                
                $response = Invoke-RestMethod `
                    -Uri "$apiUrl/api/stream/hls/start" `
                    -Method POST `
                    -ContentType "application/json" `
                    -Body $body `
                    -TimeoutSec 30
                
                $endTime = Get-Date
                $result.Success = $true
                $result.ResponseTime = ($endTime - $startTime).TotalSeconds
                $result.HlsUrl = $response
                
            } catch {
                $endTime = Get-Date
                $result.Success = $false
                $result.ResponseTime = ($endTime - $startTime).TotalSeconds
                $result.Error = $_.Exception.Message
            }
            
            return $result
        } -ArgumentList $ApiBaseUrl, $rtspUrl, $streamName, $i
        
        $jobs += $job
        
        # Small delay between requests
        Start-Sleep -Milliseconds 100
    }
    
    # Wait for batch to complete before starting next batch
    if ($batch -lt [Math]::Ceiling($StreamCount / $BatchSize) - 1) {
        Write-Host "  Waiting $BatchDelay seconds before next batch..." -ForegroundColor Yellow
        Start-Sleep -Seconds $BatchDelay
    }
}

Write-Host "`nAll requests sent. Waiting for responses..." -ForegroundColor Yellow
Write-Host "This may take a moment...`n" -ForegroundColor Gray

# Wait for all jobs to complete
$jobs | Wait-Job | Out-Null

# Collect results
Write-Host "Collecting results..." -ForegroundColor Cyan
foreach ($job in $jobs) {
    $result = Receive-Job -Job $job
    $results += $result
    
    if ($result.Success) {
        $successCount++
        Write-Host "✓ [$($result.Index)] $($result.StreamName) - Success (${($result.ResponseTime)}s)" -ForegroundColor Green
    } else {
        $failCount++
        Write-Host "✗ [$($result.Index)] $($result.StreamName) - Failed: $($result.Error)" -ForegroundColor Red
    }
}

# Clean up jobs
$jobs | Remove-Job -Force

# Calculate statistics
$avgResponseTime = ($results | Where-Object { $_.Success } | Measure-Object -Property ResponseTime -Average).Average
$minResponseTime = ($results | Where-Object { $_.Success } | Measure-Object -Property ResponseTime -Minimum).Minimum
$maxResponseTime = ($results | Where-Object { $_.Success } | Measure-Object -Property ResponseTime -Maximum).Maximum

# Save detailed results to CSV
$csvPath = "$resultsDir\test_results_$(Get-Date -Format 'yyyyMMdd_HHmmss').csv"
$results | Export-Csv -Path $csvPath -NoTypeInformation

# Display summary
Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "TEST SUMMARY" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Total Streams: $StreamCount" -ForegroundColor White
Write-Host "Successful: $successCount" -ForegroundColor Green
Write-Host "Failed: $failCount" -ForegroundColor Red
Write-Host "Success Rate: $([Math]::Round(($successCount / $StreamCount) * 100, 2))%" -ForegroundColor Yellow

if ($successCount -gt 0) {
    Write-Host "`nResponse Times:" -ForegroundColor White
    Write-Host "  Average: $([Math]::Round($avgResponseTime, 2))s" -ForegroundColor Gray
    Write-Host "  Minimum: $([Math]::Round($minResponseTime, 2))s" -ForegroundColor Gray
    Write-Host "  Maximum: $([Math]::Round($maxResponseTime, 2))s" -ForegroundColor Gray
}

Write-Host "`nResults saved to: $csvPath" -ForegroundColor Yellow
Write-Host "========================================`n" -ForegroundColor Cyan

# Show failed streams
if ($failCount -gt 0) {
    Write-Host "Failed Streams:" -ForegroundColor Red
    $results | Where-Object { -not $_.Success } | ForEach-Object {
        Write-Host "  - $($_.StreamName): $($_.Error)" -ForegroundColor Red
    }
}

# Show sample HLS URLs
if ($successCount -gt 0) {
    Write-Host "`nSample HLS URLs (first 5):" -ForegroundColor Green
    $results | Where-Object { $_.Success } | Select-Object -First 5 | ForEach-Object {
        Write-Host "  $ApiBaseUrl$($_.HlsUrl)" -ForegroundColor Gray
    }
}

Write-Host "`nTest complete!" -ForegroundColor Green