# PowerShell script: run50.ps1

# สร้าง loop 1-20
1..15 | ForEach-Object {
    $i = $_
    Start-Job -ScriptBlock {
        param($idx)
        $body = @{
            rtspUrl = "rtsp://127.0.0.1:8554/v1"
            streamName = "cam_$idx"
        } | ConvertTo-Json

        Invoke-RestMethod -Uri "http://localhost:8080/api/stream/hls/start" `
                          -Method POST `
                          -ContentType "application/json" `
                          -Body $body
        Write-Output "Started cam_$idx"
    } -ArgumentList $i
}

# รอทุก job จบ
Get-Job | Wait-Job
Get-Job | Receive-Job
