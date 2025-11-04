Write-Host "=== NVENC Diagnostic Tool ===" -ForegroundColor Green
Write-Host ""

# 1. Check NVIDIA Driver
Write-Host "1. Checking NVIDIA Driver..." -ForegroundColor Yellow
try {
    $nvidiaInfo = nvidia-smi --query-gpu=driver_version,name --format=csv,noheader
    Write-Host "   ✓ GPU: $nvidiaInfo" -ForegroundColor Green
} catch {
    Write-Host "   ✗ nvidia-smi not found!" -ForegroundColor Red
    exit
}

# 2. Check FFmpeg NVENC support
Write-Host ""
Write-Host "2. Checking FFmpeg NVENC encoders..." -ForegroundColor Yellow
$nvencCheck = ffmpeg -encoders 2>&1 | Select-String "nvenc"
if ($nvencCheck) {
    Write-Host "   ✓ NVENC encoders found:" -ForegroundColor Green
    $nvencCheck | ForEach-Object { Write-Host "     $_" }
} else {
    Write-Host "   ✗ No NVENC encoders found!" -ForegroundColor Red
    Write-Host "   Your FFmpeg build doesn't support NVENC" -ForegroundColor Red
}

# 3. Check for CUDA/NVENC DLLs
Write-Host ""
Write-Host "3. Checking for NVENC libraries..." -ForegroundColor Yellow
$cudaPath = "C:\Windows\System32\nvcuda.dll"
$nvencPath = "C:\Windows\System32\nvencodeapi64.dll"

if (Test-Path $cudaPath) {
    Write-Host "   ✓ nvcuda.dll found" -ForegroundColor Green
} else {
    Write-Host "   ✗ nvcuda.dll NOT found" -ForegroundColor Red
}

if (Test-Path $nvencPath) {
    Write-Host "   ✓ nvencodeapi64.dll found" -ForegroundColor Green
} else {
    Write-Host "   ✗ nvencodeapi64.dll NOT found" -ForegroundColor Red
}

# 4. Test NVENC encoding
Write-Host ""
Write-Host "4. Testing NVENC encoding..." -ForegroundColor Yellow
$testOutput = "test_nvenc_output.mp4"

$testResult = ffmpeg -y -f lavfi -i testsrc=duration=1:size=640x480:rate=30 `
    -c:v h264_nvenc -preset fast `
    $testOutput 2>&1

if ($LASTEXITCODE -eq 0 -and (Test-Path $testOutput)) {
    Write-Host "   ✓ NVENC encoding test PASSED!" -ForegroundColor Green
    Remove-Item $testOutput -Force
} else {
    Write-Host "   ✗ NVENC encoding test FAILED!" -ForegroundColor Red
    Write-Host "   Error details:" -ForegroundColor Red
    $testResult | Select-String "error|failed|cannot" | ForEach-Object { 
        Write-Host "     $_" -ForegroundColor Red 
    }
}

# 5. Check JavaCV FFmpeg libraries
Write-Host ""
Write-Host "5. Checking JavaCV FFmpeg location..." -ForegroundColor Yellow
$javacppCache = "$env:USERPROFILE\.javacpp\cache"
if (Test-Path $javacppCache) {
    $ffmpegDlls = Get-ChildItem -Path $javacppCache -Recurse -Filter "avcodec*.dll" -ErrorAction SilentlyContinue
    if ($ffmpegDlls) {
        Write-Host "   ✓ JavaCV FFmpeg found at:" -ForegroundColor Green
        $ffmpegDlls | Select-Object -First 1 | ForEach-Object { 
            Write-Host "     $($_.DirectoryName)" 
        }
    }
}

Write-Host ""
Write-Host "=== Diagnosis Complete ===" -ForegroundColor Green