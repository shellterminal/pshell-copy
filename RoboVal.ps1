param(
    [switch]$RunRobocopy
)

# ===================== CONFIG =====================
$Source      = "D:\Source\"
$Destination = "Z:\Destination"

$Threads = 12
$RobocopyThreads = 32
$HashAlg = "SHA256"

# Log directory (next to the script)
$ScriptDir = if ($PSScriptRoot) { $PSScriptRoot } else { (Get-Location).Path }
$LogDir = Join-Path $ScriptDir "logs"
if (-not (Test-Path $LogDir)) { New-Item -Path $LogDir -ItemType Directory -Force | Out-Null }

# Log files (resume-safe)
$RobocopyLog        = Join-Path $LogDir "robocopy_transfer.log"
$MD5Log             = Join-Path $LogDir "md5_validation.log"
$ActivityLog        = Join-Path $LogDir "robocopy_activity.log"
$RetrySuccessLog    = Join-Path $LogDir "ps_retry_copy_success.log"
$RetryFailedLog     = Join-Path $LogDir "ps_retry_copy_failed.log"
$RetryValLog        = Join-Path $LogDir "ps_retry_validation.log"
$ReportCsv          = Join-Path $LogDir "robocopy_validation_report.csv"        # resume CSV — do NOT clear
$FinalReportCsv     = Join-Path $LogDir "robocopy_final_report.csv"
$MismatchLog        = Join-Path $LogDir "robocopy_mismatches.txt"

# exclusions - folder name fragments (case-insensitive)
$ExcludedDirs = @(
    "System Volume Information",
    '$Recycle.Bin'
)

# -----------------------
# Ensure logs exist and add run header if already present (do NOT clear)
# -----------------------
$runHeader = "=== Run at {0} ===" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss")

# Helper to ensure and append header (preserve file)
function Ensure-Log {
    param($Path)
    if (-not (Test-Path $Path)) {
        New-Item -Path $Path -ItemType File -Force | Out-Null
    } else {
        Add-Content -LiteralPath $Path -Value $runHeader
    }
}

# Ensure logs (do not clear ReportCsv)
Ensure-Log $RobocopyLog
Ensure-Log $MD5Log
Ensure-Log $ActivityLog
Ensure-Log $RetrySuccessLog
Ensure-Log $RetryFailedLog
Ensure-Log $RetryValLog
Ensure-Log $MismatchLog

# Create report CSV if missing, but DO NOT clear if exists (we rely on existing for resume)
if (-not (Test-Path $ReportCsv)) {
    # create header-compatible CSV
    $header = "FullPath,RelativePath,SizeBytes,SrcHash,DstHash,Status,Error"
    $header | Out-File -FilePath $ReportCsv -Encoding UTF8
} else {
    Add-Content -LiteralPath $ReportCsv -Value $runHeader
}

# Final report can be overwritten each run
if (Test-Path $FinalReportCsv) { Remove-Item -LiteralPath $FinalReportCsv -Force -ErrorAction SilentlyContinue }

Write-Host "Logs: $LogDir" -ForegroundColor Cyan

# ===================== PHASE 1: ROBOCOPY (optional) =====================
if ($RunRobocopy) {
    Write-Host "Starting Robocopy: $Source -> $Destination" -ForegroundColor Cyan

    $robocopyArgs = @(
        $Source,
        $Destination,
        "/MIR",
        "/MT:$RobocopyThreads",
        "/R:3",
        "/W:5",
        "/FFT",
        "/Z",
        "/TEE",
        "/LOG:$RobocopyLog",
        "/XD"
    ) + $ExcludedDirs

    Write-Host "robocopy $($robocopyArgs -join ' ')" -ForegroundColor DarkGray
    & robocopy @robocopyArgs
    $last = $LASTEXITCODE
    Write-Host "Robocopy finished with exit code $last" -ForegroundColor Yellow
} else {
    Write-Host "Robocopy skipped. (Pass -RunRobocopy to enable)" -ForegroundColor Yellow
}

# ===================== PHASE 2: MD5 VALIDATION (threaded) =====================
Write-Host "Enumerating files for validation (excluding system/recycle)..." -ForegroundColor Cyan

$allFiles = Get-ChildItem -Path $Source -Recurse -File -Force -ErrorAction SilentlyContinue

$FileList = $allFiles | Where-Object {
    $full = $_.FullName
    $skip = $false
    foreach ($exc in $ExcludedDirs) {
        if ($full.ToLower().Contains($exc.ToLower().Replace('\',''))) { $skip = $true; break }
    }
    -not $skip
}

$totalAll = $FileList.Count
Write-Host "Files found (after exclusions): $totalAll" -ForegroundColor Cyan
if ($totalAll -eq 0) { Write-Host "No files to validate. Exiting." -ForegroundColor Yellow; exit 0 }

# Load existing resume CSV into $existing (do not modify the file)
$existing = @{}
if (Test-Path $ReportCsv) {
    try {
        $csv = Import-Csv -Path $ReportCsv -ErrorAction Stop
        foreach ($r in $csv) {
            # prefer RelativePath
            if ($r.PSObject.Properties.Match('RelativePath').Count -gt 0 -and $r.RelativePath) {
                $existing[$r.RelativePath] = $r
            } elseif ($r.PSObject.Properties.Match('FullPath').Count -gt 0 -and $r.FullPath) {
                $rel = $r.FullPath.Substring($Source.Length).TrimStart('\','/')
                $existing[$rel] = $r
            }
        }
        Write-Host "Loaded existing report with $($existing.Count) entries for resume." -ForegroundColor Yellow
    } catch {
        Write-Warning "Could not parse existing CSV; starting fresh."
        $existing = @{}
    }
}

# ThreadJob setup
Import-Module ThreadJob -ErrorAction SilentlyContinue

$Queue = [System.Collections.Concurrent.ConcurrentQueue[System.IO.FileInfo]]::new()
$ResultsDict = [System.Collections.Concurrent.ConcurrentDictionary[string,object]]::new()
$CurrentFileQueue = [System.Collections.Concurrent.ConcurrentQueue[string]]::new()

# Enqueue items that are not already OK in existing map
foreach ($f in $FileList) {
    $rel = $f.FullName.Substring($Source.Length).TrimStart('\','/')
    if ($existing.ContainsKey($rel) -and ($existing[$rel].Status -eq 'OK')) { continue }
    $null = $Queue.Enqueue($f)
}

$toProcess = $Queue.Count
if ($toProcess -eq 0) { Write-Host "Nothing to validate (all OK). Exiting." -ForegroundColor Green; exit 0 }
Write-Host "Queued $toProcess files for validation." -ForegroundColor Cyan

# Worker block - threaded MD5 validation
$Worker = {
    param($Queue, $SrcRoot, $DstRoot, $ResultsDict, $CurrentFileQueue, $ActivityLog, $MD5Log, $HashAlg)

    $file = $null
    while ($Queue.TryDequeue([ref]$file)) {
        $null = $CurrentFileQueue.Enqueue($file.FullName)

        $relative = $file.FullName.Substring($SrcRoot.Length).TrimStart('\','/')
        $destPath = Join-Path $DstRoot $relative

        $record = [PSCustomObject]@{
            FullPath     = $file.FullName
            RelativePath = $relative
            SizeBytes    = $file.Length
            SrcHash      = $null
            DstHash      = $null
            Status       = "OK"
            Error        = $null
        }

        try {
            if (-not (Test-Path -LiteralPath $destPath)) {
                $record.Status = "MISSING"
            } else {
                $record.SrcHash = (Get-FileHash -LiteralPath $file.FullName -Algorithm $HashAlg).Hash
                $record.DstHash = (Get-FileHash -LiteralPath $destPath -Algorithm $HashAlg).Hash
                if ($record.SrcHash -ne $record.DstHash) { $record.Status = "MISMATCH" }
            }
        }
        catch {
            $record.Status = "ERROR"
            $record.Error = $_.Exception.Message
        }

        try { $ResultsDict[$record.RelativePath] = $record } catch { $null = $ResultsDict.TryAdd($record.RelativePath, $record) }

        $logLine = "{0}`t{1}`t{2}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $record.Status, $record.FullPath
        Add-Content -LiteralPath $ActivityLog -Value $logLine
        Add-Content -LiteralPath $MD5Log -Value $logLine
    }
}

# start worker jobs
$jobs = @()
for ($i = 1; $i -le $Threads; $i++) {
    $jobs += Start-ThreadJob -ScriptBlock $Worker -ArgumentList $Queue, $Source, $Destination, $ResultsDict, $CurrentFileQueue, $ActivityLog, $MD5Log, $HashAlg
}

# progress loop - percent + current file
$currentFile = ""
$temp = $null
while (($jobs | Where-Object State -in @('Running','NotStarted')).Count -gt 0) {
    $processed = $ResultsDict.Count
    $pct = if ($toProcess -eq 0) { 0 } else { [math]::Round(($processed / $toProcess) * 100, 2) }

    while ($CurrentFileQueue.TryDequeue([ref]$temp)) { $currentFile = $temp }

    Write-Progress -Activity "MD5 Validating files" -Status ("[{0} of {1} ({2}%)] - {3}" -f $processed, $toProcess, $pct, $currentFile) -PercentComplete $pct

    Start-Sleep -Milliseconds 250
}

# ensure workers finished and drain
foreach ($job in $jobs) { Receive-Job -Job $job -ErrorAction SilentlyContinue | Out-Null }
Remove-Job -Job $jobs -Force -ErrorAction SilentlyContinue

# final progress
$processed = $ResultsDict.Count
$finalPct = if ($toProcess -eq 0) { 100 } else { [math]::Round(($processed / $toProcess) * 100, 2) }
Write-Progress -Activity "MD5 Validating files" -Status ("[{0} of {1} ({2}%)]" -f $processed, $toProcess, $finalPct) -PercentComplete $finalPct

# ===================== Write first-pass report (merge with existing) =====================
$processedResults = $ResultsDict.Values

$finalMap = @{}
foreach ($k in $existing.Keys) { $finalMap[$k] = $existing[$k] }
foreach ($r in $processedResults) {
    $obj = [PSCustomObject]@{
        FullPath     = $r.FullPath
        RelativePath = $r.RelativePath
        SizeBytes    = $r.SizeBytes
        SrcHash      = $r.SrcHash
        DstHash      = $r.DstHash
        Status       = $r.Status
        Error        = $r.Error
    }
    $finalMap[$r.RelativePath] = $obj
}

$firstPassList = $finalMap.Values | Sort-Object Status, RelativePath

# Export merged report by overwriting the report CSV with merged data (this preserves prior entries merged into finalMap)
$firstPassList | Export-Csv -Path $ReportCsv -NoTypeInformation -Encoding UTF8

# write mismatch log (overwrite current mismatch log for clarity)
if (Test-Path $MismatchLog) { Clear-Content -LiteralPath $MismatchLog -ErrorAction SilentlyContinue }
$firstPassList | Where-Object { $_.Status -ne "OK" } | ForEach-Object {
    $line = "{0}`t{1}`t{2}" -f $_.Status, $_.RelativePath, ($_.Error -join ' ')
    Add-Content -LiteralPath $MismatchLog -Value $line
}

# ===================== PHASE 3: RECOVERY (single-threaded Copy-Item -LiteralPath) =====================
$toRecover = $firstPassList | Where-Object { $_.Status -in @('MISSING','MISMATCH','ERROR') }
Write-Host "Items to attempt recovery (Copy-Item -LiteralPath): $($toRecover.Count)" -ForegroundColor Cyan

$totalRetry = $toRecover.Count
$idx = 0
$recoveredNow = @()
$failedNow = @()

foreach ($entry in $toRecover) {
    $idx++
    $pctRetry = [math]::Round(($idx / $totalRetry) * 100, 2)
    $src = $entry.FullPath
    $dst = Join-Path $Destination $entry.RelativePath

    Write-Host ("[Retry {0}%] Copying ({1} of {2}) - {3}" -f $pctRetry, $idx, $totalRetry, $src)

    try {
        $dstDir = Split-Path -Parent $dst
        if (-not (Test-Path -LiteralPath $dstDir)) { New-Item -ItemType Directory -Path $dstDir -Force | Out-Null }

        Copy-Item -LiteralPath $src -Destination $dst -Force -ErrorAction Stop

        $sh = if (Test-Path -LiteralPath $src) { (Get-FileHash -LiteralPath $src -Algorithm $HashAlg).Hash } else { $null }
        $dh = if (Test-Path -LiteralPath $dst) { (Get-FileHash -LiteralPath $dst -Algorithm $HashAlg).Hash } else { $null }

        if ($sh -and $dh -and $sh -eq $dh) {
            $entry.Status = "FIXED_BY_PS_COPY"
            $entry.SrcHash = $sh
            $entry.DstHash = $dh
            $recoveredNow += $entry
            $log = "{0}`t{1}`t{2}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), "FIXED_BY_PS_COPY", $entry.RelativePath
            Add-Content -LiteralPath $RetrySuccessLog -Value $log
            Add-Content -LiteralPath $RetryValLog -Value $log
            continue
        } else {
            Remove-Item -LiteralPath $dst -Force -ErrorAction SilentlyContinue
            throw "Hash mismatch after direct copy"
        }
    }
    catch {
        try {
            $srcLP = if ($src.StartsWith("\\?\\")) { $src } else { "\\?\$src" }
            $dstLP = if ($dst.StartsWith("\\?\\")) { $dst } else { "\\?\$dst" }
            $dstDirLP = Split-Path -Parent $dstLP
            if (-not (Test-Path -LiteralPath $dstDirLP)) { New-Item -ItemType Directory -Path $dstDirLP -Force | Out-Null }

            Copy-Item -LiteralPath $srcLP -Destination $dstLP -Force -ErrorAction Stop

            $sh = if (Test-Path -LiteralPath $srcLP) { (Get-FileHash -LiteralPath $srcLP -Algorithm $HashAlg).Hash } else { $null }
            $dh = if (Test-Path -LiteralPath $dstLP) { (Get-FileHash -LiteralPath $dstLP -Algorithm $HashAlg).Hash } else { $null }

            if ($sh -and $dh -and $sh -eq $dh) {
                $entry.Status = "FIXED_BY_PS_COPY_LONGPATH"
                $entry.SrcHash = $sh
                $entry.DstHash = $dh
                $recoveredNow += $entry
                $log = "{0}`t{1}`t{2}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), "FIXED_BY_PS_COPY_LONGPATH", $entry.RelativePath
                Add-Content -LiteralPath $RetrySuccessLog -Value $log
                Add-Content -LiteralPath $RetryValLog -Value $log
                continue
            } else {
                Remove-Item -LiteralPath $dstLP -Force -ErrorAction SilentlyContinue
                $err = "FAILED_AFTER_COPY_HASHMISMATCH"
                $entry.Status = $err
                $failedNow += $entry
                $log = "{0}`t{1}`t{2}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $err, $entry.RelativePath
                Add-Content -LiteralPath $RetryFailedLog -Value $log
                Add-Content -LiteralPath $RetryValLog -Value $log
            }
        }
        catch {
            $errMsg = $_.Exception.Message
            $entry.Status = "RECOVERY_FAILED"
            $entry.Error = $errMsg
            $failedNow += $entry
            $log = "{0}`t{1}`t{2}`t{3}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), "RECOVERY_FAILED", $entry.RelativePath, $errMsg
            Add-Content -LiteralPath $RetryFailedLog -Value $log
            Add-Content -LiteralPath $RetryValLog -Value $log
        }
    }
}

# ===================== PHASE 4: RE-VALIDATE RETRIED ITEMS (single-threaded) =====================
$retried = $recoveredNow + $failedNow
Write-Host "Re-validating $($retried.Count) retried items..." -ForegroundColor Cyan

# ensure finalMap exists (from first pass merge)
if (-not $finalMap) { $finalMap = @{}; foreach ($k in $existing.Keys) { $finalMap[$k] = $existing[$k] } }

foreach ($e in $retried) {
    $src = $e.FullPath
    $dst = Join-Path $Destination $e.RelativePath

    try {
        $srcHash = if (Test-Path -LiteralPath $src) { (Get-FileHash -LiteralPath $src -Algorithm $HashAlg).Hash } else { $null }
        $dstHash = if (Test-Path -LiteralPath $dst) { (Get-FileHash -LiteralPath $dst -Algorithm $HashAlg).Hash } else { $null }

        $e.SrcHash = $srcHash
        $e.DstHash = $dstHash

        if ($srcHash -and $dstHash -and $srcHash -eq $dstHash) {
            $e.Status = "OK"
            $log = "{0}`t{1}`t{2}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), "RETRY_OK", $e.RelativePath
            Add-Content -LiteralPath $RetrySuccessLog -Value $log
            Add-Content -LiteralPath $RetryValLog -Value $log
        } elseif ($dstHash -and -not $srcHash) {
            $e.Status = "MISSING_SRC"
            Add-Content -LiteralPath $RetryFailedLog -Value ("{0}`tMISSING_SRC`t{1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $e.RelativePath)
            Add-Content -LiteralPath $RetryValLog -Value ("{0}`tMISSING_SRC`t{1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $e.RelativePath)
        } elseif ($srcHash -and -not $dstHash) {
            $e.Status = "MISSING_DST"
            Add-Content -LiteralPath $RetryFailedLog -Value ("{0}`tMISSING_DST`t{1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $e.RelativePath)
            Add-Content -LiteralPath $RetryValLog -Value ("{0}`tMISSING_DST`t{1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $e.RelativePath)
        } else {
            if ($e.Status -notin @("FIXED_BY_PS_COPY","FIXED_BY_PS_COPY_LONGPATH")) {
                $e.Status = "FAILED_AFTER_RETRY"
                Add-Content -LiteralPath $RetryFailedLog -Value ("{0}`tFAILED_AFTER_RETRY`t{1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $e.RelativePath)
                Add-Content -LiteralPath $RetryValLog -Value ("{0}`tFAILED_AFTER_RETRY`t{1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $e.RelativePath)
            } else {
                $e.Status = "OK"
                Add-Content -LiteralPath $RetrySuccessLog -Value ("{0}`tOK_AFTER_RETRY`t{1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $e.RelativePath)
                Add-Content -LiteralPath $RetryValLog -Value ("{0}`tOK_AFTER_RETRY`t{1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $e.RelativePath)
            }
        }
    }
    catch {
        $e.Status = "REVAL_ERROR"
        $e.Error = $_.Exception.Message
        Add-Content -LiteralPath $RetryFailedLog -Value ("{0}`tREVAL_ERROR`t{1}`t{2}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $e.RelativePath, $e.Error)
        Add-Content -LiteralPath $RetryValLog -Value ("{0}`tREVAL_ERROR`t{1}`t{2}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $e.RelativePath, $e.Error)
    }

    $finalMap[$e.RelativePath] = $e
}

# ===================== FINAL REPORT =====================
$finalList = $finalMap.Values | Sort-Object Status, RelativePath
$finalList | Export-Csv -Path $FinalReportCsv -NoTypeInformation -Encoding UTF8

# refresh mismatch log with final statuses
if (Test-Path $MismatchLog) { Clear-Content -LiteralPath $MismatchLog -ErrorAction SilentlyContinue }
$finalList | Where-Object { $_.Status -ne "OK" } | ForEach-Object {
    $line = "{0}`t{1}`t{2}" -f $_.Status, $_.RelativePath, ($_.Error -join ' ')
    Add-Content -LiteralPath $MismatchLog -Value $line
}

# ===================== SUMMARY =====================
$okCount = ($finalList | Where-Object { $_.Status -eq "OK" }).Count
$missingCount = ($finalList | Where-Object { $_.Status -eq "MISSING" }).Count
$mismatchCount = ($finalList | Where-Object { $_.Status -eq "MISMATCH" }).Count
$errorCount = ($finalList | Where-Object { $_.Status -eq "ERROR" }).Count
$fixedCount = ($finalList | Where-Object { $_.Status -match '^FIXED' }).Count
$failedRecovery = ($finalList | Where-Object { $_.Status -in @('FAILED_AFTER_RETRY','RECOVERY_FAILED','FAILED_AFTER_COPY_HASHMISMATCH') }).Count

Write-Host ""
Write-Host "=== FINAL SUMMARY ===" -ForegroundColor Green
Write-Host ("OK:                {0}" -f $okCount)
Write-Host ("MISSING:           {0}" -f $missingCount)
Write-Host ("MISMATCH:          {0}" -f $mismatchCount)
Write-Host ("ERROR:             {0}" -f $errorCount)
Write-Host ("FIXED (PS Copy):   {0}" -f $fixedCount)
Write-Host ("Failed Recovery:   {0}" -f $failedRecovery)
Write-Host ""
Write-Host ("First-pass report: {0}" -f $ReportCsv)
Write-Host ("Final report:      {0}" -f $FinalReportCsv)
Write-Host ("MD5 log:           {0}" -f $MD5Log)
Write-Host ("Activity log:      {0}" -f $ActivityLog)
Write-Host ("Recovery success:  {0}" -f $RetrySuccessLog)
Write-Host ("Recovery failed:   {0}" -f $RetryFailedLog)
Write-Host ("Retry validation:  {0}" -f $RetryValLog)
Write-Host ("Mismatch log:      {0}" -f $MismatchLog)
Write-Host ""

# End
