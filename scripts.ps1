[CmdletBinding()]
param(
    [Parameter(Position = 0)]
    [ValidateSet(
        'Help',
        'Up',
        'Ps',
        'Down',
        'Destroy',
        'Seed',
        'Reset',
        'Shape',
        'PayloadSize',
        'ToastCheck',
        'RelationSize',
        'HotCount',
        'StartWorkload',
        'StopWorkload',
        'BackfillGuid',
        'BackfillCtid',
        'BackfillCtidLive',
        'Results',
        'Compare'
    )]
    [string]$Action = 'Help',

    [long]$RowCount = 1000000,
    [decimal]$HotPct = 2.5,
    [int]$TemplateCount = 128,
    [int]$BlobTargetBytes = 2500,
    [string]$HistorySpan = '365 days',
    [int]$BatchSize = 1000,
    [int]$LogEvery = 100,
    [int]$UpdateClients = 4,
    [int]$UpdateThreads = 4,
    [int]$InsertClients = 2,
    [int]$InsertThreads = 2,
    [int]$WorkloadSeconds = 600,
    [int]$ResultLimit = 5
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$Script:RepoRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$Script:RuntimeDir = Join-Path $Script:RepoRoot '.runtime'
$Script:LogsDir = Join-Path $Script:RuntimeDir 'logs'
$Script:WorkloadStatePath = Join-Path $Script:RuntimeDir 'workload-state.json'
$Script:InvariantCulture = [System.Globalization.CultureInfo]::InvariantCulture

function Ensure-RuntimeDirs {
    if (-not (Test-Path -LiteralPath $Script:RuntimeDir)) {
        New-Item -ItemType Directory -Path $Script:RuntimeDir | Out-Null
    }

    if (-not (Test-Path -LiteralPath $Script:LogsDir)) {
        New-Item -ItemType Directory -Path $Script:LogsDir | Out-Null
    }
}

function Invoke-DockerCompose {
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$Arguments
    )

    & docker compose @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "docker compose $($Arguments -join ' ') failed with exit code $LASTEXITCODE"
    }
}

function Invoke-Psql {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Sql,

        [switch]$At,
        [switch]$NoStopOnError
    )

    $args = @('exec', '-T', 'postgres', 'psql', '-U', 'postgres', '-d', 'bench')

    if ($At) {
        $args += '-At'
    }

    if (-not $NoStopOnError) {
        $args += @('-v', 'ON_ERROR_STOP=1')
    }

    $args += @('-c', $Sql)

    Push-Location $Script:RepoRoot
    try {
        $output = & docker compose @args
        if ($LASTEXITCODE -ne 0) {
            throw "psql command failed with exit code $LASTEXITCODE"
        }

        return $output
    }
    finally {
        Pop-Location
    }
}

function Start-Compose {
    Push-Location $Script:RepoRoot
    try {
        Invoke-DockerCompose -Arguments @('up', '-d')
        Invoke-DockerCompose -Arguments @('ps')
    }
    finally {
        Pop-Location
    }
}

function Show-ComposePs {
    Push-Location $Script:RepoRoot
    try {
        Invoke-DockerCompose -Arguments @('ps')
    }
    finally {
        Pop-Location
    }
}

function Stop-Compose {
    param(
        [switch]$DeleteVolumes
    )

    Push-Location $Script:RepoRoot
    try {
        Stop-Workload

        $args = @('down')
        if ($DeleteVolumes) {
            $args += '-v'
        }

        Invoke-DockerCompose -Arguments $args
    }
    finally {
        Pop-Location
    }
}

function Seed-Fixture {
    $rowCountText = $RowCount.ToString($Script:InvariantCulture)
    $hotPctText = $HotPct.ToString($Script:InvariantCulture)
    $templateCountText = $TemplateCount.ToString($Script:InvariantCulture)
    $blobTargetBytesText = $BlobTargetBytes.ToString($Script:InvariantCulture)
    $sql = "CALL bench.seed_fixture($rowCountText, $hotPctText, $templateCountText, $blobTargetBytesText, interval '$HistorySpan');"
    Invoke-Psql -Sql $sql | Write-Output
}

function Reset-Fixture {
    Invoke-Psql -Sql 'CALL bench.reset_fixture();' | Write-Output
}

function Show-Shape {
    Invoke-Psql -Sql 'SELECT source, is_hot, count(*) FROM bench.orders GROUP BY 1, 2 ORDER BY 1, 2;' -NoStopOnError | Write-Output
}

function Show-PayloadSize {
    Invoke-Psql -Sql "SELECT avg(octet_length(payload::text))::bigint AS avg_payload_text_bytes, min(octet_length(payload::text)) AS min_payload_text_bytes, max(octet_length(payload::text)) AS max_payload_text_bytes FROM (SELECT payload FROM bench.orders TABLESAMPLE SYSTEM (1) LIMIT 1000) AS sample_rows;" -NoStopOnError | Write-Output
}

function Show-ToastCheck {
    $sql = @"
WITH sample AS (
    SELECT
        pg_column_size(payload) AS stored_bytes,
        octet_length(payload::text) AS text_bytes,
        pg_column_compression(payload) AS compression
    FROM (
        SELECT payload
        FROM bench.orders
        TABLESAMPLE SYSTEM (1)
        LIMIT 1000
    ) AS sample_rows
),
toast_rel AS (
    SELECT c.reltoastrelid
    FROM pg_class AS c
    JOIN pg_namespace AS n
      ON n.oid = c.relnamespace
    WHERE n.nspname = 'bench'
      AND c.relname = 'orders'
)
SELECT
    count(*) AS sample_rows,
    avg(stored_bytes)::bigint AS avg_stored_bytes,
    avg(text_bytes)::bigint AS avg_payload_text_bytes,
    min(stored_bytes) AS min_stored_bytes,
    max(stored_bytes) AS max_stored_bytes,
    min(text_bytes) AS min_payload_text_bytes,
    max(text_bytes) AS max_payload_text_bytes,
    count(*) FILTER (WHERE stored_bytes <= 128) AS likely_external_rows,
    count(*) FILTER (WHERE compression IS NOT NULL) AS compressed_rows,
    round(avg(text_bytes::numeric / NULLIF(stored_bytes, 0)), 2) AS avg_payload_to_stored_ratio,
    pg_size_pretty(
        COALESCE(
            (SELECT pg_total_relation_size(reltoastrelid) FROM toast_rel WHERE reltoastrelid <> 0),
            0
        )
    ) AS toast_relation_total_size
FROM sample;
"@

    Invoke-Psql -Sql $sql -NoStopOnError | Write-Output
}

function Show-RelationSize {
    Invoke-Psql -Sql "SELECT pg_size_pretty(pg_table_size('bench.orders')) AS heap, pg_size_pretty(pg_indexes_size('bench.orders')) AS indexes, pg_size_pretty(pg_total_relation_size('bench.orders')) AS total;" -NoStopOnError | Write-Output
}

function Get-HotCount {
    $output = Invoke-Psql -Sql 'SELECT count(*) FROM bench.hot_seed_ids;' -At
    return [int64]($output | Select-Object -First 1)
}

function Get-WorkloadState {
    if (-not (Test-Path -LiteralPath $Script:WorkloadStatePath)) {
        return $null
    }

    return Get-Content -Raw -LiteralPath $Script:WorkloadStatePath | ConvertFrom-Json
}

function Save-WorkloadState {
    param(
        [Parameter(Mandatory = $true)]
        [pscustomobject]$State
    )

    Ensure-RuntimeDirs
    $State | ConvertTo-Json | Set-Content -LiteralPath $Script:WorkloadStatePath
}

function Remove-WorkloadState {
    if (Test-Path -LiteralPath $Script:WorkloadStatePath) {
        Remove-Item -LiteralPath $Script:WorkloadStatePath -Force
    }
}

function Stop-Workload {
    $state = Get-WorkloadState
    if ($null -eq $state) {
        Write-Host 'No detached workload processes are currently recorded.'
        return
    }

    $stoppedAny = $false
    foreach ($entry in $state.processes) {
        $proc = Get-Process -Id $entry.pid -ErrorAction SilentlyContinue
        if ($null -ne $proc) {
            Stop-Process -Id $entry.pid -Force
            Write-Host "Stopped $($entry.name) workload process PID $($entry.pid)."
            $stoppedAny = $true
        }
        else {
            Write-Host "$($entry.name) workload process PID $($entry.pid) is no longer running."
        }
    }

    Remove-WorkloadState

    if (-not $stoppedAny) {
        Write-Host 'No running workload processes were found, but the saved state was cleaned up.'
    }
}

function Start-Workload {
    Ensure-RuntimeDirs

    $existingState = Get-WorkloadState
    if ($null -ne $existingState) {
        $activePids = @($existingState.processes | Where-Object { Get-Process -Id $_.pid -ErrorAction SilentlyContinue })
        if ($activePids.Count -gt 0) {
            throw 'A detached workload is already running. Use .\scripts.ps1 StopWorkload before starting another one.'
        }

        Remove-WorkloadState
    }

    $hotCount = Get-HotCount
    if ($hotCount -lt 1) {
        throw 'bench.hot_seed_ids is empty. Seed the fixture first.'
    }

    $timestamp = Get-Date -Format 'yyyyMMdd-HHmmss'
    $updateLog = Join-Path $Script:LogsDir "workload-updates-$timestamp.stdout.log"
    $updateErrLog = Join-Path $Script:LogsDir "workload-updates-$timestamp.stderr.log"
    $insertLog = Join-Path $Script:LogsDir "workload-inserts-$timestamp.stdout.log"
    $insertErrLog = Join-Path $Script:LogsDir "workload-inserts-$timestamp.stderr.log"

    $updateArgs = @(
        'compose', 'exec', '-T', 'postgres',
        'pgbench', '-U', 'postgres', '-d', 'bench', '-n',
        '-c', $UpdateClients.ToString(),
        '-j', $UpdateThreads.ToString(),
        '-T', $WorkloadSeconds.ToString(),
        '-D', "hot_count=$hotCount",
        '-f', '/pgbench/oltp_updates.sql'
    )

    $insertArgs = @(
        'compose', 'exec', '-T', 'postgres',
        'pgbench', '-U', 'postgres', '-d', 'bench', '-n',
        '-c', $InsertClients.ToString(),
        '-j', $InsertThreads.ToString(),
        '-T', $WorkloadSeconds.ToString(),
        '-f', '/pgbench/oltp_inserts.sql'
    )

    $updateProc = Start-Process -FilePath 'docker' -ArgumentList $updateArgs -WorkingDirectory $Script:RepoRoot -RedirectStandardOutput $updateLog -RedirectStandardError $updateErrLog -PassThru
    $insertProc = Start-Process -FilePath 'docker' -ArgumentList $insertArgs -WorkingDirectory $Script:RepoRoot -RedirectStandardOutput $insertLog -RedirectStandardError $insertErrLog -PassThru

    $state = [pscustomobject]@{
        startedAt = (Get-Date).ToString('o')
        workloadSeconds = $WorkloadSeconds
        hotCount = $hotCount
        logs = [pscustomobject]@{
            updatesStdout = $updateLog
            updatesStderr = $updateErrLog
            insertsStdout = $insertLog
            insertsStderr = $insertErrLog
        }
        processes = @(
            [pscustomobject]@{
                name = 'updates'
                pid = $updateProc.Id
                stdoutLog = $updateLog
                stderrLog = $updateErrLog
            },
            [pscustomobject]@{
                name = 'inserts'
                pid = $insertProc.Id
                stdoutLog = $insertLog
                stderrLog = $insertErrLog
            }
        )
    }

    Save-WorkloadState -State $state

    Write-Host "Started updates workload PID $($updateProc.Id). Stdout: $updateLog"
    Write-Host "Started updates workload PID $($updateProc.Id). Stderr: $updateErrLog"
    Write-Host "Started inserts workload PID $($insertProc.Id). Stdout: $insertLog"
    Write-Host "Started inserts workload PID $($insertProc.Id). Stderr: $insertErrLog"
}

function Invoke-BackfillGuid {
    Invoke-Psql -Sql "CALL bench.backfill_guid_pk_order($BatchSize, $LogEvery);" | Write-Output
}

function Invoke-BackfillCtid {
    Invoke-Psql -Sql "CALL bench.backfill_ctid_order($BatchSize, $LogEvery);" | Write-Output
}

function Invoke-BackfillCtidLive {
    Invoke-Psql -Sql "CALL bench.backfill_ctid_live_cursor($BatchSize, $LogEvery);" | Write-Output
}

function Show-Results {
    Invoke-Psql -Sql "SELECT variant, batch_size, queue_rows, rows_processed, rows_updated, queue_build_ms, elapsed_ms, rows_processed_per_sec, rows_updated_per_sec, started_at, finished_at FROM bench.benchmark_runs WHERE variant IN ('guid', 'ctid', 'ctid_live') ORDER BY started_at DESC LIMIT $ResultLimit;" -NoStopOnError | Write-Output
}

function Invoke-Compare {
    Write-Host 'Seeding fixture for GUID-ordered backfill...'
    Seed-Fixture
    Start-Workload
    try {
        Write-Host 'Running GUID-ordered backfill...'
        Invoke-BackfillGuid
    }
    finally {
        Stop-Workload
    }

    Write-Host 'Seeding fixture for CTID-ordered backfill...'
    Seed-Fixture
    Start-Workload
    try {
        Write-Host 'Running CTID-ordered backfill...'
        Invoke-BackfillCtid
    }
    finally {
        Stop-Workload
    }

    Show-Results
}

function Show-HelpText {
    @'
Usage:
  .\scripts.ps1 Up
  .\scripts.ps1 Seed -RowCount 1000000 -BlobTargetBytes 2500
  .\scripts.ps1 StartWorkload -WorkloadSeconds 900
  .\scripts.ps1 BackfillGuid -BatchSize 1000 -LogEvery 100
  .\scripts.ps1 BackfillCtid -BatchSize 1000 -LogEvery 100
  .\scripts.ps1 BackfillCtidLive -BatchSize 1000 -LogEvery 100
  .\scripts.ps1 Results -ResultLimit 10
  .\scripts.ps1 Compare -RowCount 1000000 -BatchSize 1000 -WorkloadSeconds 600
  .\scripts.ps1 StopWorkload
  .\scripts.ps1 Destroy

Actions:
  Up            Start PostgreSQL via docker compose and show container status.
  Ps            Show docker compose status.
  Down          Stop PostgreSQL and leave the volume intact.
  Destroy       Stop PostgreSQL and delete the Docker volume.
  Seed          Seed bench.orders with the current fixture parameters.
  Reset         Remove OLTP rows and reset seeded rows to extracted_at = NULL.
  Shape         Show counts by source and hot flag.
  PayloadSize   Sample raw payload text sizes.
  ToastCheck    Sample payload storage size and toast table size.
  RelationSize  Show heap, index, and total relation sizes.
  HotCount      Print the number of hot seed rows used by the update workload.
  StartWorkload Start detached pgbench insert and update workloads.
  StopWorkload  Stop detached workload processes started by this script.
  BackfillGuid  Run the GUID PK ordered backfill procedure.
  BackfillCtid  Run the CTID ordered backfill procedure.
  BackfillCtidLive Run the live CTID cursor backfill procedure.
  Results       Show recent rows from bench.benchmark_runs.
  Compare       Reseed and run GUID, then reseed and run CTID, each with workload.

Common parameters:
  -RowCount
  -HotPct
  -TemplateCount
  -BlobTargetBytes
  -HistorySpan
  -BatchSize
  -LogEvery
  -UpdateClients
  -UpdateThreads
  -InsertClients
  -InsertThreads
  -WorkloadSeconds
  -ResultLimit
'@ | Write-Host
}

switch ($Action) {
    'Help' { Show-HelpText }
    'Up' { Start-Compose }
    'Ps' { Show-ComposePs }
    'Down' { Stop-Compose }
    'Destroy' { Stop-Compose -DeleteVolumes }
    'Seed' { Seed-Fixture }
    'Reset' { Reset-Fixture }
    'Shape' { Show-Shape }
    'PayloadSize' { Show-PayloadSize }
    'ToastCheck' { Show-ToastCheck }
    'RelationSize' { Show-RelationSize }
    'HotCount' { Get-HotCount | Write-Output }
    'StartWorkload' { Start-Workload }
    'StopWorkload' { Stop-Workload }
    'BackfillGuid' { Invoke-BackfillGuid }
    'BackfillCtid' { Invoke-BackfillCtid }
    'BackfillCtidLive' { Invoke-BackfillCtidLive }
    'Results' { Show-Results }
    'Compare' { Invoke-Compare }
    default { throw "Unsupported action: $Action" }
}
