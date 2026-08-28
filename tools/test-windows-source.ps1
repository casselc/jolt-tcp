#requires -version 5
<#
.SYNOPSIS
  Resolve jolt-tcp's test graph and run one checked-in source-mode Windows gate.

.DESCRIPTION
  Invokes native Chez directly. It does not route native paths or Clojure source
  through a bash wrapper, and it never relies on the current `/dev/stdin`
  implementation of `jolt -`.
#>
param(
  [string]$JoltTcpPath = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path,
  [string]$RuntimePath = "D:\src\jolt",
  [string]$ChezExe = "D:\chez-10.4.1\bin\scheme.exe",
  [string]$TestAlias = "-M:windows-portable-test",
  [switch]$InstallHegel,
  [int]$TimeoutSeconds = 180
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path $ChezExe)) {
  throw "test-windows-source.ps1: scheme.exe not found at $ChezExe"
}
if (-not (Test-Path (Join-Path $RuntimePath "host\chez\cli.ss"))) {
  throw "test-windows-source.ps1: host\chez\cli.ss not found under $RuntimePath"
}
if ($TimeoutSeconds -le 0) {
  throw "test-windows-source.ps1: TimeoutSeconds must be positive"
}
if (-not $TestAlias.StartsWith("-M:")) {
  throw "test-windows-source.ps1: TestAlias must be a -M: alias"
}

$env:JOLT_AOT_CACHE = "0"
$env:JOLT_VERSION = "v0.7.28"
$env:JOLT_SH = "C:\Program Files\Git\bin\sh.exe"

function Invoke-Jolt {
  param(
    [string]$Phase,
    [string[]]$JoltArgs
  )

  Write-Host "$Phase"
  $arguments = @("--script", "host\chez\cli.ss") + $JoltArgs
  $process = Start-Process `
    -FilePath $ChezExe `
    -ArgumentList $arguments `
    -NoNewWindow `
    -PassThru
  # PowerShell 5.1 does not reliably populate .ExitCode unless the native
  # process handle has first been materialized. Without this read, a failing
  # gate can be mistaken for success because $null compares equal to zero.
  $null = $process.Handle
  if ($process.WaitForExit($TimeoutSeconds * 1000)) {
    $exitCode = $process.ExitCode
    if ($null -eq $exitCode) {
      throw "$Phase observed no process exit code; refusing to report success"
    }
    if ($exitCode -ne 0) {
      throw "$Phase failed with exit code $exitCode"
    }
  }
  else {
    [Console]::Error.WriteLine(
      "$Phase timed out after $TimeoutSeconds seconds; terminating PID $($process.Id)"
    )
    try {
      $process.Kill()
      $process.WaitForExit()
    }
    catch {
      [Console]::Error.WriteLine(
        "failed to terminate timed-out PID $($process.Id): $($_.Exception.Message)"
      )
    }
    throw "$Phase timed out"
  }
}

Push-Location $RuntimePath
try {
  # Jolt v0.7.28 host I/O recognizes Windows drive and UNC paths, so retain the
  # real absolute project directory.
  $env:JOLT_PWD = (Resolve-Path $JoltTcpPath).Path

  Write-Host "jolt-tcp native Windows source gate"
  Write-Host "  JOLT_PWD = $env:JOLT_PWD"
  Write-Host "  runtime  = $RuntimePath"
  Write-Host "  scheme   = $ChezExe"
  Write-Host "  alias    = $TestAlias"
  Write-Host "  hegel    = $InstallHegel"
  Write-Host ""

  if ($InstallHegel) {
    $installAlias = "-A:" + $TestAlias.Substring(3)
    Invoke-Jolt -Phase "Install libhegel for $installAlias" `
      -JoltArgs @($installAlias, "-m", "hegel.install")
  }
  Invoke-Jolt -Phase "Run $TestAlias" -JoltArgs @($TestAlias)
}
finally {
  Pop-Location
}
