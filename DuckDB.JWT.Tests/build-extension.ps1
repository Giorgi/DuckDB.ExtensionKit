#!/usr/bin/env pwsh

<#
.SYNOPSIS
    Builds the DuckDB JWT extension with Native AOT for the current platform.

.DESCRIPTION
    This script detects the current platform (OS and architecture) and builds
    the DuckDB.JWT extension with Native AOT compilation. The compiled extension
    will be available for the test project to load.

.PARAMETER Configuration
    The build configuration (Debug or Release). Defaults to Release.

.EXAMPLE
    .\build-extension.ps1
    Builds the extension in Release mode for the current platform.

.EXAMPLE
    .\build-extension.ps1 -Configuration Debug
    Builds the extension in Debug mode.
#>

param(
    [Parameter()]
    [ValidateSet('Debug', 'Release')]
    [string]$Configuration = 'Release'
)

$ErrorActionPreference = 'Stop'

# Detect the current platform
$os = [System.Environment]::OSVersion.Platform
$arch = [System.Runtime.InteropServices.RuntimeInformation]::ProcessArchitecture

$runtimeIdentifier = switch ($os) {
    'Win32NT' {
        switch ($arch) {
            'X64' { 'win-x64' }
            'Arm64' { 'win-arm64' }
            default { throw "Unsupported Windows architecture: $arch" }
        }
    }
    'Unix' {
        if ([System.Runtime.InteropServices.RuntimeInformation]::IsOSPlatform([System.Runtime.InteropServices.OSPlatform]::Linux)) {
            switch ($arch) {
                'X64' { 'linux-x64' }
                'Arm64' { 'linux-arm64' }
                default { throw "Unsupported Linux architecture: $arch" }
            }
        }
        elseif ([System.Runtime.InteropServices.RuntimeInformation]::IsOSPlatform([System.Runtime.InteropServices.OSPlatform]::OSX)) {
            switch ($arch) {
                'X64' { 'osx-x64' }
                'Arm64' { 'osx-arm64' }
                default { throw "Unsupported macOS architecture: $arch" }
            }
        }
        else {
            throw "Unsupported Unix platform"
        }
    }
    default {
        throw "Unsupported platform: $os"
    }
}

Write-Host "Building DuckDB JWT extension for $runtimeIdentifier..." -ForegroundColor Cyan

$projectPath = Join-Path $PSScriptRoot '..\DuckDB.JWT\DuckDB.JWT.csproj'

dotnet publish $projectPath -c $Configuration -r $runtimeIdentifier 

if ($LASTEXITCODE -eq 0) {
    Write-Host "Extension built successfully!" -ForegroundColor Green
    Write-Host "Extension location: ..\DuckDB.JWT\bin\$Configuration\net10.0\$runtimeIdentifier\publish\jwt.duckdb_extension" -ForegroundColor Green
}
else {
    Write-Host "Extension build failed!" -ForegroundColor Red
    exit $LASTEXITCODE
}
