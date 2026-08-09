[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"

$repositoryRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$mysqlContainer = "fable-test-mysql"
$mysqlPassword = "mychat-dev-password"
$previousPath = $env:Path
$previousCGOEnabled = $env:CGO_ENABLED
$previousCC = $env:CC

function Invoke-Checked {
    param(
        [Parameter(Mandatory = $true)]
        [scriptblock]$Command,

        [Parameter(Mandatory = $true)]
        [string]$FailureMessage
    )

    & $Command
    if ($LASTEXITCODE -ne 0) {
        throw $FailureMessage
    }
}

function Test-DockerReady {
    $previousErrorActionPreference = $ErrorActionPreference
    $ErrorActionPreference = "SilentlyContinue"
    docker info *> $null
    $ready = $LASTEXITCODE -eq 0
    $ErrorActionPreference = $previousErrorActionPreference
    return $ready
}

function Test-MySQLReady {
    $previousErrorActionPreference = $ErrorActionPreference
    $ErrorActionPreference = "SilentlyContinue"
    docker exec $mysqlContainer mysqladmin ping `
        -h 127.0.0.1 `
        -uroot `
        "-p$mysqlPassword" `
        --silent *> $null
    $ready = $LASTEXITCODE -eq 0
    $ErrorActionPreference = $previousErrorActionPreference
    return $ready
}

function Wait-Docker {
    if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
        throw "docker was not found. Install Docker Desktop before running the test suite."
    }

    if (Test-DockerReady) {
        return
    }

    $dockerDesktop = "C:\Program Files\Docker\Docker\Docker Desktop.exe"
    if (-not (Test-Path -LiteralPath $dockerDesktop)) {
        throw "Docker is installed but its engine is not running. Start Docker and retry."
    }

    Write-Host "Starting Docker Desktop..."
    Start-Process -FilePath $dockerDesktop -WindowStyle Hidden
    for ($attempt = 0; $attempt -lt 30; $attempt++) {
        Start-Sleep -Seconds 2
        if (Test-DockerReady) {
            return
        }
    }

    throw "Docker did not become ready within 60 seconds."
}

function Start-TestDatabase {
    $existingContainer = docker ps -a --filter "name=^/$mysqlContainer$" --format "{{.Names}}"
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to inspect Docker containers."
    }

    if ($existingContainer -ne $mysqlContainer) {
        Write-Host "Creating MySQL test container..."
        Invoke-Checked -FailureMessage "Failed to create the MySQL test container." -Command {
            docker run -d `
                --name $mysqlContainer `
                --restart unless-stopped `
                -e "MYSQL_ROOT_PASSWORD=$mysqlPassword" `
                -e "MYSQL_DATABASE=mychat_test" `
                -p "127.0.0.1:3306:3306" `
                mysql:8.4
        }
    }
    else {
        $runningContainer = docker ps --filter "name=^/$mysqlContainer$" --format "{{.Names}}"
        if ($runningContainer -ne $mysqlContainer) {
            Write-Host "Starting MySQL test container..."
            Invoke-Checked -FailureMessage "Failed to start the MySQL test container." -Command {
                docker start $mysqlContainer
            }
        }
    }

    Write-Host "Waiting for MySQL..."
    for ($attempt = 0; $attempt -lt 30; $attempt++) {
        if (Test-MySQLReady) {
            return
        }
        Start-Sleep -Seconds 2
    }

    throw "MySQL did not become ready within 60 seconds. Run 'docker logs $mysqlContainer' for details."
}

function Find-CCompiler {
    $compiler = Get-Command gcc -ErrorAction SilentlyContinue
    if ($compiler) {
        return $compiler.Source
    }

    $knownCompilers = @(
        "C:\msys64\ucrt64\bin\gcc.exe",
        "C:\msys64\mingw64\bin\gcc.exe"
    )
    foreach ($knownCompiler in $knownCompilers) {
        if (Test-Path -LiteralPath $knownCompiler) {
            return $knownCompiler
        }
    }

    throw "A 64-bit GCC compiler is required by 'go test -race'. Install the MSYS2 UCRT64 GCC package and retry."
}

Push-Location $repositoryRoot
try {
    Wait-Docker
    Start-TestDatabase

    Write-Host "Running unit tests..."
    Invoke-Checked -FailureMessage "Unit tests failed." -Command {
        go test -count=1 ./...
    }

    Write-Host "Running integration tests..."
    Invoke-Checked -FailureMessage "Integration tests failed." -Command {
        go test -count=1 -tags=integration ./internal/integration
    }

    $compiler = Find-CCompiler
    $env:Path = "$(Split-Path -Parent $compiler);$env:Path"
    $env:CGO_ENABLED = "1"
    $env:CC = $compiler

    Write-Host "Running race-detector tests with $compiler..."
    Invoke-Checked -FailureMessage "Race-detector tests failed." -Command {
        go test -count=1 -race ./...
    }

    Write-Host "All tests passed."
}
finally {
    $env:Path = $previousPath
    $env:CGO_ENABLED = $previousCGOEnabled
    $env:CC = $previousCC
    Pop-Location
}
