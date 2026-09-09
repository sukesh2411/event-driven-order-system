$ErrorActionPreference = 'Stop'

$workspace = Split-Path -Parent $MyInvocation.MyCommand.Path

Write-Host "Starting infrastructure..."
Set-Location $workspace
docker compose up -d postgres kafka

$services = @(
    @{ Name = 'order-service'; Path = Join-Path $workspace 'order-service' },
    @{ Name = 'inventory-service'; Path = Join-Path $workspace 'inventory-service' },
    @{ Name = 'payment-service'; Path = Join-Path $workspace 'payment-service' }
)

foreach ($svc in $services) {
    Write-Host "Starting $($svc.Name)..."
    Start-Process powershell -ArgumentList '-NoExit', '-Command', "cd '$($svc.Path)'; `$env:TZ='Asia/Kolkata'; `$env:JAVA_TOOL_OPTIONS='-Duser.timezone=Asia/Kolkata'; mvn spring-boot:run"
}

Write-Host "All services started in separate PowerShell windows."
Write-Host "Use the following before running Maven manually:"
Write-Host "  `$env:TZ='Asia/Kolkata'"
Write-Host "  `$env:JAVA_TOOL_OPTIONS='-Duser.timezone=Asia/Kolkata'"
