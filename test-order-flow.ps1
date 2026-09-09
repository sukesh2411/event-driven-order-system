$ErrorActionPreference = 'Stop'

Write-Host "Starting end-to-end order flow checks..."

$cases = @(
    @{ id = 'ORD-SUCCESS-5001'; product = 'Phone'; qty = 2; label = 'happy-path' },
    @{ id = 'ORD-PAY-FAIL-5002'; product = 'Laptop'; qty = 1; label = 'payment-failure' },
    @{ id = 'ORD-INV-FAIL-5003'; product = 'Phone'; qty = 999; label = 'inventory-failure' }
)

foreach ($case in $cases) {
    $body = [ordered]@{
        orderId = $case.id
        product = $case.product
        quantity = $case.qty
    } | ConvertTo-Json -Compress

    Write-Host "=== $($case.label) :: $($case.id) ==="

    try {
        $response = Invoke-RestMethod -Uri 'http://localhost:8081/orders' -Method POST -ContentType 'application/json' -Body $body
        Write-Host "POST response: $response"
    }
    catch {
        Write-Host "POST failed: $($_.Exception.Message)"
    }

    Start-Sleep -Seconds 5

    try {
        $db = Invoke-RestMethod -Uri ("http://localhost:8081/orders/$($case.id)") -Method GET
        $db | Format-Table -AutoSize | Out-String
    }
    catch {
        Write-Host "GET failed: $($_.Exception.Message)"
    }

    Write-Host ""
}

Write-Host "End-to-end checks complete."
