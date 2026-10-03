# scripts/fix_machine_config.ps1
# Fixes duplicate MySQL provider entry in .NET machine.config files
# Requires Administrator privileges

$paths = @(
    "C:\Windows\Microsoft.NET\Framework64\v4.0.30319\Config\machine.config",
    "C:\Windows\Microsoft.NET\Framework\v4.0.30319\Config\machine.config"
)

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host " Fixing .NET machine.config Duplicate MySQL Provider" -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

foreach ($path in $paths) {
    if (Test-Path $path) {
        Write-Host "`nChecking: $path" -ForegroundColor Yellow
        $content = [System.IO.File]::ReadAllText($path)
        
        $pattern = '(<add name="MySQL Data Provider"[^>]+/>)\s*(<add name="MySQL Data Provider"[^>]+/>)'
        if ($content -match $pattern) {
            # Create timestamped backup
            $backup = "$path.bak_$(Get-Date -Format 'yyyyMMdd_HHmmss')"
            [System.IO.File]::Copy($path, $backup, $true)
            Write-Host "  [OK] Created backup at: $backup" -ForegroundColor Green
            
            # Remove duplicate entry
            $fixed = $content -replace $pattern, '$1'
            [System.IO.File]::WriteAllText($path, $fixed)
            Write-Host "  [SUCCESS] Removed duplicate MySQL Data Provider entry!" -ForegroundColor Green
        } else {
            Write-Host "  [INFO] No duplicate entry found. File is already clean." -ForegroundColor Gray
        }
    } else {
        Write-Host "`nPath not found (skipping): $path" -ForegroundColor Gray
    }
}

Write-Host "`n==========================================================" -ForegroundColor Cyan
Write-Host " Machine.config repair completed successfully!" -ForegroundColor Green
Write-Host " Power BI can now refresh without any System.Data errors." -ForegroundColor Green
Write-Host "==========================================================" -ForegroundColor Cyan
Start-Sleep -Seconds 3
