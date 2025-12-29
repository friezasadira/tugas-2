# docker/scripts/demo_lock.ps1
# Usage: powershell -ExecutionPolicy Bypass -File .\docker\scripts\demo_lock.ps1

$ports = @(8001, 8002, 8003)

function Get-LeaderPort {
  foreach ($p in $ports) {
    try {
      $st = Invoke-RestMethod "http://localhost:$p/raft/status"
      if ($st.role -eq "leader") { return $p }
    } catch {}
  }
  throw "Leader not found on ports: $($ports -join ', ')"
}

function Invoke-LeaderOnly($method, $path, $bodyJson) {
  for ($i=0; $i -lt 6; $i++) {
    $leaderPort = Get-LeaderPort
    $url = "http://localhost:$leaderPort$path"

    try {
      if ($method -eq "GET") {
        return Invoke-RestMethod $url
      } else {
        return Invoke-RestMethod -Method $method -Uri $url -ContentType "application/json" -Body $bodyJson
      }
    } catch {
      $resp = $_.Exception.Response
      if ($resp -and $resp.StatusCode.value__ -eq 409) {
        Start-Sleep -Milliseconds 200
        continue
      }
      throw
    }
  }
  throw "Request failed after retries: $method $path"
}

$leader = Get-LeaderPort
Write-Host "Leader detected on port: $leader"

Write-Host "`n--- Create deadlock scenario ---"
Invoke-LeaderOnly "POST" "/locks/acquire" '{"resource":"A","mode":"X","client_id":"c1"}' | Out-Host
Invoke-LeaderOnly "POST" "/locks/acquire" '{"resource":"B","mode":"X","client_id":"c2"}' | Out-Host
Invoke-LeaderOnly "POST" "/locks/acquire" '{"resource":"B","mode":"X","client_id":"c1"}' | Out-Host
Invoke-LeaderOnly "POST" "/locks/acquire" '{"resource":"A","mode":"X","client_id":"c2"}' | Out-Host

Write-Host "`nWaiting 1.2s for deadlock detector..."
Start-Sleep -Milliseconds 1200

Write-Host "`n--- /locks/wfg ---"
Invoke-LeaderOnly "GET" "/locks/wfg" $null | ConvertTo-Json -Depth 8 | Write-Host

Write-Host "`n--- /locks/state ---"
Invoke-LeaderOnly "GET" "/locks/state" $null | ConvertTo-Json -Depth 8 | Write-Host
