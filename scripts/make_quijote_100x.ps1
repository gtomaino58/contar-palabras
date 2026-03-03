1..100 | ForEach-Object { Get-Content data\el_quijote.txt } | Set-Content data\el_quijote_100x.txt
Write-Host "✅ Generado data\el_quijote_100x.txt"