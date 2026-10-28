$v = (Get-Item 'D:\workspace\fxr90-mqtt\dist\FXR90Reader.exe').VersionInfo
$lines = @(
  ('CompanyName=' + $v.CompanyName),
  ('ProductName=' + $v.ProductName),
  ('FileDescription=' + $v.FileDescription),
  ('FileVersion=' + $v.FileVersion),
  ('ProductVersion=' + $v.ProductVersion),
  ('LegalCopyright=' + $v.LegalCopyright),
  ('OriginalFilename=' + $v.OriginalFilename)
)
[System.IO.File]::WriteAllLines('D:\workspace\fxr90-mqtt\build\verinfo.txt', $lines, (New-Object System.Text.UTF8Encoding($false)))
Write-Output 'written'
