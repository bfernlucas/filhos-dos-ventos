$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path

function Resolve-CommandPath {
    param(
        [string[]]$Candidates,
        [string]$CommandName
    )

    foreach ($candidate in $Candidates) {
        if ($candidate -and (Test-Path $candidate)) {
            return $candidate
        }
    }

    $cmd = Get-Command $CommandName -ErrorAction SilentlyContinue
    if ($cmd) {
        return $cmd.Source
    }

    return $null
}

$pythonExe = Resolve-CommandPath @($env:PYTHON_EXE, "C:\OSGeo4W\apps\Python312\python.exe") "python"
$ogr2ogrExe = Resolve-CommandPath @($env:OGR2OGR_EXE, "C:\OSGeo4W\bin\ogr2ogr.exe") "ogr2ogr"

if (-not $pythonExe) {
    throw "Python nao encontrado. Defina PYTHON_EXE ou instale Python com as dependencias do projeto."
}

if (-not $ogr2ogrExe) {
    throw "ogr2ogr nao encontrado. Defina OGR2OGR_EXE ou instale GDAL/OSGeo4W."
}

$osgeoRoot = $null
if ((Test-Path "C:\OSGeo4W\bin") -and (($pythonExe -like "C:\OSGeo4W\*") -or ($ogr2ogrExe -like "C:\OSGeo4W\*"))) {
    $osgeoRoot = "C:\OSGeo4W"
}

if ($osgeoRoot -and (Test-Path (Join-Path $osgeoRoot "bin"))) {
    $env:PATH = "$osgeoRoot\apps\qt5\bin;$osgeoRoot\apps\Python312\Scripts;$osgeoRoot\bin;$env:PATH"
    if (Test-Path (Join-Path $osgeoRoot "apps\Python312")) { $env:PYTHONHOME = "$osgeoRoot\apps\Python312" }
    if (Test-Path (Join-Path $osgeoRoot "apps\gdal\share\gdal")) { $env:GDAL_DATA = "$osgeoRoot\apps\gdal\share\gdal" }
    if (Test-Path (Join-Path $osgeoRoot "apps\gdal\lib\gdalplugins")) { $env:GDAL_DRIVER_PATH = "$osgeoRoot\apps\gdal\lib\gdalplugins" }
    if (Test-Path (Join-Path $osgeoRoot "share\proj")) { $env:PROJ_DATA = "$osgeoRoot\share\proj" }
}

$env:PYTHONUTF8 = "1"

$logDir = Join-Path $projectRoot "logs"
$vectorDir = Join-Path $projectRoot "data\processed\vector_exports"
New-Item -ItemType Directory -Force -Path $logDir | Out-Null
New-Item -ItemType Directory -Force -Path $vectorDir | Out-Null

$logFile = Join-Path $logDir "pipeline_latest.log"
$gpkg = Join-Path $projectRoot "data\processed\nordeste_energia_eolica.gpkg"
$windKml = Join-Path $vectorDir "potencial_eolico_nordeste.kml"
$windKmz = Join-Path $vectorDir "potencial_eolico_nordeste.kmz"
$aneelKml = Join-Path $vectorDir "empreendimentos_eolicos_aneel_nordeste.kml"
$aneelKmz = Join-Path $vectorDir "empreendimentos_eolicos_aneel_nordeste.kmz"

$requiredFiles = @(
    (Join-Path $projectRoot "data\raw\geobr\municipios_nordeste\AL\AL_Municipios_2025.shp"),
    (Join-Path $projectRoot "data\raw\geobr\ufs_nordeste\AL\AL_UF_2025.shp"),
    (Join-Path $projectRoot "data\raw\semiarido\municipios_uf_poligono_sab_sudene.rar"),
    (Join-Path $projectRoot "data\raw\wind\dados_gerais_RNordeste.kmz"),
    (Join-Path $projectRoot "data\raw\aneel\siga-empreendimentos-geracao.csv"),
    (Join-Path $projectRoot "data\raw\censo_2010\resultados_universo_municipios\alagoas.zip")
)

if (($requiredFiles | Where-Object { -not (Test-Path $_) }).Count -gt 0) {
    & (Join-Path $projectRoot "src\bootstrap_raw_data.ps1")
}

& $pythonExe (Join-Path $projectRoot "src\python\build_spatial_base.py") 2>&1 | Tee-Object -FilePath $logFile
& $pythonExe (Join-Path $projectRoot "src\python\validate_outputs.py") 2>&1 | Tee-Object -FilePath $logFile -Append

foreach ($f in @($windKml, $windKmz, $aneelKml, $aneelKmz)) {
    if (Test-Path $f) { Remove-Item -LiteralPath $f -Force }
}

& $ogr2ogrExe -q -f LIBKML $windKml $gpkg wind_points 2>&1 | Tee-Object -FilePath $logFile -Append
& $ogr2ogrExe -q -f LIBKML $windKmz $gpkg wind_points 2>&1 | Tee-Object -FilePath $logFile -Append
& $ogr2ogrExe -q -f LIBKML $aneelKml $gpkg aneel_eol_points 2>&1 | Tee-Object -FilePath $logFile -Append
& $ogr2ogrExe -q -f LIBKML $aneelKmz $gpkg aneel_eol_points 2>&1 | Tee-Object -FilePath $logFile -Append

& $pythonExe (Join-Path $projectRoot "src\python\build_panel_2016_2025.py") 2>&1 | Tee-Object -FilePath $logFile -Append

Write-Host "Pipeline concluido. Veja o log em $logFile"
