$ErrorActionPreference = "Stop"
$ProgressPreference = "SilentlyContinue"

$projectRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)

function Ensure-Dir {
    param([string]$Path)
    New-Item -ItemType Directory -Force -Path $Path | Out-Null
}

function Download-IfMissing {
    param(
        [string]$Url,
        [string]$OutFile
    )

    if (Test-Path $OutFile) {
        Write-Host "Preservando arquivo existente: $OutFile"
        return
    }

    Ensure-Dir (Split-Path -Parent $OutFile)
    Write-Host "Baixando: $Url"
    Invoke-WebRequest -Uri $Url -OutFile $OutFile -UseBasicParsing
}

function Resolve-Extractor {
    param([string]$CommandName, [string[]]$Candidates)

    foreach ($candidate in $Candidates) {
        if ($candidate -and (Test-Path $candidate)) { return $candidate }
    }
    $cmd = Get-Command $CommandName -ErrorAction SilentlyContinue
    if ($cmd) { return $cmd.Source }
    return $null
}

function Extract-IfMissing {
    param(
        [string]$ArchivePath,
        [string]$Destination,
        [string]$Sentinel
    )

    if (Test-Path $Sentinel) {
        Write-Host "Preservando extracao existente: $Destination"
        return
    }

    Ensure-Dir $Destination
    Write-Host "Extraindo: $ArchivePath"

    $extension = [System.IO.Path]::GetExtension($ArchivePath).ToLowerInvariant()
    switch ($extension) {
        ".zip" {
            # Expand-Archive e nativo e cobre os arquivos do IBGE (municipios, UFs, Censo).
            Expand-Archive -LiteralPath $ArchivePath -DestinationPath $Destination -Force
        }
        ".rar" {
            # tar do Windows/bsdtar nao suporta RAR. Usamos 7-Zip ou UnRAR explicitamente.
            $sevenZip = Resolve-Extractor "7z" @(
                $env:SEVENZIP_EXE,
                "C:\Program Files\7-Zip\7z.exe",
                "C:\Program Files (x86)\7-Zip\7z.exe"
            )
            if ($sevenZip) {
                & $sevenZip x -y "-o$Destination" $ArchivePath | Out-Null
                if ($LASTEXITCODE -ne 0) { throw "7-Zip falhou ao extrair $ArchivePath (codigo $LASTEXITCODE)." }
                return
            }

            $unrar = Resolve-Extractor "unrar" @($env:UNRAR_EXE, "C:\Program Files\WinRAR\UnRAR.exe")
            if ($unrar) {
                & $unrar x -y $ArchivePath "$Destination\" | Out-Null
                if ($LASTEXITCODE -ne 0) { throw "UnRAR falhou ao extrair $ArchivePath (codigo $LASTEXITCODE)." }
                return
            }

            throw "Arquivo .rar detectado ($ArchivePath), mas nenhum extrator foi encontrado. Instale 7-Zip ou UnRAR, ou defina SEVENZIP_EXE/UNRAR_EXE."
        }
        default {
            # Fallback generico para formatos suportados por bsdtar (tar, tar.gz, etc.).
            tar -xf $ArchivePath -C $Destination
            if ($LASTEXITCODE -ne 0) { throw "tar falhou ao extrair $ArchivePath (codigo $LASTEXITCODE)." }
        }
    }
}

$ufs = @("AL","BA","CE","MA","PB","PE","PI","RN","SE")
$stateNames = @{
    "AL" = "alagoas"
    "BA" = "bahia"
    "CE" = "ceara"
    "MA" = "maranhao"
    "PB" = "paraiba"
    "PE" = "pernambuco"
    "PI" = "piaui"
    "RN" = "rio_grande_do_norte"
    "SE" = "sergipe"
}

$geobrDownloads = Join-Path $projectRoot "data\raw\geobr\downloads"
$municipiosRoot = Join-Path $projectRoot "data\raw\geobr\municipios_nordeste"
$ufsRoot = Join-Path $projectRoot "data\raw\geobr\ufs_nordeste"
$semiaridoRaw = Join-Path $projectRoot "data\raw\semiarido\municipios_uf_poligono_sab_sudene.rar"
$semiaridoInterim = Join-Path $projectRoot "data\interim\semiarido_extraido"
$windKmz = Join-Path $projectRoot "data\raw\wind\dados_gerais_RNordeste.kmz"
$aneelCsv = Join-Path $projectRoot "data\raw\aneel\siga-empreendimentos-geracao.csv"
$censoRoot = Join-Path $projectRoot "data\raw\censo_2010\resultados_universo_municipios"

foreach ($uf in $ufs) {
    $munZip = Join-Path $geobrDownloads "municipios\$uf`_Municipios_2025.zip"
    $ufZip = Join-Path $geobrDownloads "ufs\$uf`_UF_2025.zip"

    Download-IfMissing "https://geoftp.ibge.gov.br/organizacao_do_territorio/malhas_territoriais/malhas_municipais/municipio_2025/UFs/$uf/$uf`_Municipios_2025.zip" $munZip
    Download-IfMissing "https://geoftp.ibge.gov.br/organizacao_do_territorio/malhas_territoriais/malhas_municipais/municipio_2025/UFs/$uf/$uf`_UF_2025.zip" $ufZip

    $munDir = Join-Path $municipiosRoot $uf
    $ufDir = Join-Path $ufsRoot $uf
    Extract-IfMissing $munZip $munDir (Join-Path $munDir "$uf`_Municipios_2025.shp")
    Extract-IfMissing $ufZip $ufDir (Join-Path $ufDir "$uf`_UF_2025.shp")
}

Download-IfMissing "https://www.gov.br/insa/pt-br/centrais-de-conteudo/mapas/mapas-em-shapefile/municipios_uf_poligono_sab_sudene.rar/@@download/file" $semiaridoRaw
Extract-IfMissing $semiaridoRaw $semiaridoInterim (Join-Path $semiaridoInterim "LIM_Semiarido_Municipal_OFICIAL.shp")

Download-IfMissing "https://novoatlas.cepel.br/wp-content/uploads/2017/03/dados_gerais_RNordeste.kmz" $windKmz
Download-IfMissing "https://dadosabertos.aneel.gov.br/dataset/6d90b77c-c5f5-4d81-bdec-7bc619494bb9/resource/11ec447d-698d-4ab8-977f-b424d5deee6a/download/siga-empreendimentos-geracao.csv" $aneelCsv

Ensure-Dir $censoRoot
foreach ($uf in $ufs) {
    $stateName = $stateNames[$uf]
    $zipPath = Join-Path $censoRoot "$stateName.zip"
    $url = "https://ftp.ibge.gov.br/Censos/Censo_Demografico_2010/Resultados_do_Universo/xls/Municipios/$stateName.zip"
    Download-IfMissing $url $zipPath
}

Write-Host "Bootstrap dos dados brutos concluido."
