# Data Sources

Updated on 2026-04-05.

## IBGE

Territorial meshes used:

- Northeast municipalities: AL, BA, CE, MA, PB, PE, PI, RN and SE
- Northeast state boundaries: AL, BA, CE, MA, PB, PE, PI, RN and SE

Official page:

- https://www.ibge.gov.br/geociencias/organizacao-do-territorio/estrutura-territorial/15774-malhas.html

Direct download pattern used by the bootstrap:

- `https://geoftp.ibge.gov.br/organizacao_do_territorio/malhas_territoriais/malhas_municipais/municipio_2025/UFs/<UF>/<UF>_Municipios_2025.zip`
- `https://geoftp.ibge.gov.br/organizacao_do_territorio/malhas_territoriais/malhas_municipais/municipio_2025/UFs/<UF>/<UF>_UF_2025.zip`

Local storage:

- `data/raw/geobr/municipios_nordeste`
- `data/raw/geobr/ufs_nordeste`

## INSA / SUDENE

Official semiarido archive:

- `municipios_uf_poligono_sab_sudene.rar`

Official pages:

- https://www.gov.br/insa/pt-br/assuntos/noticias/insa-mcti-disponibiliza-mapas-do-semiarido-com-a-mais-recente-delimitacao-da-regiao
- https://www.gov.br/insa/pt-br/centrais-de-conteudo/mapas/mapas-em-shapefile

Direct download:

- https://www.gov.br/insa/pt-br/centrais-de-conteudo/mapas/mapas-em-shapefile/municipios_uf_poligono_sab_sudene.rar/@@download/file

Local storage:

- `data/raw/semiarido/municipios_uf_poligono_sab_sudene.rar`
- `data/interim/semiarido_extraido`

## CEPEL / Atlas Eolico Brasileiro

Wind potential file used:

- `dados_gerais_RNordeste.kmz`

Official page:

- https://novoatlas.cepel.br/index.php/mapas-tematicos/

Direct download:

- https://novoatlas.cepel.br/wp-content/uploads/2017/03/dados_gerais_RNordeste.kmz

Local storage:

- `data/raw/wind/dados_gerais_RNordeste.kmz`

## ANEEL / SIGA

Generation projects file used:

- `siga-empreendimentos-geracao.csv`

Official source:

- https://dadosabertos.aneel.gov.br/dataset/siga-sistema-de-informacoes-de-geracao-da-aneel-empreendimentos-de-geracao

Direct download:

- https://dadosabertos.aneel.gov.br/dataset/6d90b77c-c5f5-4d81-bdec-7bc619494bb9/resource/11ec447d-698d-4ab8-977f-b424d5deee6a/download/siga-empreendimentos-geracao.csv

Local storage:

- `data/raw/aneel/siga-empreendimentos-geracao.csv`

## IBGE Census 2010

Universe results used for parsimonious municipal covariates:

- population total
- urban population
- rural population

Direct download pattern used by the bootstrap:

- `https://ftp.ibge.gov.br/Censos/Censo_Demografico_2010/Resultados_do_Universo/xls/Municipios/<estado>.zip`

Local storage:

- `data/raw/censo_2010/resultados_universo_municipios`

## Methodological Notes

- IBGE shapefiles are in SIRGAS 2000.
- The semiarido municipal layer is distributed in WGS84.
- CEPEL wind points are aggregated to the municipality level by spatial containment.
- ANEEL projects are filtered to `SigTipoGeracao = EOL` and Northeast states.
- ANEEL project coordinates are taken directly from SIGA and aggregated to municipalities by spatial containment.
- The pipeline prioritizes municipal IBGE codes whenever possible, avoiding merges based only on names.
