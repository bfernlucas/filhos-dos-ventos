# Filhos dos Ventos

Projeto para construir e analisar um painel municipal do Nordeste sobre eolicas, semiarido, potencial eolico e desfechos de registro civil, com foco em estrategias de diferencas-em-diferencas escalonadas e extensoes econometricas.

## Destaques

- Pipeline reproduzivel para montar a base espacial do Nordeste e o painel municipio-ano `2016-2025`.
- Integracao entre IBGE, INSA/SUDENE, CEPEL, ANEEL, Registro Civil e Censo 2010.
- Saidas prontas para QGIS, Stata, R e Python.
- Estrutura publica pensada para GitHub, com codigo, documentacao e artefatos leves bem separados dos insumos brutos.

## Estrutura do projeto

- `data/raw`: insumos brutos oficiais, mantidos sem alteracao.
- `data/interim`: extracoes e artefatos intermediarios.
- `data/processed`: bases finais prontas para analise e arquivos espaciais.
- `src/python`: scripts de construcao das bases espaciais e do painel.
- `src/stata`: scripts de analise econometrica.
- `docs`: documentacao das fontes, reproducibilidade e codebook.
- `logs`: logs de execucao do pipeline.
- `results`: tabelas, figuras e resultados analiticos.
- `legacy`: materiais antigos preservados para rastreabilidade local.

## Produtos principais

Painel municipio-ano:

- `data/processed/panel/painel_municipio_ano_2016_2025.csv`
- `data/processed/panel/painel_municipio_ano_2016_2025.parquet`
- `data/processed/panel/painel_municipio_ano_2016_2025_metadata.json`

Arquivos espaciais leves versionaveis:

- `data/processed/vector_exports/potencial_eolico_nordeste.kmz`
- `data/processed/vector_exports/empreendimentos_eolicos_aneel_nordeste.kmz`

Arquivos espaciais completos gerados pelo pipeline:

- `data/processed/mapa_nordeste_camadas.gpkg`
- `data/processed/nordeste_energia_eolica.gpkg`

## Como reproduzir

No PowerShell, na raiz do projeto:

```powershell
powershell -ExecutionPolicy Bypass -File .\run_pipeline.ps1
```

Esse comando:

- baixa os dados brutos oficiais se eles ainda nao existirem localmente;
- executa a base espacial;
- valida os produtos principais;
- exporta as camadas KML/KMZ;
- reconstrui o painel municipio-ano `2016-2025`.

Dependencias Python estao listadas em `requirements.txt`.
Notas sobre portabilidade e versionamento estao em `docs/reproducibility.md`.

## Scripts principais

- `src/bootstrap_raw_data.ps1`
- `src/python/build_spatial_base.py`
- `src/python/validate_outputs.py`
- `src/python/build_panel_2016_2025.py`
- `src/stata/csdid_template.do`

## Estrategia de publicacao

Para manter o repositorio limpo e profissional:

- versione codigo, documentacao e artefatos finais leves;
- nao versione `data/raw`, `data/interim`, logs e materiais legados;
- gere GeoPackages e shapefiles localmente pelo pipeline quando necessario.
