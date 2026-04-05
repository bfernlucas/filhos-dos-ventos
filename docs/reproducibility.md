# Reproducibility Notes

## Recommended GitHub Scope

For a public and reproducible repository, version:

- source code in `src/`
- project documentation in `docs/`
- root files such as `README.md`, `requirements.txt`, `.gitignore` and `run_pipeline.ps1`
- lightweight final outputs in `data/processed/panel`
- lightweight CSVs in `data/processed/registro_civil`
- compact spatial exports in KMZ format in `data/processed/vector_exports`

Do not version:

- `data/raw`
- `data/interim`
- large GeoPackage and shapefile exports
- legacy folders with manual intermediate work

## Current Status

The project was validated locally on 2026-04-05 and now includes:

- `src/bootstrap_raw_data.ps1` for official raw data downloads
- `run_pipeline.ps1` for the end-to-end build

The main remaining portability point is the geospatial runtime, because the pipeline still requires a Python environment with GDAL-compatible geospatial libraries and access to `ogr2ogr`.

## What Already Works

- downloading official raw files into `data/raw`
- extracting the required IBGE and semiarido archives
- rebuilding the spatial base from raw files
- validating the spatial outputs
- exporting KMZ layers for wind potential and ANEEL wind projects
- rebuilding the municipality-year panel `2016-2025`

## Runtime Expectation

The current `run_pipeline.ps1` is more portable than before:

- it prefers `PYTHON_EXE` when defined
- it prefers `OGR2OGR_EXE` when defined
- it falls back to OSGeo4W defaults when available
- it tries `python` and `ogr2ogr` from PATH as a final fallback

## Next Step To Reach Full Portability

The natural next improvement is to define and publish a single environment recipe, for example a conda or mamba environment file, so any user can recreate the geospatial runtime with one command.
