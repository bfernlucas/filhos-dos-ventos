from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]

DATA_RAW_DIR = PROJECT_ROOT / "data" / "raw"
DATA_INTERIM_DIR = PROJECT_ROOT / "data" / "interim"
DATA_PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
DOCS_DIR = PROJECT_ROOT / "docs"
LOGS_DIR = PROJECT_ROOT / "logs"

RAW_GEOBR_DIR = DATA_RAW_DIR / "geobr" / "municipios_nordeste"
RAW_UFS_DIR = DATA_RAW_DIR / "geobr" / "ufs_nordeste"
RAW_SEMIARIDO_RAR = DATA_RAW_DIR / "semiarido" / "municipios_uf_poligono_sab_sudene.rar"
INTERIM_SEMIARIDO_DIR = DATA_INTERIM_DIR / "semiarido_extraido"
RAW_WIND_KMZ = DATA_RAW_DIR / "wind" / "dados_gerais_RNordeste.kmz"
RAW_ANEEL_CSV = DATA_RAW_DIR / "aneel" / "siga-empreendimentos-geracao.csv"

WIND_LAYER = "Nordeste_Dados_Consolidados"
NORTHEAST_UFS = ["AL", "BA", "CE", "MA", "PB", "PE", "PI", "RN", "SE"]
WIND_FIELDS = ["fator_k", "fator_c", "V_50m", "V_80m", "V_100m", "V_120m", "V_150m", "V_200m"]

# Raio do buffer (em metros, SIRGAS 2000 / Polyconic EPSG:5880) usado para
# identificar spillovers de empreendimentos eolicos vizinhos. 50 km segue a
# escolha documentada no codebook.
SPILLOVER_BUFFER_M = 50_000
PANEL_YEAR_START = 2016
PANEL_YEAR_END = 2025

INTERIM_WIND_POINTS_CSV = DATA_INTERIM_DIR / "wind_points_nordeste.csv"
INTERIM_ANEEL_POINTS_CSV = DATA_INTERIM_DIR / "aneel_eol_points_nordeste.csv"

FINAL_BASE_GPKG = DATA_PROCESSED_DIR / "nordeste_energia_eolica.gpkg"
FINAL_BASE_CSV = DATA_PROCESSED_DIR / "municipios_nordeste_base.csv"
FINAL_BASE_SHP = DATA_PROCESSED_DIR / "municipios_nordeste_base.shp"
FINAL_METADATA_JSON = DATA_PROCESSED_DIR / "municipios_nordeste_base_metadata.json"
FINAL_MAP_GPKG = DATA_PROCESSED_DIR / "mapa_nordeste_camadas.gpkg"
FINAL_VECTOR_DIR = DATA_PROCESSED_DIR / "vector_exports"

FINAL_MUNICIPIOS_SHP = FINAL_VECTOR_DIR / "nordeste_municipios.shp"
FINAL_UFS_SHP = FINAL_VECTOR_DIR / "nordeste_ufs.shp"
FINAL_SEMIARIDO_SHP = FINAL_VECTOR_DIR / "semiarido_oficial.shp"
FINAL_WIND_KML = FINAL_VECTOR_DIR / "potencial_eolico_nordeste.kml"
FINAL_WIND_KMZ = FINAL_VECTOR_DIR / "potencial_eolico_nordeste.kmz"
FINAL_ANEEL_KML = FINAL_VECTOR_DIR / "empreendimentos_eolicos_aneel_nordeste.kml"
FINAL_ANEEL_KMZ = FINAL_VECTOR_DIR / "empreendimentos_eolicos_aneel_nordeste.kmz"

GPKG_LAYERS = {
    "municipios_base": "municipios_base",
    "wind_points": "wind_points",
    "aneel_eol_points": "aneel_eol_points",
}


def _find_recursive(root: Path, filename: str) -> Path:
    # Ordenacao garante resultado deterministico entre sistemas de arquivos.
    matches = sorted(root.rglob(filename))
    if matches:
        return matches[0]
    return root / filename


SEMIARIDO_MUNICIPAL_SHP = _find_recursive(INTERIM_SEMIARIDO_DIR, "LIM_Semiarido_Municipal_OFICIAL.shp")
SEMIARIDO_POLYGON_SHP = _find_recursive(INTERIM_SEMIARIDO_DIR, "LIM_Semiarido_OFICIAL_POLIGONAL.shp")

SHP_FIELD_MAP = {
    "cd_mun": "cd_mun",
    "nm_mun": "nm_mun",
    "cd_uf": "cd_uf",
    "sigla_uf": "uf",
    "nm_uf": "nm_uf",
    "area_km2": "area_km2",
    "semi_txt": "semi_txt",
    "semi_dum": "semi_dum",
    "wind_n": "wind_n",
    "wind_v50": "w_v50",
    "wind_v80": "w_v80",
    "wind_v100": "w_v100",
    "wind_v120": "w_v120",
    "wind_v150": "w_v150",
    "wind_v200": "w_v200",
    "wind_fk": "w_fk",
    "wind_fc": "w_fc",
    "aneel_eol_n": "eol_n",
    "aneel_oper_n": "eol_oper_n",
    "aneel_const_n": "eol_const",
    "aneel_naoini_n": "eol_nini_n",
    "aneel_pot_out_kw": "pot_out_kw",
    "aneel_pot_fisc_kw": "ptfisc_kw",
    "aneel_gf_kw": "gf_kw",
}
