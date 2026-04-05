import geopandas as gpd
import pandas as pd

from pipeline_config import (
    DATA_INTERIM_DIR,
    DATA_PROCESSED_DIR,
    FINAL_BASE_CSV,
    FINAL_BASE_GPKG,
    FINAL_MAP_GPKG,
    FINAL_BASE_SHP,
    FINAL_METADATA_JSON,
    FINAL_MUNICIPIOS_SHP,
    FINAL_SEMIARIDO_SHP,
    FINAL_UFS_SHP,
    GPKG_LAYERS,
    INTERIM_ANEEL_POINTS_CSV,
    INTERIM_SEMIARIDO_DIR,
    INTERIM_WIND_POINTS_CSV,
    NORTHEAST_UFS,
    RAW_ANEEL_CSV,
    RAW_GEOBR_DIR,
    RAW_SEMIARIDO_RAR,
    RAW_UFS_DIR,
    RAW_WIND_KMZ,
    SEMIARIDO_POLYGON_SHP,
    SEMIARIDO_MUNICIPAL_SHP,
    SHP_FIELD_MAP,
    WIND_FIELDS,
    WIND_LAYER,
)
from pipeline_utils import (
    ensure_directories,
    export_shapefile_with_short_names,
    normalize_text,
    parse_description_table,
    parse_ptbr_number,
    safe_delete,
    to_numeric_columns,
    write_metadata,
)


def load_municipal_boundaries() -> gpd.GeoDataFrame:
    frames = []
    for uf in NORTHEAST_UFS:
        shp_path = RAW_GEOBR_DIR / uf / f"{uf}_Municipios_2025.shp"
        frames.append(gpd.read_file(shp_path))
    return gpd.GeoDataFrame(pd.concat(frames, ignore_index=True), crs=frames[0].crs)


def load_semiarid_status() -> pd.DataFrame:
    semi = gpd.read_file(SEMIARIDO_MUNICIPAL_SHP)
    semi = semi.rename(columns={"CD_GEOCMU": "cd_mun", "Semiarido": "semi_txt"})
    semi["cd_mun"] = semi["cd_mun"].astype(str).str.zfill(7)
    semi["semi_txt"] = semi["semi_txt"].fillna("Nao")
    semi["semi_dum"] = semi["semi_txt"].str.upper().eq("SIM").astype(int)
    return semi[["cd_mun", "semi_txt", "semi_dum"]].drop_duplicates()


def load_uf_boundaries() -> gpd.GeoDataFrame:
    frames = []
    for uf in NORTHEAST_UFS:
        shp_path = RAW_UFS_DIR / uf / f"{uf}_UF_2025.shp"
        frames.append(gpd.read_file(shp_path))
    ufs = gpd.GeoDataFrame(pd.concat(frames, ignore_index=True), crs=frames[0].crs)
    return ufs.rename(columns={"CD_UF": "cd_uf", "SIGLA_UF": "sigla_uf", "NM_UF": "nm_uf"})


def load_semiarido_polygon() -> gpd.GeoDataFrame:
    semi = gpd.read_file(SEMIARIDO_POLYGON_SHP)
    return semi.rename(columns={"Semiarido": "semi_txt"})


def load_wind_points() -> gpd.GeoDataFrame:
    wind = gpd.read_file(RAW_WIND_KMZ, layer=WIND_LAYER)
    parsed_df = pd.DataFrame(wind["description"].apply(parse_description_table).tolist())
    parsed_df = parsed_df[["LAT", "LONG"] + WIND_FIELDS].copy()
    parsed_df = to_numeric_columns(parsed_df, ["LAT", "LONG"] + WIND_FIELDS)

    wind = gpd.GeoDataFrame(
        pd.concat([wind[["Name"]].reset_index(drop=True), parsed_df.reset_index(drop=True)], axis=1),
        geometry=wind.geometry,
        crs=wind.crs,
    )
    wind = wind.rename(columns={"Name": "wind_cell"})
    return wind.dropna(subset=["geometry"])


def aggregate_wind_to_municipality(municipal: gpd.GeoDataFrame, wind_points: gpd.GeoDataFrame) -> pd.DataFrame:
    joined = gpd.sjoin(
        wind_points.to_crs(municipal.crs),
        municipal[["cd_mun", "geometry"]],
        how="inner",
        predicate="within",
    )
    return (
        joined.groupby("cd_mun")
        .agg(
            wind_n=("wind_cell", "count"),
            wind_v50=("V_50m", "mean"),
            wind_v80=("V_80m", "mean"),
            wind_v100=("V_100m", "mean"),
            wind_v120=("V_120m", "mean"),
            wind_v150=("V_150m", "mean"),
            wind_v200=("V_200m", "mean"),
            wind_fk=("fator_k", "mean"),
            wind_fc=("fator_c", "mean"),
        )
        .reset_index()
    )


def load_aneel_points() -> gpd.GeoDataFrame:
    aneel = pd.read_csv(RAW_ANEEL_CSV, sep=";", dtype=str, encoding="latin1")
    aneel = aneel[
        (aneel["SigTipoGeracao"] == "EOL") & (aneel["SigUFPrincipal"].isin(NORTHEAST_UFS))
    ].copy()

    aneel["lat"] = aneel["NumCoordNEmpreendimento"].apply(parse_ptbr_number)
    aneel["lon"] = aneel["NumCoordEEmpreendimento"].apply(parse_ptbr_number)
    aneel["pot_out_kw"] = aneel["MdaPotenciaOutorgadaKw"].apply(parse_ptbr_number)
    aneel["pot_fisc_kw"] = aneel["MdaPotenciaFiscalizadaKw"].apply(parse_ptbr_number)
    aneel["gar_fis_kw"] = aneel["MdaGarantiaFisicaKw"].apply(parse_ptbr_number)
    aneel = aneel.dropna(subset=["lat", "lon"]).copy()

    geometry = gpd.points_from_xy(aneel["lon"], aneel["lat"], crs="EPSG:4326")
    aneel = gpd.GeoDataFrame(aneel, geometry=geometry)
    aneel = aneel.rename(
        columns={
            "DatGeracaoConjuntoDados": "dat_base",
            "NomEmpreendimento": "nom_empre",
            "IdeNucleoCEG": "ide_nucleo",
            "CodCEG": "cod_ceg",
            "SigUFPrincipal": "sigla_uf",
            "SigTipoGeracao": "sig_tipo",
            "DscFaseUsina": "fase_usina",
            "DscOrigemCombustivel": "origem_comb",
            "DscFonteCombustivel": "fonte_comb",
            "DscTipoOutorga": "tipo_out",
            "NomFonteCombustivel": "nom_fonte",
            "DatEntradaOperacao": "dat_oper",
            "IdcGeracaoQualificada": "ger_qualif",
            "DatInicioVigencia": "dat_ini",
            "DatFimVigencia": "dat_fim",
            "DscPropriRegimePariticipacao": "propriet",
            "DscSubBacia": "sub_bacia",
            "DscMuninicpios": "mun_aneel",
        }
    )
    return aneel


def aggregate_aneel_to_municipality(municipal: gpd.GeoDataFrame, aneel_points: gpd.GeoDataFrame) -> pd.DataFrame:
    joined = gpd.sjoin(
        aneel_points.to_crs(municipal.crs),
        municipal[["cd_mun", "geometry"]],
        how="inner",
        predicate="within",
    )

    phase_norm = joined["fase_usina"].apply(normalize_text)
    joined["is_oper"] = phase_norm.eq("operacao").astype(int)
    joined["is_const"] = phase_norm.eq("construcao").astype(int)
    joined["is_nao_ini"] = phase_norm.eq("construcao nao iniciada").astype(int)

    return (
        joined.groupby("cd_mun")
        .agg(
            aneel_eol_n=("cod_ceg", "count"),
            aneel_oper_n=("is_oper", "sum"),
            aneel_const_n=("is_const", "sum"),
            aneel_naoini_n=("is_nao_ini", "sum"),
            aneel_pot_out_kw=("pot_out_kw", "sum"),
            aneel_pot_fisc_kw=("pot_fisc_kw", "sum"),
            aneel_gf_kw=("gar_fis_kw", "sum"),
        )
        .reset_index()
    )


def build_municipal_base() -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame, gpd.GeoDataFrame]:
    municipal = load_municipal_boundaries().rename(
        columns={
            "CD_MUN": "cd_mun",
            "NM_MUN": "nm_mun",
            "CD_UF": "cd_uf",
            "SIGLA_UF": "sigla_uf",
            "NM_UF": "nm_uf",
            "AREA_KM2": "area_km2",
        }
    )
    municipal["cd_mun"] = municipal["cd_mun"].astype(str).str.zfill(7)

    wind_points = load_wind_points()
    aneel_points = load_aneel_points()

    base = municipal.merge(load_semiarid_status(), on="cd_mun", how="left")
    base = base.merge(aggregate_wind_to_municipality(municipal, wind_points), on="cd_mun", how="left")
    base = base.merge(aggregate_aneel_to_municipality(municipal, aneel_points), on="cd_mun", how="left")

    base["semi_txt"] = base["semi_txt"].fillna("Nao")
    base["semi_dum"] = base["semi_dum"].fillna(0).astype(int)

    for column in [
        "aneel_eol_n",
        "aneel_oper_n",
        "aneel_const_n",
        "aneel_naoini_n",
        "aneel_pot_out_kw",
        "aneel_pot_fisc_kw",
        "aneel_gf_kw",
    ]:
        base[column] = base[column].fillna(0)

    keep = [
        "cd_mun",
        "nm_mun",
        "cd_uf",
        "sigla_uf",
        "nm_uf",
        "area_km2",
        "semi_txt",
        "semi_dum",
        "wind_n",
        "wind_v50",
        "wind_v80",
        "wind_v100",
        "wind_v120",
        "wind_v150",
        "wind_v200",
        "wind_fk",
        "wind_fc",
        "aneel_eol_n",
        "aneel_oper_n",
        "aneel_const_n",
        "aneel_naoini_n",
        "aneel_pot_out_kw",
        "aneel_pot_fisc_kw",
        "aneel_gf_kw",
        "geometry",
    ]
    base = base[keep].sort_values(["sigla_uf", "cd_mun"])
    return gpd.GeoDataFrame(base, geometry="geometry", crs=municipal.crs), wind_points, aneel_points


def export_outputs(base: gpd.GeoDataFrame, wind_points: gpd.GeoDataFrame, aneel_points: gpd.GeoDataFrame) -> None:
    ufs = load_uf_boundaries()
    semiarido_polygon = load_semiarido_polygon().to_crs(base.crs)
    municipios_map = base[["cd_mun", "nm_mun", "sigla_uf", "semi_dum", "geometry"]].rename(
        columns={"sigla_uf": "uf"}
    )

    base.drop(columns="geometry").to_csv(FINAL_BASE_CSV, index=False, encoding="utf-8-sig")
    export_shapefile_with_short_names(base, FINAL_BASE_SHP, SHP_FIELD_MAP)
    municipios_map.to_file(FINAL_MUNICIPIOS_SHP, driver="ESRI Shapefile", encoding="utf-8")
    ufs.to_file(FINAL_UFS_SHP, driver="ESRI Shapefile", encoding="utf-8")
    semiarido_polygon.to_file(FINAL_SEMIARIDO_SHP, driver="ESRI Shapefile", encoding="utf-8")

    safe_delete(FINAL_BASE_GPKG)
    base.to_file(FINAL_BASE_GPKG, layer=GPKG_LAYERS["municipios_base"], driver="GPKG")
    wind_points.to_file(FINAL_BASE_GPKG, layer=GPKG_LAYERS["wind_points"], driver="GPKG")
    aneel_points.to_file(FINAL_BASE_GPKG, layer=GPKG_LAYERS["aneel_eol_points"], driver="GPKG")

    safe_delete(FINAL_MAP_GPKG)
    municipios_map.to_file(FINAL_MAP_GPKG, layer="municipios", driver="GPKG")
    ufs.to_file(FINAL_MAP_GPKG, layer="ufs", driver="GPKG")
    semiarido_polygon.to_file(FINAL_MAP_GPKG, layer="semiarido", driver="GPKG")

    wind_points.drop(columns="geometry").to_csv(INTERIM_WIND_POINTS_CSV, index=False, encoding="utf-8-sig")
    aneel_points.drop(columns="geometry").to_csv(INTERIM_ANEEL_POINTS_CSV, index=False, encoding="utf-8-sig")

    write_metadata(
        FINAL_METADATA_JSON,
        {
            "municipios_total": int(len(base)),
            "municipios_semiarido": int(base["semi_dum"].sum()),
            "municipios_com_vento": int(base["wind_n"].fillna(0).gt(0).sum()),
            "municipios_com_eolicas_aneel": int(base["aneel_eol_n"].gt(0).sum()),
            "empreendimentos_eolicos_aneel_georreferenciados": int(len(aneel_points)),
            "camadas_geopackage": list(GPKG_LAYERS.values()),
            "camadas_mapa_final": ["municipios", "ufs", "semiarido"],
            "insumos": {
                "geobr_municipios": str(RAW_GEOBR_DIR),
                "geobr_ufs": str(RAW_UFS_DIR),
                "semiarido_rar": str(RAW_SEMIARIDO_RAR),
                "semiarido_extraido": str(INTERIM_SEMIARIDO_DIR),
                "wind_kmz": str(RAW_WIND_KMZ),
                "aneel_csv": str(RAW_ANEEL_CSV),
            },
        },
    )


def main() -> None:
    ensure_directories([DATA_INTERIM_DIR, DATA_PROCESSED_DIR])
    base, wind_points, aneel_points = build_municipal_base()
    export_outputs(base, wind_points, aneel_points)

    print(f"GeoPackage salvo em: {FINAL_BASE_GPKG}")
    print(f"GeoPackage cartografico salvo em: {FINAL_MAP_GPKG}")
    print(f"CSV salvo em: {FINAL_BASE_CSV}")
    print(f"Shapefile salvo em: {FINAL_BASE_SHP}")
    print(f"Metadados salvos em: {FINAL_METADATA_JSON}")
    print(f"Municipios totais: {len(base)}")
    print(f"Municipios com eolicas ANEEL: {int(base['aneel_eol_n'].gt(0).sum())}")


if __name__ == "__main__":
    main()
