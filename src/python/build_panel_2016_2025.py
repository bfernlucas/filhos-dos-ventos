from __future__ import annotations

import io
import json
import zipfile
from urllib.parse import urlencode

import geopandas as gpd
import pandas as pd

from pipeline_config import (
    DATA_PROCESSED_DIR,
    DATA_RAW_DIR,
    NORTHEAST_UFS,
    PANEL_YEAR_END,
    PANEL_YEAR_START,
    SPILLOVER_BUFFER_M,
)
# Reaproveita os loaders do build_spatial_base para manter uma unica fonte de
# verdade para vento (CEPEL), ANEEL e semiarido (SUDENE). Isso elimina a
# divergencia anterior entre os dois scripts.
from build_spatial_base import (
    load_aneel_points as _load_aneel_points_base,
    load_municipal_boundaries,
    load_semiarid_status,
    load_wind_points,
)
from pipeline_utils import http_get_text, normalize_text
from merge_utils import (
    MergeReport,
    censo_crosswalk_by_ibge_code,
    fuzzy_merge_names_within_uf,
)


# Relatorios globais preenchidos durante a construcao do painel para serem
# serializados no metadata. Permitem auditar matches fuzzy linha a linha.
_MERGE_REPORTS: dict[str, MergeReport] = {}


REGCIV_RAW_DIR = DATA_RAW_DIR / "registro_civil_painel_anual"
PANEL_OUT_DIR = DATA_PROCESSED_DIR / "panel"
PANEL_CSV = PANEL_OUT_DIR / "painel_municipio_ano_2016_2025.csv"
PANEL_PARQUET = PANEL_OUT_DIR / "painel_municipio_ano_2016_2025.parquet"
PANEL_METADATA = PANEL_OUT_DIR / "painel_municipio_ano_2016_2025_metadata.json"

YEARS = list(range(PANEL_YEAR_START, PANEL_YEAR_END + 1))
REGISTRY_BASE_URL = "https://transparencia.registrocivil.org.br/api"


def ensure_dirs() -> None:
    REGCIV_RAW_DIR.mkdir(parents=True, exist_ok=True)
    PANEL_OUT_DIR.mkdir(parents=True, exist_ok=True)


def fetch_csv_text(endpoint: str, params: dict[str, str]) -> str:
    url = f"{REGISTRY_BASE_URL}/{endpoint}?{urlencode(params)}"
    return http_get_text(url)


def load_municipal_base() -> gpd.GeoDataFrame:
    gdf = load_municipal_boundaries().rename(
        columns={
            "CD_MUN": "cd_mun",
            "NM_MUN": "nm_mun",
            "CD_UF": "cd_uf",
            "SIGLA_UF": "sigla_uf",
            "NM_UF": "nm_uf",
            "AREA_KM2": "area_km2",
        }
    )
    gdf["cd_mun"] = gdf["cd_mun"].astype(str).str.zfill(7)
    gdf["nm_mun_norm"] = gdf["nm_mun"].apply(normalize_text)
    return gdf


def load_semiarido_status() -> pd.DataFrame:
    # Delegamos para o loader oficial em build_spatial_base para garantir que
    # painel e base espacial compartilhem as mesmas colunas (semi_dum, semi_txt).
    return load_semiarid_status()


def aggregate_wind_to_municipality(municipal: gpd.GeoDataFrame) -> pd.DataFrame:
    joined = gpd.sjoin(
        load_wind_points().to_crs(municipal.crs),
        municipal[["cd_mun", "geometry"]],
        how="inner",
        predicate="within",
    )
    return joined.groupby("cd_mun").agg(wind_v100=("V_100m", "mean")).reset_index()


def load_aneel_points() -> gpd.GeoDataFrame:
    # Reaproveita o loader da base espacial e apenas enriquece com a data de
    # operacao (necessaria para construir o tratamento anual).
    aneel = _load_aneel_points_base().copy()
    aneel["dat_oper_ts"] = pd.to_datetime(aneel["dat_oper"], errors="coerce")
    aneel["ano_oper"] = aneel["dat_oper_ts"].dt.year
    return aneel


def build_eolica_flags(municipal: gpd.GeoDataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    points = load_aneel_points().to_crs(municipal.crs)
    joined = gpd.sjoin(points, municipal[["cd_mun", "geometry"]], how="inner", predicate="within")

    yearly_rows = []
    for year in YEARS:
        active = joined[(joined["ano_oper"].notna()) & (joined["ano_oper"] <= year)]
        agg = (
            active.groupby("cd_mun")
            .agg(
                eolica_presenca=("cod_ceg", "count"),
                eolica_pot_out_kw=("pot_out_kw", "sum"),
                eolica_pot_fisc_kw=("pot_fisc_kw", "sum"),
            )
            .reset_index()
        )
        agg["ano"] = year
        agg["eolica_presenca"] = (agg["eolica_presenca"] > 0).astype(int)
        yearly_rows.append(agg)

    yearly = pd.concat(yearly_rows, ignore_index=True) if yearly_rows else pd.DataFrame()

    first_year = (
        joined.loc[joined["ano_oper"].notna(), ["cd_mun", "ano_oper"]]
        .groupby("cd_mun", as_index=False)["ano_oper"]
        .min()
        .rename(columns={"ano_oper": "ano_primeira_eolica"})
    )
    first_year["eolica_pre2016"] = (first_year["ano_primeira_eolica"] < 2016).astype(int)
    return yearly, first_year


def build_spillover_50km(municipal: gpd.GeoDataFrame) -> pd.DataFrame:
    muni_metric = municipal.to_crs(5880)
    centroids = muni_metric.copy()
    centroids["geometry"] = centroids.geometry.centroid

    points = load_aneel_points()
    pre2016 = points[(points["ano_oper"].notna()) & (points["ano_oper"] < 2016)].to_crs(5880)
    if pre2016.empty:
        return centroids[["cd_mun"]].assign(eolica_pre2016_50km=0)

    buffers = pre2016[["geometry"]].copy()
    buffers["geometry"] = buffers.geometry.buffer(SPILLOVER_BUFFER_M)
    union_geom = buffers.union_all()
    centroids["eolica_pre2016_50km"] = centroids.geometry.within(union_geom).astype(int)
    return centroids[["cd_mun", "eolica_pre2016_50km"]]


def download_pais_ausentes_year(year: int, uf: str) -> pd.DataFrame:
    out_path = REGCIV_RAW_DIR / f"father_off_{uf}_{year}.csv"
    if out_path.exists():
        df = pd.read_csv(out_path)
        df["uf"] = uf
        df["ano"] = year
        return df
    params = {
        "data_inicial": f"{year}-01-01",
        "data_final": f"{year}-12-31",
        "groupBy": "cidade",
        "uf": uf,
        "csv": "true",
    }
    text = fetch_csv_text("father-off", params)
    out_path.write_text(text, encoding="utf-8")
    df = pd.read_csv(io.StringIO(text))
    df["uf"] = uf
    df["ano"] = year
    return df


def download_reconhecimento_year(year: int, uf: str) -> pd.DataFrame:
    out_path = REGCIV_RAW_DIR / f"father_on_{uf}_{year}.csv"
    if out_path.exists():
        df = pd.read_csv(out_path)
        df["uf"] = uf
        return df
    params = {
        "data_inicial": f"{year}-01-01",
        "data_final": f"{year}-12-31",
        "groupBy": "cidade",
        "uf": uf,
        "csv": "true",
    }
    text = fetch_csv_text("father-on", params)
    out_path.write_text(text, encoding="utf-8")
    df = pd.read_csv(io.StringIO(text))
    df["uf"] = uf
    return df


def _merge_registry_names(
    frame: pd.DataFrame, municipal: gpd.GeoDataFrame, name_col: str, report_key: str
) -> pd.DataFrame:
    """Une uma fonte do Registro Civil a malha IBGE por nome (exato + fuzzy).

    O Registro Civil (ARPEN) so publica o nome do municipio. Usamos match
    exato por nome normalizado dentro da UF e, para o residuo, um match
    aproximado via RapidFuzz. Os pares fuzzy ficam registrados em
    ``_MERGE_REPORTS`` para auditoria posterior no metadata.
    """
    muni_lookup = (
        municipal[["cd_mun", "sigla_uf", "nm_mun"]]
        .drop_duplicates()
        .rename(columns={"sigla_uf": "uf"})
    )
    frame = frame.rename(columns={"uf": "uf"})  # noop, explicita a coluna
    merged, report = fuzzy_merge_names_within_uf(
        left=frame,
        right=muni_lookup,
        left_name=name_col,
        right_name="nm_mun",
        uf_col="uf",
    )
    _MERGE_REPORTS[report_key] = report
    return merged


def build_registry_panel(municipal: gpd.GeoDataFrame) -> pd.DataFrame:
    off_frames = []
    on_frames = []
    for year in YEARS:
        for uf in NORTHEAST_UFS:
            off_frames.append(download_pais_ausentes_year(year, uf))
            on_frames.append(download_reconhecimento_year(year, uf))

    father_off = pd.concat(off_frames, ignore_index=True)
    father_off = father_off.rename(
        columns={
            "Estado": "municipio",
            "qt_pais_ausente": "pais_ausentes",
            "qt_nascimento": "nascimentos",
        }
    )
    father_off["pais_ausentes"] = pd.to_numeric(father_off["pais_ausentes"], errors="coerce")
    father_off["nascimentos"] = pd.to_numeric(father_off["nascimentos"], errors="coerce")

    father_on = pd.concat(on_frames, ignore_index=True)
    father_on = father_on.rename(
        columns={
            "Estado": "municipio",
            "qt_reconhecimento_paternidade": "reconhecimento_paternidade",
        }
    )
    father_on["reconhecimento_paternidade"] = pd.to_numeric(
        father_on["reconhecimento_paternidade"], errors="coerce"
    )
    father_on["ano"] = pd.to_numeric(father_on["ano"], errors="coerce")

    # Pareamento por nome (exato + fuzzy) com a malha IBGE, antes de agregar
    # por municipio-ano. Isso garante que o fuzzy rode sobre nomes unicos e
    # nao repetidamente por ano-UF.
    father_off_unique = father_off[["uf", "municipio"]].drop_duplicates()
    father_on_unique = father_on[["uf", "municipio"]].drop_duplicates()

    off_map = _merge_registry_names(
        father_off_unique, municipal, "municipio", "registro_civil_pais_ausentes"
    )[["uf", "municipio", "cd_mun"]]
    on_map = _merge_registry_names(
        father_on_unique, municipal, "municipio", "registro_civil_reconhecimento"
    )[["uf", "municipio", "cd_mun"]]

    father_off = father_off.merge(off_map, on=["uf", "municipio"], how="left")
    father_on = father_on.merge(on_map, on=["uf", "municipio"], how="left")

    # Agregacao por municipio-ano: soma dentro de cd_mun evita duplicidade
    # caso dois nomes externos diferentes mapeiem para o mesmo municipio IBGE
    # (o fuzzy pode absorver variantes historicas do nome).
    father_off = (
        father_off.dropna(subset=["cd_mun"])
        .groupby(["cd_mun", "ano"], as_index=False)[["pais_ausentes", "nascimentos"]]
        .sum(min_count=1)
    )
    father_on = (
        father_on.dropna(subset=["cd_mun"])
        .groupby(["cd_mun", "ano"], as_index=False)["reconhecimento_paternidade"]
        .sum(min_count=1)
    )

    base = pd.MultiIndex.from_product(
        [municipal["cd_mun"].sort_values().unique(), YEARS], names=["cd_mun", "ano"]
    ).to_frame(index=False)
    base = base.merge(
        municipal[["cd_mun", "nm_mun", "sigla_uf", "area_km2"]].drop_duplicates(),
        on="cd_mun",
        how="left",
    )

    panel = base.merge(father_off, on=["cd_mun", "ano"], how="left")
    panel = panel.merge(father_on, on=["cd_mun", "ano"], how="left")
    panel["pais_ausentes"] = panel["pais_ausentes"].fillna(0)
    panel["nascimentos"] = panel["nascimentos"].fillna(0)
    panel["reconhecimento_paternidade"] = panel["reconhecimento_paternidade"].fillna(0)
    return panel


def extract_censo_covariates() -> pd.DataFrame:
    mapping = {
        "AL": "alagoas.zip",
        "BA": "bahia.zip",
        "CE": "ceara.zip",
        "MA": "maranhao.zip",
        "PB": "paraiba.zip",
        "PE": "pernambuco.zip",
        "PI": "piaui.zip",
        "RN": "rio_grande_do_norte.zip",
        "SE": "sergipe.zip",
    }

    rows = []
    for uf, filename in mapping.items():
        zip_path = DATA_RAW_DIR / "censo_2010" / "resultados_universo_municipios" / filename
        with zipfile.ZipFile(zip_path) as zf:
            target = next(name for name in zf.namelist() if name.endswith(".1.1.xls"))
            data = io.BytesIO(zf.read(target))
            df = pd.read_excel(data, sheet_name=0, header=None)
        df = df.iloc[13:].copy()
        df = df[[0, 1, 4, 7, 10]].copy()
        df.columns = ["nome", "pop_total", "pop_urbana", "pop_rural", "cd_raw"]
        df = df[df["cd_raw"].notna()].copy()
        df["cd_raw"] = pd.to_numeric(df["cd_raw"], errors="coerce")
        df = df[df["cd_raw"] >= 100000].copy()
        rows.append(df.assign(uf=uf))

    censo = pd.concat(rows, ignore_index=True)
    return censo


def harmonize_censo_with_municipal(
    censo: pd.DataFrame, municipal: gpd.GeoDataFrame
) -> pd.DataFrame:
    """Harmoniza o Censo 2010 com a malha IBGE 2025.

    Usa cruzamento deterministico pelo codigo do IBGE: o ``cd_raw`` de 6
    digitos das planilhas ``.1.1.xls`` do Censo corresponde exatamente aos
    seis primeiros digitos do ``cd_mun`` de 7 digitos da malha municipal
    atual (o 7o digito e o verificador). Portanto nao ha espaco para erro
    por grafia ou acentuacao.
    """
    merged = censo_crosswalk_by_ibge_code(censo, municipal, code_col="cd_raw")

    report = MergeReport()
    report.exact_matches = int(merged["cd_mun"].notna().sum())
    report.unmatched = int(merged["cd_mun"].isna().sum())
    if report.unmatched:
        unmatched_rows = merged.loc[merged["cd_mun"].isna(), ["uf", "nome", "cd_raw"]]
        report.unmatched_rows = unmatched_rows.head(50).to_dict(orient="records")
    _MERGE_REPORTS["censo_2010"] = report

    merged["pop_total_2010"] = pd.to_numeric(merged["pop_total"], errors="coerce")
    merged["pop_urbana_2010"] = pd.to_numeric(merged["pop_urbana"], errors="coerce")
    merged["pop_rural_2010"] = pd.to_numeric(merged["pop_rural"], errors="coerce")
    # Taxa de ruralizacao: indefinida (NaN) quando pop_total e zero ou ausente.
    total = merged["pop_total_2010"].where(merged["pop_total_2010"] > 0)
    merged["share_rural_2010"] = merged["pop_rural_2010"] / total
    merged = merged[
        ["cd_mun", "pop_total_2010", "pop_urbana_2010", "pop_rural_2010", "share_rural_2010"]
    ].dropna(subset=["cd_mun"])
    return merged.drop_duplicates(subset=["cd_mun"])


def main() -> None:
    ensure_dirs()
    municipal = load_municipal_base()

    registry = build_registry_panel(municipal)
    wind = aggregate_wind_to_municipality(municipal)
    semiarido = load_semiarido_status()
    eolica_yearly, eolica_first = build_eolica_flags(municipal)
    spill = build_spillover_50km(municipal)
    censo = harmonize_censo_with_municipal(extract_censo_covariates(), municipal)

    panel = registry.merge(semiarido, on="cd_mun", how="left")
    panel = panel.merge(wind, on="cd_mun", how="left")
    panel = panel.merge(eolica_yearly, on=["cd_mun", "ano"], how="left")
    panel = panel.merge(eolica_first, on="cd_mun", how="left")
    panel = panel.merge(spill, on="cd_mun", how="left")
    panel = panel.merge(censo, on="cd_mun", how="left")

    panel["semi_txt"] = panel["semi_txt"].fillna("Nao")
    panel["semi_dum"] = panel["semi_dum"].fillna(0).astype(int)
    panel["eolica_presenca"] = panel["eolica_presenca"].fillna(0).astype(int)
    panel["eolica_pot_out_kw"] = panel["eolica_pot_out_kw"].fillna(0)
    panel["eolica_pot_fisc_kw"] = panel["eolica_pot_fisc_kw"].fillna(0)
    panel["eolica_pre2016"] = panel["eolica_pre2016"].fillna(0).astype(int)
    panel["eolica_pre2016_50km"] = panel["eolica_pre2016_50km"].fillna(0).astype(int)
    panel["ever_treated"] = panel["ano_primeira_eolica"].notna().astype(int)
    panel["event_time"] = panel["ano"] - panel["ano_primeira_eolica"]
    panel.loc[panel["ano_primeira_eolica"].isna(), "event_time"] = pd.NA
    # Taxas por 1.000 nascimentos: quando nao ha nascimentos registrados no
    # municipio-ano, a taxa e indefinida (NaN) em vez de zero ou infinito.
    nasc = panel["nascimentos"].where(panel["nascimentos"] > 0)
    panel["tx_pais_ausentes_1000"] = panel["pais_ausentes"] / nasc * 1000
    panel["tx_reconhecimento_1000"] = panel["reconhecimento_paternidade"] / nasc * 1000

    panel = panel[[
        "cd_mun",
        "nm_mun",
        "sigla_uf",
        "ano",
        "pais_ausentes",
        "reconhecimento_paternidade",
        "nascimentos",
        "tx_pais_ausentes_1000",
        "tx_reconhecimento_1000",
        "semi_txt",
        "semi_dum",
        "eolica_presenca",
        "eolica_pot_out_kw",
        "eolica_pot_fisc_kw",
        "ano_primeira_eolica",
        "eolica_pre2016",
        "eolica_pre2016_50km",
        "ever_treated",
        "event_time",
        "wind_v100",
        "pop_total_2010",
        "share_rural_2010",
        "area_km2",
    ]]

    panel = (
        panel.groupby(["cd_mun", "ano"], as_index=False)
        .agg(
            nm_mun=("nm_mun", "first"),
            sigla_uf=("sigla_uf", "first"),
            pais_ausentes=("pais_ausentes", "first"),
            reconhecimento_paternidade=("reconhecimento_paternidade", "first"),
            nascimentos=("nascimentos", "first"),
            tx_pais_ausentes_1000=("tx_pais_ausentes_1000", "first"),
            tx_reconhecimento_1000=("tx_reconhecimento_1000", "first"),
            semi_txt=("semi_txt", "first"),
            semi_dum=("semi_dum", "first"),
            eolica_presenca=("eolica_presenca", "first"),
            eolica_pot_out_kw=("eolica_pot_out_kw", "first"),
            eolica_pot_fisc_kw=("eolica_pot_fisc_kw", "first"),
            ano_primeira_eolica=("ano_primeira_eolica", "first"),
            eolica_pre2016=("eolica_pre2016", "first"),
            eolica_pre2016_50km=("eolica_pre2016_50km", "first"),
            ever_treated=("ever_treated", "first"),
            event_time=("event_time", "first"),
            wind_v100=("wind_v100", "first"),
            pop_total_2010=("pop_total_2010", "first"),
            share_rural_2010=("share_rural_2010", "first"),
            area_km2=("area_km2", "first"),
        )
        .sort_values(["cd_mun", "ano"])
    )

    panel.to_csv(PANEL_CSV, index=False, encoding="utf-8-sig")
    panel.to_parquet(PANEL_PARQUET, index=False)

    # Diagnosticos de qualidade dos joins frageis (Registro Civil e Censo 2010
    # sao unidos por nome normalizado dentro da UF, portanto podem ter perdas).
    expected_rows = len(YEARS) * int(panel["cd_mun"].nunique())
    if len(panel) != expected_rows:
        raise RuntimeError(
            f"Painel tem {len(panel)} linhas, esperado {expected_rows} "
            f"({panel['cd_mun'].nunique()} municipios x {len(YEARS)} anos)."
        )
    muni_zero_births = int(
        panel.groupby("cd_mun")["nascimentos"].sum().eq(0).sum()
    )
    muni_sem_censo = int(panel.loc[panel["pop_total_2010"].isna(), "cd_mun"].nunique())

    metadata = {
        "years": [PANEL_YEAR_START, PANEL_YEAR_END],
        "rows": int(len(panel)),
        "municipios": int(panel["cd_mun"].nunique()),
        "treated_ever": int(panel.drop_duplicates("cd_mun")["ever_treated"].sum()),
        "variables": list(panel.columns),
        "diagnosticos_qualidade": {
            "municipios_com_zero_nascimentos_todos_anos": muni_zero_births,
            "municipios_sem_pareamento_censo_2010": muni_sem_censo,
            "municipios_no_semiarido": int(
                panel.drop_duplicates("cd_mun")["semi_dum"].sum()
            ),
            "merge_reports": {
                key: report.to_dict() for key, report in _MERGE_REPORTS.items()
            },
        },
        "notes": [
            "Tratamento anual definido por presenca de eolica no municipio com base na data de entrada em operacao da ANEEL.",
            "Covariadas do Censo 2010 mantidas parcimoniosas: populacao total e participacao rural.",
            "Velocidade media do vento municipal medida pela media dos pontos do CEPEL a 100m.",
            "Painel construido sem exclusoes amostrais.",
            "Registro Civil (ARPEN/transparencia.registrocivil.org.br) e Censo 2010 sao unidos por nome municipal normalizado dentro da UF; consulte diagnosticos_qualidade para monitorar perdas.",
        ],
    }
    PANEL_METADATA.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"Painel salvo em: {PANEL_CSV}")
    print(f"Linhas: {len(panel)}")
    print(f"Municipios: {panel['cd_mun'].nunique()}")


if __name__ == "__main__":
    main()
