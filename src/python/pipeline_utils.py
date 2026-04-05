import json
import re
import unicodedata
import urllib.error
import urllib.request
import warnings
from pathlib import Path

import geopandas as gpd
import pandas as pd


def parse_ptbr_number(value: object) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    text = text.replace(".", "").replace(",", ".")
    try:
        return float(text)
    except ValueError:
        return None


def normalize_text(value: object) -> str:
    text = "" if value is None else str(value)
    text = unicodedata.normalize("NFKD", text)
    text = "".join(char for char in text if not unicodedata.combining(char))
    return re.sub(r"\s+", " ", text).strip().lower()


def parse_description_table(description: str) -> dict:
    matches = re.findall(r"<td>([^<]+)</td>\s*<td>([^<]*)</td>", description or "")
    return {key.strip(): value.strip() for key, value in matches}


def ensure_directories(paths: list[Path]) -> None:
    for path in paths:
        path.mkdir(parents=True, exist_ok=True)


def write_metadata(path: Path, payload: dict) -> None:
    with path.open("w", encoding="utf-8") as file:
        json.dump(payload, file, ensure_ascii=False, indent=2)


def export_shapefile_with_short_names(gdf: gpd.GeoDataFrame, output_path: Path, field_map: dict[str, str]) -> None:
    export = gdf.rename(columns=field_map)
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message="Column names longer than 10 characters.*")
        export.to_file(output_path, driver="ESRI Shapefile", encoding="utf-8")


def safe_delete(path: Path) -> None:
    if path.exists():
        path.unlink()


def to_numeric_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    for column in columns:
        df[column] = pd.to_numeric(df[column], errors="coerce")
    return df


def http_get_text(url: str, timeout: int = 60) -> str:
    """Baixa o corpo de uma URL como texto.

    Usa apenas a biblioteca padrao para evitar dependencias especificas de
    plataforma (como ``curl.exe`` no Windows).
    """
    request = urllib.request.Request(url, headers={"User-Agent": "filhos-dos-ventos/1.0"})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            charset = response.headers.get_content_charset() or "utf-8"
            return response.read().decode(charset, errors="replace")
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Falha ao baixar {url}: {exc}") from exc


def concat_geoframes(frames: list[gpd.GeoDataFrame], context: str) -> gpd.GeoDataFrame:
    """Concatena uma lista nao vazia de GeoDataFrames preservando o CRS."""
    if not frames:
        raise ValueError(f"Lista de GeoDataFrames vazia para: {context}")
    return gpd.GeoDataFrame(pd.concat(frames, ignore_index=True), crs=frames[0].crs)
