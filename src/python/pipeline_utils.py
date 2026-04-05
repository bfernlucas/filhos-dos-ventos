import json
import re
import unicodedata
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
