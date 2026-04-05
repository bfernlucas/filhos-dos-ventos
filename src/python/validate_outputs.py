import json
import sys

import geopandas as gpd
import pandas as pd

from pipeline_config import FINAL_BASE_CSV, FINAL_BASE_GPKG, FINAL_BASE_SHP, FINAL_METADATA_JSON, GPKG_LAYERS


class ValidationError(RuntimeError):
    """Erro levantado quando as saidas finais nao estao consistentes."""


def _check(condition: bool, message: str) -> None:
    if not condition:
        raise ValidationError(message)


def main() -> int:
    for path in (FINAL_BASE_CSV, FINAL_BASE_SHP, FINAL_BASE_GPKG, FINAL_METADATA_JSON):
        if not path.exists():
            print(f"ERRO: arquivo esperado nao encontrado: {path}", file=sys.stderr)
            return 1

    try:
        csv = pd.read_csv(FINAL_BASE_CSV)
        shp = gpd.read_file(FINAL_BASE_SHP)
        gpkg = gpd.read_file(FINAL_BASE_GPKG, layer=GPKG_LAYERS["municipios_base"])
        with FINAL_METADATA_JSON.open("r", encoding="utf-8") as file:
            metadata = json.load(file)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        print(f"ERRO ao ler saidas finais: {exc}", file=sys.stderr)
        return 1

    try:
        _check(
            len(csv) == len(shp) == len(gpkg) == metadata["municipios_total"],
            f"Contagens divergentes entre CSV ({len(csv)}), SHP ({len(shp)}), "
            f"GPKG ({len(gpkg)}) e metadata ({metadata['municipios_total']}).",
        )
        _check(
            int(csv["semi_dum"].sum()) == metadata["municipios_semiarido"],
            "Total de municipios no semiarido no CSV nao bate com metadata.",
        )
        _check(
            int((csv["aneel_eol_n"] > 0).sum()) == metadata["municipios_com_eolicas_aneel"],
            "Total de municipios com eolicas ANEEL no CSV nao bate com metadata.",
        )
    except ValidationError as exc:
        print(f"ERRO de validacao: {exc}", file=sys.stderr)
        return 1

    print("Validacao concluida com sucesso.")
    print(f"Municipios: {metadata['municipios_total']}")
    print(f"Municipios no semiarido: {metadata['municipios_semiarido']}")
    print(f"Municipios com eolicas ANEEL: {metadata['municipios_com_eolicas_aneel']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
