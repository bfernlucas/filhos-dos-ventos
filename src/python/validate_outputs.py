import json

import geopandas as gpd
import pandas as pd

from pipeline_config import FINAL_BASE_CSV, FINAL_BASE_GPKG, FINAL_BASE_SHP, FINAL_METADATA_JSON, GPKG_LAYERS


def main() -> None:
    csv = pd.read_csv(FINAL_BASE_CSV)
    shp = gpd.read_file(FINAL_BASE_SHP)
    gpkg = gpd.read_file(FINAL_BASE_GPKG, layer=GPKG_LAYERS["municipios_base"])

    with FINAL_METADATA_JSON.open("r", encoding="utf-8") as file:
        metadata = json.load(file)

    assert len(csv) == len(shp) == len(gpkg) == metadata["municipios_total"]
    assert int(csv["semi_dum"].sum()) == metadata["municipios_semiarido"]
    assert int((csv["aneel_eol_n"] > 0).sum()) == metadata["municipios_com_eolicas_aneel"]

    print("Validacao concluida com sucesso.")
    print(f"Municipios: {metadata['municipios_total']}")
    print(f"Municipios no semiarido: {metadata['municipios_semiarido']}")
    print(f"Municipios com eolicas ANEEL: {metadata['municipios_com_eolicas_aneel']}")


if __name__ == "__main__":
    main()
