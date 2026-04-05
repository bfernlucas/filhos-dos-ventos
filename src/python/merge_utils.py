"""Utilitarios de merge para o painel municipal do Nordeste.

Este modulo concentra as melhores praticas de pareamento entre bases que
usam o codigo do IBGE como chave e bases que so publicam o nome do
municipio. A estrategia segue a ordem:

1. Match deterministico por codigo IBGE quando existir chave compativel
   (caso do Censo 2010, cujo codigo de 6 digitos corresponde aos seis
   primeiros digitos do codigo de 7 digitos da malha municipal atual).
2. Match exato por nome normalizado dentro da UF.
3. Match aproximado (fuzzy) via RapidFuzz (Levenshtein / WRatio) dentro
   da UF, com limiar conservador para reduzir falsos positivos.

O objetivo e preservar o maximo possivel de linhas nao-pareadas pela
etapa exata, mantendo um log auditavel de cada decisao do fuzzy matcher.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

import pandas as pd
from rapidfuzz import fuzz, process

from pipeline_utils import normalize_text


# fuzz.ratio (Levenshtein normalizado) e mais estrito que WRatio e evita
# falsos positivos por substring comum (ex.: "acu" contido em "ipanguacu").
# Com nomes municipais curtos, esse controle e essencial.
DEFAULT_FUZZY_SCORER = fuzz.ratio
DEFAULT_FUZZY_THRESHOLD = 88.0


@dataclass
class MergeReport:
    """Resumo auditavel de um merge por nome municipal."""

    exact_matches: int = 0
    fuzzy_matches: int = 0
    unmatched: int = 0
    fuzzy_pairs: list[dict] = field(default_factory=list)
    unmatched_rows: list[dict] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "exact_matches": self.exact_matches,
            "fuzzy_matches": self.fuzzy_matches,
            "unmatched": self.unmatched,
            "fuzzy_pairs_sample": self.fuzzy_pairs[:50],
            "unmatched_sample": self.unmatched_rows[:50],
        }


def censo_crosswalk_by_ibge_code(
    censo: pd.DataFrame, municipal: pd.DataFrame, code_col: str = "cd_raw"
) -> pd.DataFrame:
    """Pareia o Censo 2010 com a malha municipal atual pelo codigo IBGE.

    As planilhas ``.1.1.xls`` do Censo 2010 ja publicam o codigo IBGE
    completo de 7 digitos. O cruzamento portanto e direto contra
    ``cd_mun`` (tambem 7 digitos) da malha 2025. Linhas com codigos mais
    longos (9+ digitos) correspondem a distritos/subdivisoes e sao
    descartadas antes do merge.
    """
    censo = censo.copy()
    censo[code_col] = pd.to_numeric(censo[code_col], errors="coerce").astype("Int64")
    # Mantem apenas codigos de 7 digitos (1_000_000 a 9_999_999). Acima
    # disso sao codigos de distritos e subdivisoes.
    censo = censo[
        censo[code_col].between(1_000_000, 9_999_999, inclusive="both")
    ].copy()

    muni = municipal[["cd_mun"]].drop_duplicates().copy()
    muni["cd_mun_int"] = (
        muni["cd_mun"].astype(str).str.zfill(7).astype("Int64")
    )

    merged = censo.merge(
        muni[["cd_mun", "cd_mun_int"]],
        left_on=code_col,
        right_on="cd_mun_int",
        how="left",
    )
    return merged.drop(columns=["cd_mun_int"])


def fuzzy_merge_names_within_uf(
    left: pd.DataFrame,
    right: pd.DataFrame,
    left_name: str,
    right_name: str,
    uf_col: str,
    threshold: float = DEFAULT_FUZZY_THRESHOLD,
) -> tuple[pd.DataFrame, MergeReport]:
    """Merge em duas camadas: exato por nome normalizado, depois fuzzy.

    Parametros
    ----------
    left
        DataFrame da fonte externa (ex.: Registro Civil) contendo pelo
        menos ``left_name`` e ``uf_col``.
    right
        DataFrame da malha municipal oficial contendo ``right_name``,
        ``uf_col`` e ``cd_mun``.
    left_name, right_name
        Colunas com o nome do municipio em cada lado (serao normalizadas
        internamente sem modificar os DataFrames de entrada).
    uf_col
        Coluna com a sigla da UF presente em ambos os lados. O fuzzy
        match so acontece dentro da mesma UF para reduzir falsos positivos.
    threshold
        Score minimo do RapidFuzz ``WRatio`` (0-100) para aceitar um par
        fuzzy. O valor padrao e conservador (88) para evitar colar
        municipios distintos com grafias parecidas.

    Retorna
    -------
    (merged, report)
        ``merged`` contem todas as linhas de ``left`` com ``cd_mun``
        preenchido onde houve match exato ou fuzzy aceito. ``report``
        contem contagens e uma amostra auditavel dos pares fuzzy e das
        linhas nao pareadas.
    """
    report = MergeReport()

    left = left.copy()
    right = right[[right_name, uf_col, "cd_mun"]].drop_duplicates().copy()

    left["_norm"] = left[left_name].map(normalize_text)
    right["_norm"] = right[right_name].map(normalize_text)

    # 1. Match exato por nome normalizado dentro da UF.
    exact = left.merge(
        right[["_norm", uf_col, "cd_mun"]],
        on=["_norm", uf_col],
        how="left",
    )
    report.exact_matches = int(exact["cd_mun"].notna().sum())

    missing_mask = exact["cd_mun"].isna()
    if not missing_mask.any():
        report.unmatched = 0
        return exact.drop(columns="_norm"), report

    # 2. Fuzzy por UF para o residuo.
    right_by_uf: dict[str, list[tuple[str, str]]] = {}
    for uf, group in right.groupby(uf_col):
        right_by_uf[uf] = list(zip(group["_norm"].tolist(), group["cd_mun"].tolist()))

    fuzzy_cd = []
    for idx, row in exact.loc[missing_mask, ["_norm", uf_col, left_name]].iterrows():
        candidates = right_by_uf.get(row[uf_col])
        if not candidates:
            fuzzy_cd.append((idx, pd.NA, None, None))
            continue
        choices = [name for name, _ in candidates]
        match = process.extractOne(
            row["_norm"], choices, scorer=DEFAULT_FUZZY_SCORER, score_cutoff=threshold
        )
        if match is None:
            fuzzy_cd.append((idx, pd.NA, None, None))
            continue
        matched_name, score, pos = match
        cd_mun = candidates[pos][1]
        fuzzy_cd.append((idx, cd_mun, matched_name, float(score)))

    for idx, cd_mun, matched_name, score in fuzzy_cd:
        if pd.notna(cd_mun):
            exact.at[idx, "cd_mun"] = cd_mun
            report.fuzzy_matches += 1
            report.fuzzy_pairs.append(
                {
                    "left": exact.at[idx, left_name],
                    "match": matched_name,
                    "uf": exact.at[idx, uf_col],
                    "score": score,
                }
            )
        else:
            report.unmatched += 1
            report.unmatched_rows.append(
                {
                    "left": exact.at[idx, left_name],
                    "uf": exact.at[idx, uf_col],
                }
            )

    return exact.drop(columns="_norm"), report


def residual_fuzzy_fill(
    source: pd.DataFrame,
    municipal: pd.DataFrame,
    name_col: str,
    uf_col: str,
    threshold: float = DEFAULT_FUZZY_THRESHOLD,
    existing_code_col: Optional[str] = "cd_mun",
) -> tuple[pd.DataFrame, MergeReport]:
    """Preenche ``cd_mun`` nulo em ``source`` via fuzzy dentro da UF.

    Usado quando ja existe uma tentativa previa de match (por nome exato
    ou por codigo). Apenas as linhas sem ``cd_mun`` passam pelo fuzzy,
    preservando integralmente os matches deterministicos anteriores.
    """
    report = MergeReport()
    if existing_code_col not in source.columns:
        source = source.copy()
        source[existing_code_col] = pd.NA

    already = source[existing_code_col].notna()
    report.exact_matches = int(already.sum())

    if already.all():
        return source, report

    missing = source.loc[~already, [name_col, uf_col]].copy()
    missing["_norm"] = missing[name_col].map(normalize_text)

    right = municipal[[name_col, uf_col, "cd_mun"]].drop_duplicates().copy() \
        if name_col in municipal.columns else None
    if right is None:
        # Se a malha nao carrega o nome_col, assumimos que a coluna padrao
        # normalizada esta em municipal.
        right = municipal[["nm_mun_norm", uf_col, "cd_mun"]].drop_duplicates().copy()
        right = right.rename(columns={"nm_mun_norm": "_norm"})
    else:
        right["_norm"] = right[name_col].map(normalize_text)

    right_by_uf: dict[str, list[tuple[str, str]]] = {}
    for uf, group in right.groupby(uf_col):
        right_by_uf[uf] = list(zip(group["_norm"].tolist(), group["cd_mun"].tolist()))

    for idx, row in missing.iterrows():
        candidates = right_by_uf.get(row[uf_col])
        if not candidates:
            report.unmatched += 1
            report.unmatched_rows.append({"left": row[name_col], "uf": row[uf_col]})
            continue
        choices = [name for name, _ in candidates]
        match = process.extractOne(
            row["_norm"], choices, scorer=DEFAULT_FUZZY_SCORER, score_cutoff=threshold
        )
        if match is None:
            report.unmatched += 1
            report.unmatched_rows.append({"left": row[name_col], "uf": row[uf_col]})
            continue
        matched_name, score, pos = match
        cd_mun = candidates[pos][1]
        source.at[idx, existing_code_col] = cd_mun
        report.fuzzy_matches += 1
        report.fuzzy_pairs.append(
            {
                "left": row[name_col],
                "match": matched_name,
                "uf": row[uf_col],
                "score": float(score),
            }
        )

    return source, report
