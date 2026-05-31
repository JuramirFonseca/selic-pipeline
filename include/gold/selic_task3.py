import logging
import os
from pathlib import Path

import pandas as pd

log = logging.getLogger(__name__)

BASE_PATH = os.getenv('AIRFLOW_DATA_PATH', './data')
INPUT_PATH = f"{BASE_PATH}/silver/selic/selic_clean.parquet"
OUTPUT_PATH = f"{BASE_PATH}/gold/selic/selic_metrics.parquet"
INPUT_PATH_EXPORT = f"{BASE_PATH}/gold/selic/selic_metrics.parquet"
OUTPUT_PATH_EXPORT = f"{BASE_PATH}/gold/selic/selic_metrics.xlsx"

def export_results(
    input_path: str = INPUT_PATH_EXPORT,
    output_path: str = OUTPUT_PATH_EXPORT,
) -> None:
    if not Path(input_path).exists():
        raise FileNotFoundError(
            f"Arquivo Gold não encontrado: {input_path}"
        )
    df = pd.read_parquet(input_path)
    df.to_excel(output_path, index=False)
    log.info("Gold: resultado exportado para %s", output_path)
    log.info(
        "Gold: shape=%s | colunas=%s",
        df.shape,
        list(df.columns),
    )


def aggregate() -> None:
    if not Path(INPUT_PATH).exists():
        raise FileNotFoundError(
            f"Arquivo Silver não encontrado: {INPUT_PATH}"
        )
    df = pd.read_parquet(INPUT_PATH)

    df["ano"] = df["data"].dt.year
    df["mes"] = df["data"].dt.month

    month = (
        df.groupby(["ano", "mes"])["valor"]
        .mean()
        .reset_index()
        .rename(columns={"valor": "media_mensal"})
    )

    month["variacao_mensal"] = (
        month["media_mensal"].pct_change().mul(100).fillna(0)
    )

    def taxa_acumulada(grupo) -> float:
        return ((1 + grupo / 100).prod() - 1) * 100

    anual = (
        df.groupby("ano")["valor"]
        .apply(taxa_acumulada)
        .reset_index()
        .rename(columns={"valor": "taxa_acumulada_anual"})
    )

    resultado = month.merge(anual, on="ano", how="left")
    resultado["variacao_mensal"] = resultado["variacao_mensal"].round(4)
    resultado["media_mensal"] = resultado["media_mensal"].round(4)
    resultado["taxa_acumulada_anual"] = (
        resultado["taxa_acumulada_anual"].round(4)
    )

    Path(OUTPUT_PATH).parent.mkdir(parents=True, exist_ok=True)
    resultado.to_parquet(OUTPUT_PATH, index=False)
    export_results()
    log.info(
        "Gold: %d meses processados salvos em %s",
        len(resultado),
        OUTPUT_PATH,
    )