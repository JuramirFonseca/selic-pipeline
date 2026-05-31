import logging
import os

import pandas as pd

log = logging.getLogger(__name__)

BASE_PATH = os.getenv('AIRFLOW_DATA_PATH', './data')
INPUT_PATH = f'{BASE_PATH}/bronze/selic'
OUTPUT_PATH = f'{BASE_PATH}/silver/selic/selic_clean.parquet'
MIN_RECORDS = 100
MAX_GAP_DAYS = 10


def structure_validation(
    data: pd.DataFrame,
    name_file: str,
    expected_columns: list[str],
) -> bool:
    missing = [col for col in expected_columns if col not in data.columns]
    
    if missing:
        raise ValueError(f"Colunas faltando: {missing} em {name_file}")
    return True


def volume_validation(data: pd.DataFrame, min_records: int = MIN_RECORDS) -> None:
    if len(data) < min_records:
        log.warning(
            "Silver: volume abaixo do esperado — %d registros "
            "(mínimo esperado: %d)",
            len(data),
            min_records,
        )
    else:
        log.info("Silver: volume OK — %d registros", len(data))


def temporal_continuity_validation(
    data: pd.DataFrame,
    max_gap_days: int = MAX_GAP_DAYS,
) -> None:
    dates = data['data'].sort_values().reset_index(drop=True)
    gaps = dates.diff().dropna()
    large_gaps = gaps[gaps.dt.days > max_gap_days]
    if not large_gaps.empty:
        gap_dates = dates[large_gaps.index].tolist()
        log.warning(
            "Silver: %d lacuna(s) acima de %d dias detectada(s). "
            "Datas após lacuna: %s",
            len(large_gaps),
            max_gap_days,
            gap_dates,
        )


def value_range_validation(data: pd.DataFrame) -> None:
    invalid = data[data['valor'] <= 0]
    if not invalid.empty:
        log.warning(
            "Silver: %d registro(s) com valor <= 0. Datas: %s",
            len(invalid),
            invalid['data'].tolist(),
        )
    else:
        log.info("Silver: range de valores OK — todos positivos")      


def concat_bronze_files() -> pd.DataFrame:
    files = [
        os.path.join(INPUT_PATH, f)
        for f in os.listdir(INPUT_PATH)
        if f.endswith('.parquet')
    ]
    if not files:
        raise FileNotFoundError(
            f"Nenhum arquivo Parquet encontrado em {INPUT_PATH}"
        )

    dfs = []
    for file in files:
        df_temp = pd.read_parquet(file)
        if structure_validation(
            data=df_temp,
            expected_columns=['data', 'valor', 'datetime_insert'],
            name_file=file,
        ):
            dfs.append(df_temp)

    log.info("Silver: %d arquivo(s) Bronze carregado(s)", len(dfs))
    return pd.concat(dfs, ignore_index=True)


def transform(data: pd.DataFrame) -> pd.DataFrame:
    data['valor'] = data['valor'].astype(float).round(2)
    data["data"] = pd.to_datetime(data["data"], format="%d/%m/%Y")
    data['datetime_insert'] = pd.to_datetime(data['datetime_insert'])
    data = data.dropna(subset=["valor"])
    data = data.sort_values('datetime_insert', ascending=False)
    data = data.drop_duplicates(subset=['data'], keep='first')
    data = data.sort_values('data').reset_index(drop=True)
    return data


def save(data: pd.DataFrame) -> None:
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    data.to_parquet(OUTPUT_PATH, index=False)
    log.info("Silver: %d registros salvos em %s", len(data), OUTPUT_PATH)


def clean_silver() -> None:
    df = concat_bronze_files()
    df = transform(df)
    volume_validation(df)
    temporal_continuity_validation(df)
    value_range_validation(df)
    save(df)