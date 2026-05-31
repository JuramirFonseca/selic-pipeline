import logging
import os
from datetime import datetime
from functools import wraps
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import requests

log = logging.getLogger(__name__)

BASE_PATH = os.getenv('AIRFLOW_DATA_PATH', './data')

def save_parquet(output_dir: str, prefix: str = 'selic'):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            data = func(*args, **kwargs)
            now = datetime.now()
            datetime_insert = now.strftime('%Y-%m-%d %H:%M:%S')
            output_path = (
                f"{output_dir}/{prefix}_{now.strftime('%Y%m%d_%H%M%S')}.parquet"
                )
            for row in data:
                row['datetime_insert'] = datetime_insert
            Path(output_path).parent.mkdir(parents=True, exist_ok=True)
            table = pa.Table.from_pylist(data)
            pq.write_table(table, output_path)
            return output_path
        return wrapper
    return decorator


@save_parquet(f"{BASE_PATH}/bronze/selic")
def ingest(date_start: str = '01/01/2020', date_end: str = '31/12/2024') -> list[dict]:
    base_url = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.11/dados"
    url = (
        f"{base_url}?formato=json"
        f"&dataInicial={date_start}&dataFinal={date_end}"
    )
    try:
        response = requests.get(url, timeout=60)
        response.raise_for_status()
    except requests.exceptions.Timeout:
        raise RuntimeError("Timeout ao conectar na API do BCB") from None
    except requests.exceptions.HTTPError as exc:
        raise RuntimeError(
            f"Erro HTTP da API do BCB: {exc.response.status_code}"
        ) from exc

    data = response.json()
    if not data:
        raise ValueError("API do BCB retornou lista vazia para o período solicitado")

    log.info("Bronze: %d registros recebidos da API", len(data))
    return data
