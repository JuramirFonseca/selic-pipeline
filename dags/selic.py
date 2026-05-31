import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta

from include.bronze.selic_task1 import ingest
from include.silver.selic_task2 import clean_silver
from include.gold.selic_task3 import aggregate

default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "email_on_failure": False,
}

with DAG(
    dag_id="Selic",
    description="Pipeline Selic",
    default_args=default_args,
    schedule=None,
    start_date=pendulum.datetime(2025,1,1,tz="America/Sao_Paulo"),
    catchup=False,
    tags=["BeAnalytic","Pipeline","Selic"],
    params={                         
        "date_start": "01/01/2020",
        "date_end": "31/12/2024",
    }
) as dag:
    task1 = PythonOperator(
        task_id='ingest',
        python_callable=ingest,
        op_kwargs={                  
            "date_start": "{{ params.date_start }}",
            "date_end": "{{ params.date_end }}",
        }
    )
    task2 = PythonOperator(task_id='transform', python_callable=clean_silver)
    task3 = PythonOperator(task_id='aggregate', python_callable=aggregate)

    task1 >> task2 >> task3