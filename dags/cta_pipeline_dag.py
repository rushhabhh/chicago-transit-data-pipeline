from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'rushabh',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'cta_bus_data_pipeline',
    default_args=default_args,
    description='Fetch CTA bus data and process using Spark',
    schedule_interval='*/2 * * * *',  # every 2 minutes
    start_date=datetime(2025, 10, 16),
    catchup=False,
    tags=['cta', 'kafka', 'spark'],
) as dag:

    fetch_bus_data = BashOperator(
        task_id='fetch_bus_data',
        bash_command='source /path/to/your/venv/bin/activate && python /full/path/scripts/fetch_top_routes_data.py',
    )

    run_spark_streaming = BashOperator(
        task_id='run_spark_streaming',
        bash_command='spark-submit /full/path/scripts/spark_stream_consumer.py',
    )

    fetch_bus_data >> run_spark_streaming