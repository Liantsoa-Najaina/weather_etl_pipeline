from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime

from weather_etl_pipeline.scripts.transform_to_star import transform_to_star
from weather_etl_pipeline.scripts.merge_daily_and_historical import merge_daily_to_historical
from weather_etl_pipeline.scripts.extract import extract_weather
from weather_etl_pipeline.scripts.merge import merge_files

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 7, 1),
}

cities = ['Bangkok', 'Istanbul', 'London', 'Hong Kong', 'Mecca', 'Antalya', 'Dubai', 'Macau', 'Paris', 'Kuala Lumpur']

with DAG(
    dag_id='weather_data_pipeline_for_tourism',
    dag_display_name='Weather Data Pipeline for Tourism',
    default_args=default_args,
    max_active_runs=1,
    catchup=False,
) as dag:
    extract_weather_task = [
        PythonOperator(
            task_id=f'extract_{city.lower().replace(" ", "_")}_weather',
            python_callable=extract_weather,
            op_args=[city, "{{ var.value.API_KEY }}", "{{ ds }}"],
            show_return_value_in_logs=True,
        )
        for city in cities
    ]

    merge_files_task = PythonOperator(
        task_id='merge_files',
        python_callable=merge_files,
        op_args=["{{ ds }}"],
        show_return_value_in_logs=True,
    )

    merge_daily_to_historical_task = PythonOperator(
        task_id='merge_daily_to_historical',
        python_callable=merge_daily_to_historical,
        show_return_value_in_logs=True,
    )

    transform_to_star = PythonOperator(
        task_id='transform_to_star',
        python_callable=transform_to_star,
        show_return_value_in_logs=True,
    )

    extract_weather_task >> merge_files_task >> merge_daily_to_historical_task >> transform_to_star