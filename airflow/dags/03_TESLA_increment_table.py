from datetime import datetime,timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from tesla_app import run_app

# аргументы дага по умолчанию
default_args = {
    "owner": "BS",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}



with DAG(
    default_args = default_args,
    dag_id = '03_TESLA_increment',
    description = 'Сбор данных об акциях TESLA за предыдущий день',
    start_date = datetime(2023,12,10),
    schedule_interval="0 6 * * 2-6",
    tags=["python","BS"],
    catchup=False

) as dag:
    
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    TESLA_increment_data = PythonOperator(
    task_id='TESLA_increment_data',
    python_callable=run_app,
    dag=dag)


    start >> TESLA_increment_data >> end
