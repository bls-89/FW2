from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

# аргументы дага по умолчанию
default_args = {
    "owner": "BS",
    "retries": 0,
    "start_date": datetime.today()
}

# функция добавления переменных в airflow

with DAG(dag_id="01_init",description = 'Прикручиваем connections и variables без геморроя', default_args=default_args, schedule_interval='@once', catchup=False) as dag:

    start = EmptyOperator(task_id='start')

    with TaskGroup("01_init", tooltip="Добавление connections") as init_tg:

        # ****************** добавление переменных ***********************

        set_variables = BashOperator(
            task_id = 'set_variables',
            bash_command='airflow variables import /opt/airflow/dags/variables.json'
        )

        # ****************** добавление connecions ***********************

        set_connections = BashOperator(
            task_id='set_connections',
            bash_command='airflow connections import /opt/airflow/dags/connections.json'
        )

    end = EmptyOperator(task_id='end')

    start >> init_tg >> end

