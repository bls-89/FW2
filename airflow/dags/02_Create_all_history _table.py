from datetime import datetime
import requests
import psycopg2 as ps2
from airflow.hooks.postgres_hook import PostgresHook
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

# соединение с базой 
conn_id = Variable.get("conn_name")

#-----------------------------------------------------------APPLE---------------------------------------------------------------------------
#получаем данные из переменных
function_value = Variable.get("function")
symbol_value_apple= Variable.get("symbol_apple") 
interval_value = Variable.get("interval")
outputsize_value = Variable.get("outputsize")
apikey_value = Variable.get("apikey")
base_url = Variable.get("base_url")
symbol_value_ibm = Variable.get("symbol_ibm") 
symbol_value_tesla = Variable.get("symbol_tesla") 

#записываем словарь из полученных переменных
params_apple = {
    'function': function_value,
    'symbol': symbol_value_apple,
    'interval': interval_value,
    'outputsize': outputsize_value,
    'apikey': apikey_value
}


params_ibm = {
    'function': function_value,
    'symbol': symbol_value_ibm,
    'interval': interval_value,
    'outputsize': outputsize_value,
    'apikey': apikey_value
}

params_tesla = {
    'function': function_value,
    'symbol': symbol_value_tesla,
    'interval': interval_value,
    'outputsize': outputsize_value,
    'apikey': apikey_value
}


#собираем запрос в кучу
url_apple = requests.Request('GET', base_url, params=params_apple).prepare().url
url_ibm = requests.Request('GET', base_url, params=params_ibm).prepare().url
url_tesla = requests.Request('GET', base_url, params=params_tesla).prepare().url

def load_data_apple(**context):
    # соединяемся с БД
    hook = PostgresHook(postgres_conn_id=conn_id)
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        # создадим сначала  таблицу
        cursor.execute(f"""CREATE TABLE IF NOT EXISTS apple_FULL(
                DateTime TIMESTAMP(0),
                Open FLOAT,
                High FLOAT,
                Low FLOAT,
                Close FLOAT,
                Volume INT
                    );""")
        response = requests.get(url_apple)
        data = response.json()

        for datetime, values in data['Time Series (15min)'].items():
            cursor.execute(f"""INSERT INTO apple_FULL (DateTime, Open, High, Low, Close, Volume)
                VALUES ('{datetime}', {values['1. open']}, {values['2. high']}, {values['3. low']}, {values['4. close']}, {values['5. volume']})
                """)

        conn.commit()

        cursor.close()
        conn.close()
        print("Данные apple успешно загружены в таблицу!")
        print("СТАТУС: ",context["task_instance"].current_state())
    except Exception as error:
        conn.rollback()
        raise Exception(f'Загрузить данные apple не получилось: {error}!')
#-----------------------------------------------------------IBM---------------------------------------------------------------------------



def load_data_ibm(**context):
    # соединяемся с БД
    hook = PostgresHook(postgres_conn_id=conn_id)
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        # создадим сначала  таблицу
        cursor.execute(f"""CREATE TABLE IF NOT EXISTS ibm_FULL(
                DateTime TIMESTAMP(0),
                Open FLOAT,
                High FLOAT,
                Low FLOAT,
                Close FLOAT,
                Volume INT
                    );""")
        response = requests.get(url_ibm)
        data = response.json()

        for datetime, values in data['Time Series (15min)'].items():
            cursor.execute(f"""INSERT INTO ibm_FULL (DateTime, Open, High, Low, Close, Volume)
                VALUES ('{datetime}', {values['1. open']}, {values['2. high']}, {values['3. low']}, {values['4. close']}, {values['5. volume']})
                """)

        conn.commit()

        cursor.close()
        conn.close()
        print("Данные ibm успешно загружены в таблицу!")
        print("СТАТУС: ",context["task_instance"].current_state())
    except Exception as error:
        conn.rollback()
        raise Exception(f'Загрузить данные ibm не получилось: {error}!')
#-----------------------------------------------------------TESLA---------------------------------------------------------------------------

def load_data_tesla(**context):
    # соединяемся с БД
    hook = PostgresHook(postgres_conn_id=conn_id)
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        # создадим сначала  таблицу
        cursor.execute(f"""CREATE TABLE IF NOT EXISTS tesla_FULL(
                DateTime TIMESTAMP(0),
                Open FLOAT,
                High FLOAT,
                Low FLOAT,
                Close FLOAT,
                Volume INT
                    );""")
        response = requests.get(url_tesla)
        data = response.json()

        for datetime, values in data['Time Series (15min)'].items():
            cursor.execute(f"""INSERT INTO tesla_FULL (DateTime, Open, High, Low, Close, Volume)
                VALUES ('{datetime}', {values['1. open']}, {values['2. high']}, {values['3. low']}, {values['4. close']}, {values['5. volume']})
                """)

        conn.commit()

        cursor.close()
        conn.close()
        print("Данные tesla успешно загружены в таблицу!")
        print("СТАТУС: ",context["task_instance"].current_state())
    except Exception as error:
        conn.rollback()
        raise Exception(f'Загрузить данные tesla не получилось: {error}!')


# аргументы дага по умолчанию
default_args = {
    "owner": "BS",
    "retries": 5,
    "retry_delay": 5,
    "start_date": datetime(2023, 12, 10),
}
with DAG(dag_id="02_Create_all_historical_table",
         default_args=default_args,
         schedule_interval="@once",
         description= "Создание таблиц с историческими данными",
         template_searchpath = "/tmp",
         tags=["python","BS"],
         catchup=False) as dag:

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    # загружаем в postgresql сырые данные
    load_data_to_apple = PythonOperator(
        task_id='load_data_apple',
        python_callable=load_data_apple
    )
    load_data_to_ibm = PythonOperator(
        task_id='load_data_ibm',
        python_callable=load_data_ibm
    )

    load_data_to_tesla = PythonOperator(
        task_id='load_data_tesla',
        python_callable=load_data_tesla
    )

    start >> [load_data_to_apple, load_data_to_ibm, load_data_to_tesla]>> end