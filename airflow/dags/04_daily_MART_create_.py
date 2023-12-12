from datetime import datetime,timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook

# аргументы дага по умолчанию
default_args = {
    "owner": "BS",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

# соединение с базой 
conn_id = Variable.get("conn_name")

def CREATE_MART(**context):
    # соединяемся с БД
    hook = PostgresHook(postgres_conn_id=conn_id)
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:#очистим витринку от греха)
        cursor.execute(f"""DROP VIEW IF EXISTS MART;""")
        conn.commit()
        # делаем витрину запросом
        cursor.execute(f"""CREATE VIEW MART AS
                (SELECT 
                'IBM' AS "Ключ категории",
                'IBM' AS "Валюта",
                SUM(Volume) AS "Объем за 24 часа",
                (SELECT Open FROM (SELECT Open, ROW_NUMBER() OVER (ORDER BY DateTime) as rn FROM IBM_daily) t WHERE rn = 1) AS "Курс открытия",
                (SELECT Close FROM (SELECT Close, ROW_NUMBER() OVER (ORDER BY DateTime DESC) as rn FROM IBM_daily) t WHERE rn = 1) AS "Курс закрытия",
                ((SELECT Close FROM (SELECT Close, ROW_NUMBER() OVER (ORDER BY DateTime DESC) as rn FROM IBM_daily) t WHERE rn = 1) - (SELECT Open FROM (SELECT Open, ROW_NUMBER() OVER (ORDER BY DateTime) as rn FROM IBM_daily) t WHERE rn = 1)) / (SELECT Open FROM (SELECT Open, ROW_NUMBER() OVER (ORDER BY DateTime) as rn FROM IBM_daily) t WHERE rn = 1) * 100 as "Разница в %",
                (SELECT DateTime FROM (SELECT DateTime, ROW_NUMBER() OVER (ORDER BY Volume DESC) as rn FROM IBM_daily) t WHERE rn = 1) AS "Интервал макс. объема",
                (SELECT DateTime FROM (SELECT DateTime, ROW_NUMBER() OVER (ORDER BY High DESC) as rn FROM IBM_daily) t WHERE rn = 1) AS "Интервал макс. курса",
                (SELECT DateTime FROM (SELECT DateTime, ROW_NUMBER() OVER (ORDER BY Low) as rn FROM IBM_daily) t WHERE rn = 1) AS "Интервал мин. курса"
                FROM
                IBM_daily
                UNION ALL
                SELECT 
                'APPLE' AS "Ключ категории",
                'APPLE' AS "Валюта",
                SUM(Volume) AS "Объем за 24 часа",
                (SELECT Open FROM (SELECT Open, ROW_NUMBER() OVER (ORDER BY DateTime) as rn FROM apple_daily) t WHERE rn = 1) AS "Курс открытия",
                (SELECT Close FROM (SELECT Close, ROW_NUMBER() OVER (ORDER BY DateTime DESC) as rn FROM apple_daily) t WHERE rn = 1) AS "Курс закрытия",
                ((SELECT Close FROM (SELECT Close, ROW_NUMBER() OVER (ORDER BY DateTime DESC) as rn FROM apple_daily) t WHERE rn = 1) - (SELECT Open FROM (SELECT Open, ROW_NUMBER() OVER (ORDER BY DateTime) as rn FROM apple_daily) t WHERE rn = 1)) / (SELECT Open FROM (SELECT Open, ROW_NUMBER() OVER (ORDER BY DateTime) as rn FROM apple_daily) t WHERE rn = 1) * 100 as "Разница в %",
                (SELECT DateTime FROM (SELECT DateTime, ROW_NUMBER() OVER (ORDER BY Volume DESC) as rn FROM apple_daily) t WHERE rn = 1) AS "Интервал макс. объема",
                (SELECT DateTime FROM (SELECT DateTime, ROW_NUMBER() OVER (ORDER BY High DESC) as rn FROM apple_daily) t WHERE rn = 1) AS "Интервал макс. курса",
                (SELECT DateTime FROM (SELECT DateTime, ROW_NUMBER() OVER (ORDER BY Low) as rn FROM apple_daily) t WHERE rn = 1) AS "Интервал мин. курса"
                FROM 
                apple_daily
                UNION ALL
                SELECT 
                'TESLA' AS "Ключ категории",
                'TESLA' AS "Валюта",
                SUM(Volume) AS "Объем за 24 часа",
                (SELECT Open FROM (SELECT Open, ROW_NUMBER() OVER (ORDER BY DateTime) as rn FROM tesla_daily) t WHERE rn = 1) AS "Курс открытия",
                (SELECT Close FROM (SELECT Close, ROW_NUMBER() OVER (ORDER BY DateTime DESC) as rn FROM tesla_daily) t WHERE rn = 1) AS "Курс закрытия",
                ((SELECT Close FROM (SELECT Close, ROW_NUMBER() OVER (ORDER BY DateTime DESC) as rn FROM tesla_daily) t WHERE rn = 1) - (SELECT Open FROM (SELECT Open, ROW_NUMBER() OVER (ORDER BY DateTime) as rn FROM tesla_daily) t WHERE rn = 1)) / (SELECT Open FROM (SELECT Open, ROW_NUMBER() OVER (ORDER BY DateTime) as rn FROM tesla_daily) t WHERE rn = 1) * 100 as "Разница в %",
                (SELECT DateTime FROM (SELECT DateTime, ROW_NUMBER() OVER (ORDER BY Volume DESC) as rn FROM tesla_daily) t WHERE rn = 1) AS "Интервал макс. объема",
                (SELECT DateTime FROM (SELECT DateTime, ROW_NUMBER() OVER (ORDER BY High DESC) as rn FROM tesla_daily) t WHERE rn = 1) AS "Интервал макс. курса",
                (SELECT DateTime FROM (SELECT DateTime, ROW_NUMBER() OVER (ORDER BY Low) as rn FROM tesla_daily) t WHERE rn = 1) AS "Интервал мин. курса"
                FROM 
                tesla_daily);""")
        conn.commit()

        cursor.close()
        conn.close()
        print("ВИТРИНА ГОТОВА!")
        print("СТАТУС: ",context["task_instance"].current_state())
    except Exception as error:
        conn.rollback()
        raise Exception(f'Загрузить данные в витрину не получилось: {error}!')


with DAG(
    default_args = default_args,
    dag_id = '04_Daily_MART_create',
    description = 'Создание витрины за сутки',
    start_date = datetime(2023,12,10),
    schedule_interval="10 6 * * 2-6",
    tags=["python","BS"],
    catchup=False

) as dag:
    
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    CREATE_MART = PythonOperator(
    task_id='CREATE_MART',
    python_callable=CREATE_MART,
    dag=dag)


    start >> CREATE_MART >> end
