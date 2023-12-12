def run_app():
    from datetime import datetime
    import requests
    import psycopg2 as ps2
    from airflow.models import Variable
    from airflow.hooks.postgres_hook import PostgresHook

    #получаем данные из переменных
    function_value = Variable.get("function")
    symbol_value_apple= Variable.get("symbol_apple") 
    interval_value = Variable.get("interval")
    apikey_value = Variable.get("apikey")
    base_url = Variable.get("base_url")

    params_apple = {
    'function': function_value,
    'symbol': symbol_value_apple,
    'interval': interval_value,
    'apikey': apikey_value
    }
    url_apple = requests.Request('GET', base_url, params=params_apple).prepare().url
#-------------
    response = requests.get(url_apple)
    data = response.json()
#--------------

    #прикрутим плодключение к Postgres
    conn_id = Variable.get("conn_name")
    hook = PostgresHook(postgres_conn_id=conn_id)
    conn = hook.get_conn()



    cursor = conn.cursor()
    #создаем таблицу с сырыми 100 строками с интервалом по 15 минут. Из-за этого в нее попадают все данные за "вчера" и часть данных за "позавчера" (сделам ее временной)
    cursor.execute(f"""CREATE TEMP TABLE IF NOT EXISTS apple_raw (
        DateTime TIMESTAMP(0),
        Open FLOAT,
        High FLOAT,
        Low FLOAT,
        Close FLOAT,
        Volume INT
    );""")
    conn.commit()

    #cursor.execute("TRUNCATE TABLE apple_raw") #очистка RAW таблицы когда она не временная

    for datetime, values in data['Time Series (15min)'].items():
        cursor.execute(f"""
            INSERT INTO apple_raw (DateTime, Open, High, Low, Close, Volume)
            VALUES ('{datetime}', {values['1. open']}, {values['2. high']}, {values['3. low']}, {values['4. close']}, {values['5. volume']})
        """)

    conn.commit()

    #созадем вспомогательную таблицу с данными за прошедший торговый день (за "вчера")
    cursor.execute(f"""CREATE TABLE IF NOT EXISTS apple_daily (
        DateTime TIMESTAMP(0),
        Open FLOAT,
        High FLOAT,
        Low FLOAT,
        Close FLOAT,
        Volume INT
    );""")
    conn.commit()
    cursor.execute("TRUNCATE TABLE apple_daily")
    conn.commit()

    #выбираем из сырых данных те которые отвечают нашим требованиям и записываем в чистую таблицу
    cursor.execute(f"""INSERT INTO apple_daily
    SELECT *
    FROM apple_raw
    WHERE date_trunc('day', DateTime) = CURRENT_DATE - INTERVAL '1 day';;""")
    conn.commit()
    cursor.execute("SELECT * FROM apple_daily")
    res = cursor.fetchall()

    print(res)
    #добавляем все в архивную таблицу
    cursor.execute(f"""INSERT INTO apple_full
    SELECT * FROM apple_daily;""")
    conn.commit()
    cursor.close()
    conn.close()

if __name__ == '__main__':
        print('Application started')
