import os
from airflow import DAG
from airflow.contrib.hooks.fs_hook import FSHook
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.hooks.mysql_hook import MySqlHook
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from structlog import get_logger
import pandas as pd
import requests
import json


logger = get_logger()

COLUMNS = {
    "Dates": "",
    "Delta": "",
    "Total":""
}

DATE_COLUMNS = ["Dates"]

FILE_CONNECTION_NAME = 'monitor_file'
CONNECTION_DB_NAME = 'mysql_db'

def etl_process(**kwargs):
    logger.info(kwargs["execution_date"])
    r = requests.get('https://api.covid19india.org/v4/timeseries.json', auth=(user, pwd))
    response = r.json()
    file_path = FSHook(FILE_CONNECTION_NAME).get_path()
    filename = timeseries.json
    mysql_connection = MySqlHook(mysql_conn_id=CONNECTION_DB_NAME).get_sqlalchemy_engine()
    full_path = f'{file_path}/{filename}'
    df = pd.read_json(full_path, encoding = "ISO-8859-1").rename(columns= {'Lat': 'lat', 'Long': 'lon'})
    data = df.melt(id_vars=["Dates": "", "Delta": "", "Total": "" ], var_name='Fecha', value_name='Casos')
    with mysql_connection.begin() as connection:
        connection.execute("DELETE FROM test.confirmado WHERE 1=1")
        df1 = data.rename(columns=COLUMNS).to_sql('confirmado', con=connection, schema='test', if_exists='append', index=False)
        df2 = df1.Agg(Max(col(Dates))).collect()
    os.remove(full_path)

    logger.info(f"Rows inserted {len(data.index)}")


dag = DAG('confirmados', description='Confirmados',
          default_args={
              'owner': 'himanshu',
              'depends_on_past': False,
              'max_active_runs': 1,
              'start_date': days_ago(5)
          },
          schedule_interval='0 1 * * *',
          catchup=False)

sensor = FileSensor(task_id="file_sensor_task_confirmed",
                    dag=dag,
                    filepath='time_series.json',
                    fs_conn_id=FILE_CONNECTION_NAME,
                    poke_interval=10,
                    timeout=600)

etl = PythonOperator(task_id="confirmed_etl",
                     provide_context=True,
                     python_callable=etl_process,
                     dag=dag
                     )

sensor >> etl
