from airflow import DAG
from datetime import datetime, timedelta
from pendulum import timezone

from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator

from airflow.models import Variable

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.getcwd(), '..')))

import library.airflow_functions as afl

local_tz = timezone("Europe/Moscow")
schedule = Variable.get("schedule_Krasnodar_GisMeteo")

def notify_telegram_failure(context):
    message = (
        f"❌ Task failed!\n"
        f"DAG: {context['dag'].dag_id}\n"
        f"Task: {context['task_instance'].task_id}\n"
        f"Execution date: {context['ts']}\n"
        f"Exception: {context.get('exception')}"
    )
    TelegramOperator(
        task_id='notify_failure',
        token=Variable.get("telegram_token"),
        chat_id=Variable.get("telegram_chat_id"),
        text=message,
        dag=context['dag'],
    ).execute(context=context)


default_args = {
    'owner': '@CHAO',                                   # Указывает владельца задачи
    'depends_on_past': False,                           # Если False, задача не зависит от успешного выполнения своего предыдущего запуска
    'retries': 1,                                       # Количество попыток перезапуска задачи в случае её падения
    'retry_delay': timedelta(minutes=30),               # Время ожидания между повторными попытками (5 минут).
    'on_failure_callback': notify_telegram_failure      # Функция notify_telegram_failure, которая будет вызвана при неудачном выполнении задачи
}



dag = DAG(
    dag_id='Krasnodar_GisMeteo',
    start_date=datetime(2025, 4, 20, tzinfo=local_tz),
    schedule_interval=schedule,
    default_args=default_args,
    catchup=False,
    is_paused_upon_creation=True,
    tags=['Krasnodar', 'GisMeteo'],
    params={'table': 't_krasnodar_gismeteo',
            'city': 'Krasnodar',
            'type': 'GisMeteo'}
)


start = DummyOperator(task_id='start')

get_weather_forecast = PythonOperator(
    task_id='get_weather_forecast',
    python_callable=afl.get_weather_forecast_GisMeteo,
    op_kwargs={'city': "{{ params.city }}",
               'type': "{{ params.type }}"},
    doc="Получение данных с сайта",
    dag=dag
)

create_df = PythonOperator(
    task_id='create_DF',
    python_callable=afl.create_today,
    op_kwargs={'city': "{{ params.city }}",
               'type': "{{ params.type }}",
               'airflow_mode': True},
    doc="Формирование df из одной строки с загруженными данными",
    dag=dag
)

truncate_table = PostgresOperator(
    task_id='truncate_backup_table',
    postgres_conn_id='database_connect',
    doc="TRUNCATE backup таблицы",
    sql='''TRUNCATE TABLE backup.{{ params.table }}''',
    dag=dag
)

insert_table = PostgresOperator(
    task_id='insert_backup_table',
    postgres_conn_id='database_connect',
    doc="Заполнение backup таблицы",
    sql='''INSERT INTO backup.{{ params.table }}
            SELECT * FROM prom.{{ params.table }}''',
    dag=dag
)

update_table = PythonOperator(
    task_id='update_table',
    python_callable=afl.update,
    op_kwargs={'city': "{{ params.city }}",
               'type': "{{ params.type }}",
               'airflow_mode': True},
    doc="Добавление новых данных в таблицу",
    dag=dag
)

telegram_message_success = TelegramOperator(
    task_id='telegram_message_success',
    token=Variable.get("telegram_token"),
    chat_id=Variable.get("telegram_chat_id"),
    text=(
        "✅ DAG succeeded!\n"
        "DAG: {{ dag.dag_id }}\n"
        "Execution date: {{ ts }}"
    ),
    dag=dag
)

start >> [get_weather_forecast, truncate_table]
get_weather_forecast >> create_df >> update_table
truncate_table >> insert_table >> update_table
update_table >> telegram_message_success