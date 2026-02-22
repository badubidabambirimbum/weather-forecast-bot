from airflow import DAG
from datetime import datetime, timedelta
from pendulum import timezone

from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

from airflow.models import Variable

import sys
import os
# sys.path.append(os.path.abspath(os.path.join(os.getcwd(), '..')))

import machine_learning.utils.ml_storage as ms

city = 'Moscow'
timezone_city = 'Europe/Moscow'
local_tz = timezone("Europe/Moscow")
schedule = Variable.get(f"schedule_Model_{city}")

def notify_telegram_failure(context):
    local_time = (context["logical_date"].in_timezone(local_tz).strftime("%Y-%m-%d %H:%M:%S"))

    message = (
        f"❌ Task failed!\n"
        f"DAG: {context['dag'].dag_id}\n"
        f"Task: {context['task_instance'].task_id}\n"
        f"Execution date: {local_time}\n"
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
    'retry_delay': timedelta(minutes=1),                # Время ожидания между повторными попытками (5 минут).
    'on_failure_callback': notify_telegram_failure      # Функция notify_telegram_failure, которая будет вызвана при неудачном выполнении задачи
}



dag = DAG(
    dag_id=f'Model_{city}',
    start_date=datetime(2025, 4, 20, tzinfo=local_tz),
    schedule_interval=schedule,
    default_args=default_args,
    catchup=False,
    is_paused_upon_creation=True,
    tags=['Model', city],
    params={'city': city,
            'timezone': timezone_city}
)


start = DummyOperator(task_id='start')

# fit_model = PythonOperator(
#     task_id='fit_model',
#     python_callable=nrl.fit_model,
#     op_kwargs={'city'       : "{{ params.city }}",
#                'timezone'   : "{{ params.timezone }}"},
#     doc="Обучение модели нейронной сети",
#     dag=dag
# )

fit_model = DockerOperator(
    task_id="fit_model",
    image="ml_fit_model:latest",
    command="python fit_model.py --city {{ params.city }} --timezone {{ params.timezone }}",
    dag=dag,
    docker_url="unix://var/run/docker.sock",
    mounts=[
        Mount(
            source="/Users/alexey/PycharmProjects/weather-forecast-bot/models",     # путь на хосте
            target="/app/models",                                                   # путь в контейнере
            type="bind"
        )
    ],
    auto_remove=True,
    do_xcom_push=True,
    mount_tmp_dir=False,
)

load_metrics = PythonOperator(
    task_id='load_metrics',
    python_callable=ms.load_metrics,
    op_kwargs={'city': "{{ params.city }}"},
    doc="Загрузка метрик обучения",
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

start >> fit_model >> load_metrics >> telegram_message_success