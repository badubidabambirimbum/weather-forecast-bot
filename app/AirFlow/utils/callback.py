from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.models import Variable
from pendulum import timezone

def notify_telegram_failure(context):
    local_tz = timezone("Europe/Moscow")
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