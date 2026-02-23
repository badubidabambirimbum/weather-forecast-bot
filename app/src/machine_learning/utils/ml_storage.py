import sys
import os
import json
import pandas as pd
from datetime import datetime
from core.database import DataBase

def load_metrics(city, airflow_mode=True, db=None, **kwargs):
    '''Загрузка полученных метрик с последнего шага обучения в БД'''

    if airflow_mode:
        from airflow.models import Variable
        ti = kwargs['ti']
        metrics_str = ti.xcom_pull(task_ids='fit_model')
        print('metrics_str:', metrics_str)
        metrics_dict = json.loads(metrics_str)

        db = DataBase(host=Variable.get('host_db'),
                      port=Variable.get('port_db'),
                      user=Variable.get('user_db'),
                      password=Variable.get('password_db'),
                      database=Variable.get('name_db'))
    else:
        import neural_network as n
        metrics_dict = n.fit_model()

    try:
        table_name = f"model_{city.lower()}"
        schema = 'metrics'

        metric_columns = ['date', 'loss', 'mse', 'r2', 'rmse']
        db.insert(schema=schema,
                  table_name=table_name,
                  columns_list=metric_columns,
                  data=(datetime.now().strftime("%Y-%m-%d"), *metrics_dict.values()))

        print(f"{city} load metrics GOOD!")
    except Exception as e:
        raise ValueError(f"{city} load metrics ERROR!\n{e}")


def load_forecast(city, airflow_mode=True, db=None, **kwargs):
    '''Загружаем прогноз в БД'''

    if airflow_mode:
        from airflow.models import Variable
        ti = kwargs['ti']
        data_str = ti.xcom_pull(task_ids='get_predict')
        print('data_str:', data_str)
        data_dict = json.loads(data_str)

        db = DataBase(host=Variable.get('host_db'),
                      port=Variable.get('port_db'),
                      user=Variable.get('user_db'),
                      password=Variable.get('password_db'),
                      database=Variable.get('name_db'))
    else:
        import neural_network as n
        data_dict = n.get_predict()

    df_forecast = pd.DataFrame(data_dict)
    df_forecast.set_index('date', inplace=True)

    try:
        table_name = f"forecast_{city.lower()}"
        schema = 'predict'
        columns_weather_list = [
            "date",
            "day1", "day2", "day3", "day4", "day5", "day6", "day7", "day8", "day9", "day10",
            "night1", "night2", "night3", "night4", "night5", "night6", "night7", "night8", "night9", "night10"
        ]

        for row in df_forecast.itertuples(index=True, name=None):
            db.insert(schema=schema, table_name=table_name, columns_list=columns_weather_list, data=row)

        print(f"{city} load forecast GOOD!")
    except Exception as e:
        raise ValueError(f"{city} load forecast ERROR!\n{e}")