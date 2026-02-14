from typing import List

import pandas as pd
from collections import deque
from datetime import datetime
from telegram_constants import *
from database import DataBase
import functools

def view(city: str, type: str, db: DataBase, schema='prom', key="tail", OrderBy_column='date'):
    '''Просмотр таблицы по городу и сайту'''

    table_name = f"t_{city}_{type}"
    query = f"SELECT * FROM {schema}.{table_name} order by {OrderBy_column};"
    rows = db.execute_query(query)

    # df = pd.read_sql_query(query, connection)
    # df.set_index('date', inplace=True)
    df = pd.DataFrame(rows)
    df.set_index('date', inplace=True)

    if key == "tail":
        return df.tail()
    elif key == "head":
        return df.head()
    elif key == "all":
        return df
    else:
        raise KeyError("key Error!")


def create_forecast(city, dist, period, db):
    '''формирование строки с прогнозом'''

    forecast_Yandex = view(TRANSLATE_CITIES[city], "Yandex", db, key='all').iloc[-1]
    forecast_GisMeteo = view(TRANSLATE_CITIES[city], "GisMeteo", db, key='all').iloc[-1]
    date_forecast = view(TRANSLATE_CITIES[city], "GisMeteo", db, key='all').index[-1]

    future_dates = pd.date_range(start=date_forecast, periods=period)
    forecast_data = ""

    for i in range(1, int(dist) + 1):
        date = future_dates[i - 1]
        if (forecast_Yandex[f'weather{i}'] in WEATHER_YANDEX_SMILE and
                forecast_GisMeteo[f'weather{i}'] in WEATHER_GISMETEO_SMILE):
            if WEATHER_YANDEX_SMILE[forecast_Yandex[f'weather{i}']] == WEATHER_GISMETEO_SMILE[forecast_GisMeteo[f'weather{i}']]:
                forecast_data += (f"\n"
                                  f"✨ {date.strftime('%Y-%m-%d')} ✨\n"
                                  f"<b>Температура</b> ⬇️ <b>{str(forecast_Yandex[f'night{i}'])}°</b> ⬆️ <b>{str(forecast_Yandex[f'day{i}'])}°</b>\n"
                                  f"🔸<b>Yandex</b> и 🔹<b>GisMeteo</b> прогнозируют {WEATHER_GISMETEO_SMILE[forecast_GisMeteo[f'weather{i}']]}\n")
            else:
                forecast_data += (f"\n"
                                  f"✨ {date.strftime('%Y-%m-%d')} ✨\n"
                                  f"<b>Температура</b> ⬇️ <b>{str(forecast_Yandex[f'night{i}'])}°</b> ⬆️ <b>{str(forecast_Yandex[f'day{i}'])}°</b>\n "
                                  f"🔸<b>Yandex</b> прогнозирует {WEATHER_YANDEX_SMILE[forecast_Yandex[f'weather{i}']]}\n "
                                  f"🔹<b>GisMeteo</b> прогнозирует {WEATHER_GISMETEO_SMILE[forecast_GisMeteo[f'weather{i}']]}\n")
        else:
            forecast_data += (f"\n"
                              f"✨ {date.strftime('%Y-%m-%d')} ✨\n"
                              f"<b>Температура</b> ⬇️ <b>{str(forecast_Yandex[f'night{i}'])}°</b> ⬆️ <b>{str(forecast_Yandex[f'day{i}'])}°</b>\n "
                              f"🔸<b>Yandex</b> прогнозирует {forecast_Yandex[f'weather{i}']}\n "
                              f"🔹<b>GisMeteo</b> прогнозирует {forecast_GisMeteo[f'weather{i}']}\n")

    return forecast_data

def backup(db, tables=('prom.t_moscow_gismeteo',
                       'prom.t_moscow_yandex',
                       'prom.t_ekaterinburg_gismeteo',
                       'prom.t_ekaterinburg_yandex',
                       'prom.t_krasnodar_gismeteo',
                       'prom.t_krasnodar_yandex',
                       'prom.all_users',
                       'prom.subscribers',
                       'metrics.model_moscow',
                       'metrics.model_krasnodar',
                       'metrics.model_ekaterinburg',
                       'predict.forecast_moscow',
                       'predict.forecast_krasnodar',
                       'predict.forecast_ekaterinburg')):
    try:
        for table in tables:
            query = f"SELECT * FROM {table} order by date;"  # SQL-запрос
            rows = db.execute_query(query)
            df = pd.DataFrame(rows)
            df.to_csv(f'backup/{table.split(".")[0]}/{table.split(".")[1]}.csv', index=False)
        print('BACKUP good!')
    except Exception as e:
        raise ValueError(e)

def tail(filepath, n=100) -> List:
    '''
    Функция для получения n последних строк файла
    :param filepath: путь к файлу
    :param n: кол-во строк
    :return:
    '''
    with open(filepath, "r", encoding="utf-8") as f:
        return list(deque(f, n))


def log_function(logger):
    """Декоратор для логирования выполнения функций"""
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            logger.info(f"_started '{func.__name__}'")
            try:
                result = await func(*args, **kwargs)
                logger.info(f"_stopped '{func.__name__}'")
                return result
            except Exception as e:
                logger.error(f"_failed '{func.__name__}' {e}")
        return wrapper
    return decorator