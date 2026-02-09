import pandas as pd
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor
from library.telegram_constants import *

def create_connect(host, port, user, password, database):
    '''Подключение к базе данных'''

    connection = None
    try:
        connection = psycopg2.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
        )
        connect_text = f"✅ Подключение установлено!"
    except Exception as ex:
        connect_text = f"❌ Не удалось установить подключение {ex}!"

    return connection, connect_text

def view(city, type, connection, schema='prom', key="tail", OrderBy_column='date'):
    '''Просмотр таблицы по городу и сайту'''

    table_name = f"t_{city}_{type}"

    query = f"SELECT * FROM {schema}.{table_name} order by {OrderBy_column};"  # SQL-запрос

    df = pd.read_sql_query(query, connection)
    df.set_index('date', inplace=True)

    if key == "tail":
        return df.tail()
    elif key == "head":
        return df.head()
    elif key == "all":
        return df
    else:
        raise KeyError("key Error!")


def create_forecast(city, dist, period, connection):
    '''формирование строки с прогнозом'''

    forecast_Yandex = view(TRANSLATE_CITIES[city], "Yandex", connection, key='all').iloc[-1]
    forecast_GisMeteo = view(TRANSLATE_CITIES[city], "GisMeteo", connection, key='all').iloc[-1]
    date_forecast = view(TRANSLATE_CITIES[city], "GisMeteo", connection, key='all').index[-1]

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

def backup(connection, tables=('prom.t_moscow_gismeteo',
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
            query = f"SELECT * FROM {table} orderby date;"  # SQL-запрос

            df = pd.read_sql_query(query, connection)
            df.to_csv(f'backup/{table.split(".")[0]}/{table.split(".")[1]}.csv', index=False)
        print('BACKUP good!')
    except Exception as e:
        raise ValueError(e)
