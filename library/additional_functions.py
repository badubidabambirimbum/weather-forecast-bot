from airflow_functions import create_today
import pandas as pd
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor
from telegram_constants import *


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


def create_new_day(city:str, type:str, year:int, month:int, day:int, list_days:list, list_nights:list, list_weathers:list, connection, schema='prom'):
    '''Ручное добавление одного дня по указанному городу и сайту'''

    date = datetime(year, month, day)
    date = date.strftime('%Y-%m-%d')

    data = {'date': [date]}

    for i in range(10):
        data[f'day{i + 1}'] = int(list_days[i])
        data[f'night{i + 1}'] = int(list_nights[i])
        data[f'weather{i + 1}'] = list_weathers[i]

    df = pd.DataFrame(data)
    df.set_index('date', inplace=True)

    try:
        df_new = create_today(city, type)

        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            table_name = f"t_{city}_{type}"

            for index, row in df_new.iterrows():
                cursor.execute(f"""
                        INSERT INTO {schema}.{table_name} (
                                            date,
                                            day1,day2,day3,day4,day5,day6,day7,day8,day9,day10,
                                            night1,night2,night3,night4,night5,night6,night7,night8,night9,night10,
                                            weather1,weather2,weather3,weather4,weather5,weather6,weather7,weather8,weather9,weather10)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                    """, (index, *tuple(row)))
        # Сохранение изменений
        connection.commit()

        print(f"{city} {type} GOOD!")
    except Exception as e:
        raise ValueError(f"{city} {type} ERROR!\n{e}")


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