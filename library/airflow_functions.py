import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
from psycopg2.extras import RealDictCursor
import additional_functions as lib

import asyncio


CITIES_URL = {
    "GisMeteo":
        {"Moscow": 'https://www.gismeteo.ru/weather-moscow-4368/10-days/',
         "Krasnodar": 'https://www.gismeteo.ru/weather-krasnodar-5136/10-days/',
         "Ekaterinburg": 'https://www.gismeteo.ru/weather-yekaterinburg-4517/10-days/'},
    "Yandex":
        {"Moscow": 'https://yandex.ru/weather/moscow?lat=55.755863&lon=37.6177',
         "Krasnodar": 'https://yandex.ru/weather?lat=45.03546906&lon=38.97531128',
         "Ekaterinburg": 'https://yandex.ru/weather?lat=56.8380127&lon=60.59747314'}}


def get_weather_forecast_Yandex(city, type, **kwargs):
    '''Получение данных с сайта Yandex'''

    city_url = CITIES_URL[type][city]

    headers = requests.utils.default_headers()

    headers.update(
        {
            'User-Agent': 'My User Agent 1.0',
        }
    )
    response = requests.get(city_url, headers=headers)
    soup = BeautifulSoup(response.content, 'html.parser')

    temp_max = soup.find_all('div', class_='temp forecast-briefly__temp forecast-briefly__temp_day')
    temp_min = soup.find_all('div', class_='temp forecast-briefly__temp forecast-briefly__temp_night')
    weather = soup.find_all('div', class_='forecast-briefly__condition')

    forecast_data = []

    for i in range(1, 11):
        max_temp = temp_max[i].find('span', class_='temp__value temp__value_with-unit').get_text(strip=True)
        min_temp = temp_min[i].find('span', class_='temp__value temp__value_with-unit').get_text(strip=True)
        weather_today = weather[i].get_text(strip=True)

        forecast_data.append({
            'max_temp': max_temp,
            'min_temp': min_temp,
            'weather': weather_today
        })

    return forecast_data


def get_weather_forecast_GisMeteo(city, type, **kwargs):
    '''Получение данных с сайта GisMeteo'''

    city_url = CITIES_URL[type][city]

    headers = requests.utils.default_headers()

    headers.update(
        {
            'User-Agent': 'My User Agent 1.0',
        }
    )
    response = requests.get(city_url, headers=headers)
    soup = BeautifulSoup(response.content, 'html.parser')

    temp_max = soup.find_all('div', class_='maxt')
    temp_min = soup.find_all('div', class_='mint')
    weather = soup.find_all('div', class_='row-item')

    forecast_data = []

    for i in range(10):
        max_temp = temp_max[i].find('temperature-value')["value"]
        min_temp = temp_min[i].find('temperature-value')["value"]
        weather_today = weather[i]['data-tooltip']

        forecast_data.append({
            'max_temp': max_temp,
            'min_temp': min_temp,
            'weather': weather_today
        })

    return forecast_data


def create_today(city, type, airflow_mode='False', today=datetime.now().strftime('%Y-%m-%d'), **kwargs):
    '''Создание таблицы из одной строки с подгруженными данными'''

    if airflow_mode:
        ti = kwargs['ti']
        forecast_data = ti.xcom_pull(task_ids='get_weather_forecast')

    else:
        if type == "Yandex":
            forecast_data = get_weather_forecast_Yandex(city, type)
        elif type == "GisMeteo":
            forecast_data = get_weather_forecast_GisMeteo(city, type)
        else:
            raise "TypeError"

    max_temps = [data['max_temp'] for data in forecast_data]
    min_temps = [data['min_temp'] for data in forecast_data]
    weather = [data['weather'] for data in forecast_data]

    data = {'date': [today]}

    for i in range(10):
        data[f'day{i + 1}'] = [int(max_temps[i]) if max_temps[i][0] != '−' else int(max_temps[i][1:]) * (-1)]

    for i in range(10):
        data[f'night{i + 1}'] = [int(min_temps[i]) if min_temps[i][0] != '−' else int(min_temps[i][1:]) * (-1)]

    for i in range(10):
        data[f'weather{i + 1}'] = [weather[i] if i < len(weather) else None]

    df = pd.DataFrame(data)
    df.set_index('date', inplace=True)

    return df


def update(city, type, connection=None, airflow_mode='False', **kwargs):
    '''Обновление таблицы по указанному городу и сайту'''

    if airflow_mode:
        from airflow.models import Variable

        ti = kwargs['ti']
        df_new = ti.xcom_pull(task_ids='create_today')

        connection = lib.create_connect(host=Variable.get('host_db'),
                                        port=Variable.get('port_db'),
                                        user=Variable.get('user_db'),
                                        password=Variable.get('password_db'),
                                        database=Variable.get('name_db'))

    try:
        if not airflow_mode:
            df_new = create_today(city, type)

        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            table_name = f"t_{city}_{type}"

            for index, row in df_new.iterrows():
                cursor.execute(f"""
                        INSERT INTO {table_name} (
                                            date,
                                            day1,day2,day3,day4,day5,day6,day7,day8,day9,day10,
                                            night1,night2,night3,night4,night5,night6,night7,night8,night9,night10,
                                            weather1,weather2,weather3,weather4,weather5,weather6,weather7,weather8,weather9,weather10)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                    """, (index, *tuple(row)))
        # Сохранение изменений
        connection.commit()
    except Exception as e:
        connection.rollback()
        raise f"update ERROR!\n{e}"