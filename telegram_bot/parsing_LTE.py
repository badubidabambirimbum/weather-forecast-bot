import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import pymysql
from auth_data import *
from telegram_constants import *


cities_url = {
                "GisMeteo":
                        {"Moscow": 'https://www.gismeteo.ru/weather-moscow-4368/10-days/',
                        "Krasnodar": 'https://www.gismeteo.ru/weather-krasnodar-5136/10-days/',
                        "Ekaterinburg": 'https://www.gismeteo.ru/weather-yekaterinburg-4517/10-days/'},
                "Yandex":
                        {"Moscow": 'https://yandex.ru/weather/moscow?lat=55.755863&lon=37.6177',
                        "Krasnodar": 'https://yandex.ru/weather?lat=45.03546906&lon=38.97531128',
                        "Ekaterinburg": 'https://yandex.ru/weather?lat=56.8380127&lon=60.59747314'}}


def get_weather_forecast_Yandex(city, type):
    '''Получение данных с сайта Yandex'''

    city_url = cities_url[type][city]

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


def get_weather_forecast_GisMeteo(city, type):
    '''Получение данных с сайта GisMeteo'''

    city_url = cities_url[type][city]

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


def create_today(city, type, today=datetime.now().strftime('%Y-%m-%d')):
    '''Создание таблицы из одной строки с подгруженными данными'''

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


def update(city, type, connection):
    '''Обновление таблицы по указанному городу и сайту'''
    
    try:
        df_new = create_today(city, type)

        with connection.cursor() as cursor:
            table_name = f"{city}_{type}"

            for index, row in df_new.iterrows():
                cursor.execute(f"""
                        INSERT INTO {table_name} (
                                            date,
                                            day1,day2,day3,day4,day5,day6,day7,day8,day9,day10,
                                            night1,night2,night3,night4,night5,night6,night7,night8,night9,night10,
                                            weather1,weather2,weather3,weather4,weather5,weather6,weather7,weather8,weather9,weather10)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                    """, (index,*tuple(row)))
        # Сохранение изменений
        connection.commit()

        print(f"{city} {type} GOOD!")
    except:
        raise ValueError(f"{city} {type} ERROR!")


def create_new_day(city, type, year, month, day, list_days, list_nights, list_weathers, connection):
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

        with connection.cursor() as cursor:
            table_name = f"{city}_{type}"

            for index, row in df_new.iterrows():
                cursor.execute(f"""
                        INSERT INTO {table_name} (
                                            date,
                                            day1,day2,day3,day4,day5,day6,day7,day8,day9,day10,
                                            night1,night2,night3,night4,night5,night6,night7,night8,night9,night10,
                                            weather1,weather2,weather3,weather4,weather5,weather6,weather7,weather8,weather9,weather10)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                    """, (index,*tuple(row)))
        # Сохранение изменений
        connection.commit()

        print(f"{city} {type} GOOD!")
    except:
        raise ValueError(f"{city} {type} ERROR!")


def backup(connection):
    '''Создание backup-а'''
    
    try:
        with connection.cursor() as cursor:

            for city_key in SET_CITIES:
                for type_key in SET_TYPES:
                    city_key = TRANSLATE_CITIES[city_key]

                    table_name = f"{city_key}_{type_key}"
                    table_name_backup = f"backup_{city_key}_{type_key}"

                    cursor.execute(f"""
                        DROP TABLE IF EXISTS {table_name_backup};
                    """)

                    cursor.execute(f"""
                        CREATE TABLE {table_name_backup} AS SELECT * FROM {table_name};
                    """)

                    # Сохранение изменений
                    connection.commit()

        print("BACKUP GOOD!")

    except:
        raise ValueError("BACKUP ERROR!")


def view(city, type, connection, key="tail"):
    '''Просмотр таблицы по городу и сайту'''

    table_name = f"{city}_{type}"

    query = f"SELECT * FROM {table_name};"  # SQL-запрос

    with connection.cursor() as cursor:
        cursor.execute(query)
        data = cursor.fetchall() 

    df = pd.DataFrame(data)
    df.set_index('date', inplace=True)
    
    if key == "tail":
        return df.tail()
    elif key == "head":
        return df.head()
    elif key == "all":
        return df
    else:
        raise KeyError("key Error!")