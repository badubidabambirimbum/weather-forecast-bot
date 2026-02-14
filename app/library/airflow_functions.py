import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import re

from app.library.database import DataBase

CITIES_URL = {
    "GisMeteo":
        {"Moscow": 'https://www.gismeteo.ru/weather-moscow-4368/10-days/',
         "Krasnodar": 'https://www.gismeteo.ru/weather-krasnodar-5136/10-days/',
         "Ekaterinburg": 'https://www.gismeteo.ru/weather-yekaterinburg-4517/10-days/'},
    "Yandex":
        {"Moscow": 'https://yandex.ru/weather/moscow?lat=55.755863&lon=37.6177',
         "Krasnodar": 'https://yandex.ru/weather?lat=45.03546906&lon=38.97531128',
         "Ekaterinburg": 'https://yandex.ru/weather?lat=56.8380127&lon=60.59747314'}}


# def get_weather_forecast_Yandex(city, type='Yandex', **kwargs):
#     '''Получение данных с сайта Yandex'''
#
#     city_url = CITIES_URL[type][city]
#
#     headers = requests.utils.default_headers()
#
#     headers.update(
#         {
#             'User-Agent': 'My User Agent 1.0',
#         }
#     )
#     response = requests.get(city_url, headers=headers)
#     soup = BeautifulSoup(response.content, 'html.parser')
#
#     temp_max = soup.find_all('div', class_='temp forecast-briefly__temp forecast-briefly__temp_day')
#     temp_min = soup.find_all('div', class_='temp forecast-briefly__temp forecast-briefly__temp_night')
#     weather = soup.find_all('div', class_='forecast-briefly__condition')
#
#     forecast_data = []
#
#     for i in range(1, 11):
#         max_temp = temp_max[i].find('span', class_='temp__value temp__value_with-unit').get_text(strip=True)
#         min_temp = temp_min[i].find('span', class_='temp__value temp__value_with-unit').get_text(strip=True)
#         weather_today = weather[i].get_text(strip=True)
#
#         forecast_data.append({
#             'max_temp': max_temp,
#             'min_temp': min_temp,
#             'weather': weather_today
#         })
#
#     return forecast_data


def get_weather_forecast_Yandex(city, type='Yandex', **kwargs):
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

    temps = soup.find_all("span", class_="AppShortForecastDay_temperature__DV3oM")

    temperature_values = [temp.get_text(strip=True).replace('°', '') for temp in temps]

    max_temp = [t for idx, t in enumerate(temperature_values) if idx % 2 == 0]
    min_temp = [t for idx, t in enumerate(temperature_values) if idx % 2 != 0]
    weather_today = []

    icons = soup.find_all("div", class_=lambda x: x and "AppShortForecastDay_icon" in x)

    icon_values = []

    for icon in icons:
        style = icon.get("style", "")
        match = re.search(r"--icon:(\d+)", style)
        if match:
            icon_values.append(int(match.group(1)))

    icons_dict = {
        '0': 'Небольшой дождь',
        '1': 'Дождь',
        '2': 'Ливень',
        '12': 'Небольшой дождь',
        '13': 'Дождь',
        '21': 'Дождь с грозой',
        '23': 'Облачно с прояснениями',
        '25': 'Пасмурно',
        '26': 'Ясно'
    }

    for val in icon_values:
        weather_today.append(icons_dict.get(str(val), icons_dict['23']))

    forecast_data = {
        'max_temp': max_temp,
        'min_temp': min_temp,
        'weather': weather_today
    }

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

    max_temp = [temp_max[i].find('temperature-value')["value"] for i in range(10)]
    min_temp = [temp_min[i].find('temperature-value')["value"] for i in range(10)]
    weather_today = [weather[i]['data-tooltip'] for i in range(10)]

    forecast_data = {
        'max_temp': max_temp,
        'min_temp': min_temp,
        'weather': weather_today
    }

    return forecast_data

# def get_weather_forecast_GisMeteo(city, type, **kwargs):
#     '''Получение данных с сайта GisMeteo'''
#
#     city_url = CITIES_URL[type][city]
#
#     headers = requests.utils.default_headers()
#
#     headers.update(
#         {
#             'User-Agent': 'My User Agent 1.0',
#         }
#     )
#     response = requests.get(city_url, headers=headers)
#     soup = BeautifulSoup(response.content, 'html.parser')
#
#     temp_max = soup.find_all('div', class_='maxt')
#     temp_min = soup.find_all('div', class_='mint')
#     weather = soup.find_all('div', class_='row-item')
#
#     forecast_data = []
#
#     for i in range(10):
#         max_temp = temp_max[i].find('temperature-value')["value"]
#         min_temp = temp_min[i].find('temperature-value')["value"]
#         weather_today = weather[i]['data-tooltip']
#
#         forecast_data.append({
#             'max_temp': max_temp,
#             'min_temp': min_temp,
#             'weather': weather_today
#         })
#
#     return forecast_data


def create_today(city, type, airflow_mode=False, today=datetime.now().strftime('%Y-%m-%d'), **kwargs):
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
            raise TypeError("TypeError")

    max_temps = forecast_data['max_temp']
    min_temps = forecast_data['min_temp']
    weather = forecast_data['weather']

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

def create_new_day(city:str, type:str, year:int, month:int, day:int, list_days:list, list_nights:list, list_weathers:list, db, schema='prom'):
    '''Ручное добавление одного дня по указанному городу и сайту'''

    date = datetime(year, month, day)
    date = date.strftime('%Y-%m-%d')

    data = {'date': [date]}

    for i in range(10):
        data[f'day{i + 1}'] = int(list_days[i])

    for i in range(10):
        data[f'night{i + 1}'] = int(list_nights[i])

    for i in range(10):
        data[f'weather{i + 1}'] = list_weathers[i]

    df = pd.DataFrame(data)
    df.set_index('date', inplace=True)

    table_name = f"t_{city}_{type}"
    columns_weather_list = [
        "date",
        "day1", "day2", "day3", "day4", "day5", "day6", "day7", "day8", "day9", "day10",
        "night1", "night2", "night3", "night4", "night5", "night6", "night7", "night8", "night9", "night10",
        "weather1", "weather2", "weather3", "weather4", "weather5", "weather6", "weather7", "weather8", "weather9", "weather10"
    ]

    for row in df.itertuples(index=True, name=None):
        db.insert(schema=schema, table_name=table_name, columns_list=columns_weather_list, data=row)


def update(city, type, db=None, airflow_mode=False, **kwargs):
    '''Обновление таблицы по указанному городу и сайту'''

    if airflow_mode:
        from airflow.models import Variable

        ti = kwargs['ti']
        df_new = ti.xcom_pull(task_ids='create_DF')

        db = DataBase(host=Variable.get('host_db'),
                      port=Variable.get('port_db'),
                      user=Variable.get('user_db'),
                      password=Variable.get('password_db'),
                      database=Variable.get('name_db'))

    if not airflow_mode:
        df_new = create_today(city, type)

    table_name = f"t_{city}_{type}"
    columns_weather_list = [
        "date",
        "day1", "day2", "day3", "day4", "day5", "day6", "day7", "day8", "day9", "day10",
        "night1", "night2", "night3", "night4", "night5", "night6", "night7", "night8", "night9", "night10",
        "weather1", "weather2", "weather3", "weather4", "weather5", "weather6", "weather7", "weather8", "weather9",
        "weather10"
    ]

    for row in df_new.itertuples(index=True, name=None):
        db.insert(schema='prom', table_name=table_name, columns_list=columns_weather_list, data=row)

    
