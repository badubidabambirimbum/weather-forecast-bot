import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import os

current_dir = os.path.dirname(__file__)
path_to_data = os.path.join(current_dir, "..", "data")

class table:
    """Класс для парсинга данных с GisMeteo и Yandex"""

    def __init__(self):
        self.cities_url = {"GisMeteo":
                               {"Moscow": 'https://www.gismeteo.ru/weather-moscow-4368/10-days/',
                                "Krasnodar": 'https://www.gismeteo.ru/weather-krasnodar-5136/10-days/',
                                "Ekaterinburg": 'https://www.gismeteo.ru/weather-yekaterinburg-4517/10-days/'},
                           "Yandex":
                               {"Moscow": 'https://yandex.ru/weather/moscow?lat=55.755863&lon=37.6177',
                                "Krasnodar": 'https://yandex.ru/weather?lat=45.03546906&lon=38.97531128',
                                "Ekaterinburg": 'https://yandex.ru/weather?lat=56.8380127&lon=60.59747314'}}

        self.datasets = {
            "Moscow": {
                "GisMeteo": pd.read_csv(os.path.join(path_to_data, 'Moscow_GisMeteo_10.csv'), sep=',',
                                  index_col='date'), 
                "Yandex": pd.read_csv(os.path.join(path_to_data, 'Moscow_Yandex_10.csv'), sep=',',
                                  index_col='date')},
            "Krasnodar": {
                "GisMeteo": pd.read_csv(os.path.join(path_to_data, 'Krasnodar_GisMeteo_10.csv'), sep=',',
                                        index_col='date'),
                "Yandex": pd.read_csv(os.path.join(path_to_data, 'Krasnodar_Yandex_10.csv'), sep=',',
                                      index_col='date')},
            "Ekaterinburg": {
                "GisMeteo": pd.read_csv(os.path.join(path_to_data, 'Ekaterinburg_GisMeteo_10.csv'), sep=',',
                                        index_col='date'),
                "Yandex": pd.read_csv(os.path.join(path_to_data, 'Ekaterinburg_Yandex_10.csv'), sep=',',
                                      index_col='date')}}


    # Получение данных с сайта Yandex
    def get_weather_forecast_Yandex(self, city, type):
        city_url = self.cities_url[type][city]

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


    # Получение данных с старой версии сайта GisMeteo
    def get_weather_forecast_GisMeteo(self, city, type):
        city_url = self.cities_url[type][city]

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
            max_temp = temp_max[i].find('span', class_='unit unit_temperature_c').get_text(strip=True)
            min_temp = temp_min[i].find('span', class_='unit unit_temperature_c').get_text(strip=True)
            weather_today = weather[i].find('div', class_='weather-icon tooltip').get('data-text', '')

            forecast_data.append({
                'max_temp': max_temp,
                'min_temp': min_temp,
                'weather': weather_today
            })

        return forecast_data


    # Получение данных с новой версии сайта GisMeteo
    def get_weather_forecast_GisMeteo_V2(self, city, type):
        city_url = self.cities_url[type][city]

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


    # Создание таблицы из одной строки с подгруженными данными
    def create_today(self, city, type, today=datetime.now().strftime('%Y-%m-%d')):
        if type == "Yandex":
            forecast_data = self.get_weather_forecast_Yandex(city, type)
        elif type == "GisMeteo":
            # forecast_data = self.get_weather_forecast_GisMeteo(city, type)
            forecast_data = self.get_weather_forecast_GisMeteo_V2(city, type='GisMeteo')
        else:
            raise "TypeError"

        max_temps = [data['max_temp'] for data in forecast_data]
        min_temps = [data['min_temp'] for data in forecast_data]
        weather = [data['weather'] for data in forecast_data]

        data = {'date': [today]}

        for i in range(10):
            data[f'day{i + 1}'] = [int(max_temps[i]) if max_temps[i][0] != '-' else int(max_temps[i]) * (-1)]

        for i in range(10):
            data[f'night{i + 1}'] = [int(min_temps[i]) if min_temps[i][0] != '-' else int(min_temps[i]) * (-1)]

        for i in range(10):
            data[f'weather{i + 1}'] = [weather[i] if i < len(weather) else None]

        df = pd.DataFrame(data)
        df.set_index('date', inplace=True)

        return df


    # Обновление таблицы по указанному городу и сайту
    def update(self, city, type):
        try:
            df_new = self.create_today(city, type)

            self.datasets[city][type] = pd.concat([self.datasets[city][type], df_new])

            self.datasets[city][type].to_csv(os.path.join(path_to_data, f'{city}_{type}_10.csv'))
            print(f"{city} {type} GOOD!")
        except:
            raise ValueError(f"{city} {type} ERROR!")


    # Ручное добавление одного дня по указанному городу и сайту
    def create_new_day(self, city, type, year, month, day, list_days, list_nights, list_weathers):
        date = datetime(year, month, day)
        date = date.strftime('%Y-%m-%d')

        data = {'date': [date]}

        for i in range(10):
            data[f'day{i + 1}'] = int(list_days[i])
            data[f'night{i + 1}'] = int(list_nights[i])
            data[f'weather{i + 1}'] = list_weathers[i]

        df = pd.DataFrame(data)
        df.set_index('date', inplace=True)

        self.datasets[city][type] = pd.concat([self.datasets[city][type], df]).sort_index()

        self.datasets[city][type].to_csv(os.path.join(path_to_data, f'{city}_{type}_10.csv'))


    # Создание backup-а
    def backup(self):
        try:
            csv_folder = os.path.join(current_dir, "..", "backup")
            for city in self.datasets:
                for type in self.datasets[city]:
                    file_name_csv = f'{city}_{type}_10.csv'
                    file_path_csv = os.path.join(csv_folder, file_name_csv)

                    self.datasets[city][type].to_csv(file_path_csv)
            print("BACKUP GOOD!")
        except:
            raise ValueError("BACKUP ERROR!")


    # Просмотр таблицы по городу и сайту
    def view(self, city, type, key="tail"):
        if key == "tail":
            return self.datasets[city][type].tail()
        elif key == "head":
            return self.datasets[city][type].head()
        elif key == "all":
            return self.datasets[city][type]
        else:
            raise KeyError("key Error!")