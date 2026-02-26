import sys
import os

import numpy as np 
import pandas as pd
import requests

from datetime import datetime
from dateutil.relativedelta import relativedelta
from geopy import Nominatim

from sklearn.preprocessing import MinMaxScaler
from sklearn.pipeline import make_pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import FunctionTransformer

import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import layers
from tensorflow.keras.optimizers.schedules import PolynomialDecay

from core.logger import create_logger, log_class_method


class NeuralNetwork:

    def __init__(self, n_timesteps=15*24, n_forecast=10*24, num_epochs=8, batch_size=32, logger=None):

        if logger is None:
            self.logger, _ = create_logger('log_neural_network')
        else:
            self.logger = logger

        self.target_column = 'temp'  # целевой атрибут
        self.features_columns_start = ["temperature_2m", "dew_point_2m", "relative_humidity_2m", "wind_speed_10m",
                                  "surface_pressure", "snowfall"]  # атрибуты для прогноза
        self.features_columns = ['temp', 'dwpt', 'rhum', 'wspd', 'pres', 'snow']

        self.n_timesteps = n_timesteps  # размер окна
        self.n_features = len(self.features_columns)  # кол-во признаков
        self.n_forecast = n_forecast  # размер прогноза

        self.num_epochs = num_epochs  # кол-во эпох для обучения
        self.batch_size = batch_size

    @log_class_method
    def get_interval_for_forecast(self, mode='fit') -> tuple[datetime, datetime]:
        '''
        Функция для получения интервала дат
        mode может принимать 2 значения: fit и predict
        '''
        if mode == 'fit':
            end_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - relativedelta(days=1)
            start_date = end_date - relativedelta(years=1, months=6)
        elif mode == 'predict':
            end_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - relativedelta(hours=1)
            start_date = end_date + relativedelta(hours=1) - relativedelta(days=int(self.n_timesteps // 24))
        else:
            raise KeyError()

        return start_date, end_date

    @staticmethod
    @log_class_method
    def get_city_coordinates(city_name: str) -> tuple[float, float]:
        '''Получаем координаты города'''
        geolocator = Nominatim(user_agent="weather_data_fetcher")
        location = geolocator.geocode(city_name)
        return location.latitude, location.longitude

    @log_class_method
    def get_dataset(self, city: str, timezone: str, start: datetime, end: datetime) -> pd.DataFrame:
        '''
        Получаем pandas df по прогнозу параметров в определенном городе на определенном интервале времени
        :param city: город
        :param timezone: часовой пояс
        :param start: начало интервала
        :param end: конец интервала
        :return: pd.DataFrame
        '''

        url = "https://archive-api.open-meteo.com/v1/archive"

        latitude, longitude = self.get_city_coordinates(city)

        params = {
            "latitude": latitude,
            "longitude": longitude,
            "start_date": start.strftime("%Y-%m-%d"),
            "end_date": end.strftime("%Y-%m-%d"),
            "hourly": self.features_columns_start,
            "timezone": timezone
        }

        response = requests.get(url, params=params)
        data = response.json()
        df = pd.DataFrame(data["hourly"])

        df = df.rename(columns=dict([(x,y) for x,y in zip(self.features_columns_start, self.features_columns)]))
        df['time'] = pd.to_datetime(df['time'])
        df = df.sort_values(by='time')
        df.set_index('time', inplace=True)

        print(df.head(5))

        return df

    @staticmethod
    @log_class_method
    def scale_snow_column(series: pd.Series) -> pd.Series:
        '''
        :param series: pd.Series с параметром snow
        :return: pd.Series - применяем не к 0 значениям MinMaxScaler
        '''
        snow = series.copy().values.astype(float)

        mask = snow != 0

        if mask.any():
            scaler = MinMaxScaler()
            snow[mask] = scaler.fit_transform(snow[mask].reshape(-1, 1)).flatten()

        return pd.Series(snow, index=series.index)

    @log_class_method
    def create_data(self, X, Y):
        '''
        Формируем входные и выходные данные для модели
        :param X: набор данных для прогноза
        :param Y: набор прогнозируемых параметров
        :param n_timesteps: размер окна
        :param n_forecast: размер прогноза
        :param n_features: кол-во признаков
        :return:
        '''
        print("start create_data")
        x_data = []
        y_data = []

        for i in range(len(X)-self.n_timesteps-self.n_forecast+1):
            x_data.append(X[i:i+self.n_timesteps])
            y_data.append(Y[i+self.n_timesteps:i+self.n_timesteps+self.n_forecast])

        x_data, y_data = np.array(x_data), np.array(y_data)
        print(f"x_data.shape: {x_data.shape}")
        print(f"y_data.shape: {y_data.shape}")

        print(x_data[:10])
        print(y_data[:10])

        return x_data, y_data

    @log_class_method
    def create_model(self, X, Y):
        '''Создание модели'''
        model = keras.Sequential([
            layers.LSTM(32, return_sequences=True, input_shape=(self.n_timesteps, self.n_features)),
            layers.LSTM(64, return_sequences=False),
            layers.Dense(self.n_forecast)
        ])

        num_train_steps = (len(X) // self.batch_size) * self.num_epochs

        lr_scheduler = PolynomialDecay(
            initial_learning_rate=1e-3,
            end_learning_rate=0.0,
            decay_steps=num_train_steps
        )

        model.compile(loss=tf.keras.losses.MeanAbsoluteError(),
                  optimizer=tf.optimizers.Adam(learning_rate=lr_scheduler),
                  metrics=[tf.keras.metrics.RootMeanSquaredError(), tf.keras.metrics.R2Score(),tf.metrics.MeanSquaredError()])

        return model

    @staticmethod
    @log_class_method
    def save_history_fit(history):
        '''Сохранение метрик с последнего шага обучения'''
        metrics = {'loss': 'Loss',
                   'mean_squared_error': 'MSE',
                   'r2_score': 'R2',
                   'root_mean_squared_error': 'RMSE'}

        result = {}

        for metric in metrics:
            # plt.plot(history.history[metric], label=f'Train {metrics[metric]}')
            # plt.title(metrics[metric])
            # plt.xlabel('Epoch')
            # plt.ylabel(metrics[metric])
            # plt.legend()
            # plt.savefig(f"metrics/{datetime.now().strftime("%Y-%m-%d")}/{metric}.png")
            # plt.clf()
            result[metric] = history.history[metric][-1]

        return result

    @log_class_method
    def fit_model(self, city, timezone, airflow_mode=True,  **kwargs):
        '''Обучение модели'''
        start, end = self.get_interval_for_forecast() # Получаем начало и конец интервала для прогноза (1.5 года)

        dataset = self.get_dataset(city, timezone, start, end)

        dataset['snow'] = self.scale_snow_column(dataset['snow'])

        print(dataset.head(5))
        print(dataset.info())

        X_train_data = dataset[self.features_columns].loc[start:end]
        y_train_data = dataset[[self.target_column]].loc[start:end]

        scale_cols = ['temp', 'dwpt', 'rhum', 'wspd', 'pres']
        pass_cols = ['snow']

        # scaling data
        preprocessing_X = ColumnTransformer([
            ('scale', make_pipeline(MinMaxScaler()), scale_cols),
            ('passthrough', FunctionTransformer(lambda x: x), pass_cols)
            ])

        preprocessing_Y = ColumnTransformer([
            ('pipe', make_pipeline(MinMaxScaler()), list(dataset[[self.target_column]].columns))
            ])

        X_train_scal = preprocessing_X.fit_transform(X_train_data)
        y_train_scal = preprocessing_Y.fit_transform(y_train_data).reshape(len(y_train_data))

        X_train, y_train = self.create_data(X_train_scal, y_train_scal)

        print(X_train[:10])
        print(y_train[:10])

        model = self.create_model(X_train, y_train)

        history = model.fit(X_train, y_train, batch_size=self.batch_size, epochs=self.num_epochs)
        result_fit = self.save_history_fit(history)

        os.makedirs('models', exist_ok=True)
        if airflow_mode:
            model.save(f'/app/models/model_{city}.keras')
        else:
            model.save(f'./models/model_{city}.keras')

        return result_fit

    @staticmethod
    @log_class_method
    def get_window_min_max(arr, window_size=24):
        '''Формируем набор минимальных и максимальных значений по дням, так как изначально у нас все по часам'''
        # Разбить на окна и посчитать min и max в каждом
        chunks = [arr[i:i+window_size] for i in range(0, len(arr), window_size)]
        mins = [np.min(chunk) for chunk in chunks]
        maxs = [np.max(chunk) for chunk in chunks]
        return mins, maxs

    @log_class_method
    def get_predict(self, city, timezone, airflow_mode=True, **kwargs) -> dict:
        '''Получаем прогноз'''
        start, end = self.get_interval_for_forecast(mode='predict')

        dataset = self.get_dataset(city, timezone, start, end)

        dataset['snow'] = self.scale_snow_column(dataset['snow'])

        print(dataset.info())

        X_window_data = dataset[self.features_columns].loc[start:end]
        y_window_data = dataset[[self.target_column]].loc[start:end]

        scale_cols = ['temp', 'dwpt', 'rhum', 'wspd', 'pres']
        pass_cols = ['snow']

        # scaling data
        preprocessing_X = ColumnTransformer([
            ('scale', make_pipeline(MinMaxScaler()), scale_cols),
            ('passthrough', FunctionTransformer(lambda x: x), pass_cols)
            ])

        preprocessing_Y = ColumnTransformer([
            ('pipe', make_pipeline(MinMaxScaler()), list(dataset[[self.target_column]].columns))
            ])

        X_window_scal = preprocessing_X.fit_transform(X_window_data)
        y_window_scal = preprocessing_Y.fit_transform(y_window_data).reshape(len(y_window_data))

        print(X_window_scal[:10])
        print(y_window_scal[:10])

        # X, y = create_data(X_window_scal, y_window_scal, N_TIMESTEPS, N_FORECAST, N_FEATURES)

        X = np.array([X_window_scal])

        if airflow_mode:
            model = tf.keras.models.load_model(f'/app/models/model_{city}.keras')
        else:
            model = tf.keras.models.load_model(f'models/model_{city}.keras')

        pred = model.predict(X)
        print(f"pred: {pred}")
        predict = np.reshape(pred, (pred.shape[0], pred.shape[1]))
        # Получаем scaler из пайплайна
        scaler = preprocessing_Y.named_transformers_['pipe'].named_steps['minmaxscaler']
        # Возвращаем исходные значения
        predict = scaler.inverse_transform(predict)
        print(f"predict: {predict}")

        nights, days = self.get_window_min_max(predict[0], window_size=24)

        print(days)
        print(len(days))

        today = datetime.now().strftime('%Y-%m-%d')

        data = {'date': [today]}

        for i in range(10):
            data[f'day{i + 1}'] = int(days[i])

        for i in range(10):
            data[f'night{i + 1}'] = int(nights[i])

        return data