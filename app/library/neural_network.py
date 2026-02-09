import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import layers
from tensorflow.keras.optimizers.schedules import PolynomialDecay

from sklearn.preprocessing import MinMaxScaler  
from sklearn.pipeline import make_pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import FunctionTransformer

import matplotlib.pyplot as plt
import numpy as np 
import pandas as pd 

from meteostat import Point, Hourly
from datetime import datetime
from geopy.geocoders import Nominatim
from dateutil.relativedelta import relativedelta

from psycopg2.extras import RealDictCursor
import library.additional_functions as lib


CITY = 'Moscow'                                                     # Город для прогноза

TARGET_COLUMN = 'temp'                                              # целевой атрибут
FEATURES_COLUMNS = ['temp', 'dwpt', 'rhum', 'wspd', 'pres', 'snow'] # атрибуты для прогноза

N_TIMESTEPS = 15 * 24                                               # размер окна
N_FEATURES = len(FEATURES_COLUMNS)                                  # кол-во признаков
N_FORECAST = 10 * 24                                                # размер прогноза

NUM_EPOCHS = 8                                                      # кол-во эпох для обучения
BATCH_SIZE = 32                                                     # размер пакета для обучения


def get_interval_for_forecast(mode='fit'):
    '''
    Функция для получения интервала дат
    mode может принимать 2 значения: fit и predict
    '''
    if mode == 'fit':
        end_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - relativedelta(days=1)
        start_date = end_date - relativedelta(years=1, months=6)
    elif mode == 'predict':
        end_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - relativedelta(hours=1)
        start_date = end_date + relativedelta(hours=1) - relativedelta(days=15)
    else:
        raise KeyError()
    return start_date, end_date


def get_city_coordinates(city_name):
    geolocator = Nominatim(user_agent="weather_data_fetcher")
    location = geolocator.geocode(city_name)
    return location.latitude, location.longitude


def get_dataset(city, start, end):
    latitude, longitude = get_city_coordinates(city)
    location = Point(latitude, longitude)
    df = Hourly(location, start, end)
    df = df.fetch()
    df = df.reset_index()
    df = df.sort_values(by='time')
    df['snow'] = df['snow'].fillna(0)
    df.set_index('time', inplace=True) 
    return df


def scale_snow_column(series):
    snow = series.copy().values.astype(float)
    mask = snow != 0
    if mask.any():
        scaler = MinMaxScaler()
        snow[mask] = scaler.fit_transform(snow[mask].reshape(-1, 1)).flatten()
    return pd.Series(snow)


def create_data(X, Y, n_timesteps, n_forecast, n_features):
    x_data = []
    y_data = []

    for i in range(len(X)-n_timesteps-n_forecast+1):
        x_data.append(X[i:i+n_timesteps])
        y_data.append(Y[i+n_timesteps:i+n_timesteps+n_forecast])

    x_data, y_data = np.array(x_data), np.array(y_data)
    print(f"x_data.shape: {x_data.shape}")
    print(f"y_data.shape: {y_data.shape}")

    return x_data, y_data


def create_model(X, Y, n_timesteps, n_forecast, n_features):
    model = keras.Sequential([
        layers.LSTM(32, return_sequences=True, input_shape=(n_timesteps, n_features)),
        layers.LSTM(64, return_sequences=False),
        layers.Dense(n_forecast)
    ])

    num_train_steps = (len(X) // BATCH_SIZE) * NUM_EPOCHS

    lr_scheduler = PolynomialDecay(
        initial_learning_rate=1e-3,
        end_learning_rate=0.0,
        decay_steps=num_train_steps
    )

    model.compile(loss=tf.keras.losses.MeanAbsoluteError(),
              optimizer=tf.optimizers.Adam(learning_rate=lr_scheduler),
              metrics=[tf.keras.metrics.RootMeanSquaredError(), tf.keras.metrics.R2Score(),tf.metrics.MeanSquaredError()])
    
    return model


def save_history_fit(history):
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


def fit_model(city=CITY, **kwargs):
    start, end = get_interval_for_forecast() # Получаем начало и конец интервала для прогноза (1.5 года)

    dataset = get_dataset(city, start, end)

    dataset['snow'] = scale_snow_column(dataset['snow'])

    X_train_data = dataset[FEATURES_COLUMNS].loc[start:end]
    y_train_data = dataset[[TARGET_COLUMN]].loc[start:end]

    scale_cols = ['temp', 'dwpt', 'rhum', 'wspd', 'pres']
    pass_cols = ['snow'] 

    # scaling data
    preprocessing_X = ColumnTransformer([
        ('scale', make_pipeline(MinMaxScaler()), scale_cols),
        ('passthrough', FunctionTransformer(lambda x: x), pass_cols)
        ])

    preprocessing_Y = ColumnTransformer([
        ('pipe', make_pipeline(MinMaxScaler()), list(dataset[[TARGET_COLUMN]].columns))
        ])

    X_train_scal = preprocessing_X.fit_transform(X_train_data)
    y_train_scal = preprocessing_Y.fit_transform(y_train_data).reshape(len(y_train_data))

    X_train, y_train = create_data(X_train_scal, y_train_scal, N_TIMESTEPS, N_FORECAST, N_FEATURES)

    model = create_model(X_train, y_train, N_TIMESTEPS, N_FORECAST, N_FEATURES)

    history = model.fit(X_train, y_train, batch_size=BATCH_SIZE, epochs=NUM_EPOCHS)
    result_fit = save_history_fit(history)
    
    model.save(f'models/model_{city}.keras')

    return result_fit


def load_metrics(city=CITY, airflow_mode=True, connection=None, **kwargs):

    if airflow_mode:
        from airflow.models import Variable
        ti = kwargs['ti']
        metrics = ti.xcom_pull(task_ids='fit_model')

        connection, _ = lib.create_connect(host=Variable.get('host_db'),
                                           port=Variable.get('port_db'),
                                           user=Variable.get('user_db'),
                                           password=Variable.get('password_db'),
                                           database=Variable.get('name_db'))
    else:
        metrics = fit_model()

    try:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            table_name = f"model_{city.lower()}"
            schema = 'metrics'

            cursor.execute(f"""
                    INSERT INTO {schema}.{table_name} (
                                        date,
                                        loss, mse, r2, rmse)
                    VALUES (%s, %s, %s, %s, %s);
                """, (datetime.now().strftime("%Y-%m-%d"), *tuple(metrics.values())))
        # Сохранение изменений
        connection.commit()

        print(f"{city} load metrics GOOD!")
    except Exception as e:
        raise ValueError(f"{city} load metrics ERROR!\n{e}")


def get_window_min_max(arr, window_size=24):
    # Разбить на окна и посчитать min и max в каждом
    chunks = [arr[i:i+window_size] for i in range(0, len(arr), window_size)]
    mins = [np.min(chunk) for chunk in chunks]
    maxs = [np.max(chunk) for chunk in chunks]
    return mins, maxs


def get_predict(city=CITY, **kwargs):
    start, end = get_interval_for_forecast(mode='predict')

    dataset = get_dataset(city, start, end)

    dataset['snow'] = scale_snow_column(dataset['snow'])

    X_window_data = dataset[FEATURES_COLUMNS].loc[start:end]
    y_window_data = dataset[[TARGET_COLUMN]].loc[start:end]

    scale_cols = ['temp', 'dwpt', 'rhum', 'wspd', 'pres']
    pass_cols = ['snow']

    # scaling data
    preprocessing_X = ColumnTransformer([
        ('scale', make_pipeline(MinMaxScaler()), scale_cols),
        ('passthrough', FunctionTransformer(lambda x: x), pass_cols)
        ])

    preprocessing_Y = ColumnTransformer([
        ('pipe', make_pipeline(MinMaxScaler()), list(dataset[[TARGET_COLUMN]].columns))
        ])

    X_window_scal = preprocessing_X.fit_transform(X_window_data)
    y_window_scal = preprocessing_Y.fit_transform(y_window_data).reshape(len(y_window_data))

    X, y = create_data(X_window_scal, y_window_scal, N_TIMESTEPS, N_FORECAST, N_FEATURES)

    model = tf.keras.models.load_model(f'models/model_{city}.keras')

    pred = model.predict(X)
    predict = np.reshape(pred, (pred.shape[0], pred.shape[1]))
    # Получаем scaler из пайплайна
    scaler = preprocessing_Y.named_transformers_['pipe'].named_steps['minmaxscaler']
    # Возвращаем исходные значения
    predict = scaler.inverse_transform(predict)

    nights, days = get_window_min_max(predict, window_size=24)

    today = datetime.now().strftime('%Y-%m-%d')

    data = {'date': [today]}

    for i in range(10):
        data[f'day{i + 1}'] = int(days[i])

    for i in range(10):
        data[f'night{i + 1}'] = int(nights[i])

    df = pd.DataFrame(data)
    df.set_index('date', inplace=True)

    return df


def load_forecast(city=CITY, airflow_mode=True, connection=None, **kwargs):

    if airflow_mode:
        from airflow.models import Variable
        ti = kwargs['ti']
        df_forecast = ti.xcom_pull(task_ids='get_predict')

        connection, _ = lib.create_connect(host=Variable.get('host_db'),
                                           port=Variable.get('port_db'),
                                           user=Variable.get('user_db'),
                                           password=Variable.get('password_db'),
                                           database=Variable.get('name_db'))
    else:
        df_forecast = get_predict()

    try:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            table_name = f"forecast_{city.lower()}"
            schema = 'predict'

            for index, row in df_forecast.iterrows():
                cursor.execute(f"""
                        INSERT INTO {schema}.{table_name} (
                                            date,
                                            day1,day2,day3,day4,day5,day6,day7,day8,day9,day10,
                                            night1,night2,night3,night4,night5,night6,night7,night8,night9,night10)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                    """, (index, *tuple(row)))
        # Сохранение изменений
        connection.commit()

        print(f"{city} load forecast GOOD!")
    except Exception as e:
        raise ValueError(f"{city} load forecast ERROR!\n{e}")