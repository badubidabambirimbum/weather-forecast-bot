import numpy as np 
import pandas as pd 
import matplotlib.pyplot as plt
import seaborn as sns
from keras.models import Sequential
from keras.layers import LSTM, Dense ,Dropout, Bidirectional, Input, Reshape


def create_model(x_train, 
                 n_forecast, 
                 units, 
                 n_timesteps,
                 n_features=1,
                 optimizer='adam',
                 loss='mean_squared_error',
                 metrics='mae'):
    
    regressor = Sequential()
    
    regressor.add(Input(shape=(x_train.shape[1], n_features)))
    
    regressor.add(Bidirectional(LSTM(units=units, return_sequences=True)))
    regressor.add(Dropout(0.2))
    
    regressor.add(LSTM(units=units, return_sequences=True))
    regressor.add(Dropout(0.2))
    
    regressor.add(LSTM(units=units, return_sequences=True))
    regressor.add(Dropout(0.2))
    
    regressor.add(LSTM(units=units))
    regressor.add(Dropout(0.2))
    
    regressor.add(Dense(units=n_forecast * n_features, activation='linear'))
    regressor.add(Reshape((n_forecast, n_features)))

    regressor.summary()

    regressor.compile(optimizer=optimizer, loss=loss, metrics=[metrics])

    return regressor


def create_train_test_data(dataset, percent_train_data):
    size = len(dataset)
    target_idx = int((size / 100) * percent_train_data)

    train = dataset[:target_idx]
    test = dataset[target_idx:]

    return train, test, target_idx


def create_data(data, n_timesteps, n_forecast, n_features):
    x_data = []
    y_data = []

    for i in range(len(data)-n_timesteps-n_forecast+1):
        x_data.append(data[i:i+n_timesteps])
        y_data.append(data[i+n_timesteps:i+n_timesteps+n_forecast])

    x_data, y_data = np.array(x_data), np.array(y_data)
    x_data = np.reshape(x_data, (x_data.shape[0], x_data.shape[1], n_features))

    return x_data, y_data