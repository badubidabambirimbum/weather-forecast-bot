import json
import argparse
import time
from utils.neural_network import NeuralNetwork

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--city', action='store', dest='city', type=str)
    parser.add_argument('--timezone', action='store', dest='timezone', type=str)
    parser.add_argument('--airflow_mode', action='store_true', dest='airflow_mode')
    parser.add_argument('--n_timesteps', action='store', dest='n_timesteps', type=int)
    parser.add_argument('--n_forecast', action='store', dest='n_forecast', type=int)

    args = parser.parse_args()

    return args

def main():
    args = get_args()
    print(args)
    model = NeuralNetwork(n_timesteps=args.n_timesteps, n_forecast=args.n_forecast)
    predict = model.get_predict(city=args.city, timezone=args.timezone, airflow_mode=args.airflow_mode)
    return predict

if __name__ == '__main__':
    forecast = main()
    print("===RESULT===")
    time.sleep(5)
    print(json.dumps(forecast))
