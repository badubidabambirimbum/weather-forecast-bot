import json
import argparse
import time
import utils.neural_network as n


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--city', action='store', dest='city', type=str)
    parser.add_argument('--timezone', action='store', dest='timezone', type=str)
    parser.add_argument('--airflow_mode', action='store_true', dest='airflow_mode')

    args = parser.parse_args()

    return args

def main():
    args = get_args()
    print(args)
    result_fit = n.fit_model(city=args.city, timezone=args.timezone, airflow_mode=args.airflow_mode)
    return result_fit

if __name__ == '__main__':
    metrics = main()
    print("===RESULT===")
    time.sleep(5)
    print(json.dumps(metrics))
