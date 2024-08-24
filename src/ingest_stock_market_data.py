import requests
import json
import csv
from datetime import date, datetime
import os
import pandas as pd

current_date = date.today().strftime('%Y-%m-%d')
current_time = datetime.now().strftime("%H")
if not os.path.exists("./data"): os.makedirs("./data")
json_file = f"./data/stock_market_data_{current_date}_{current_time}.json"
csv_file = f"./data/stock_market_{current_date}_{current_time}.csv"
ticker_file = f"./data/tickers_{current_date}.json"

#get stock market api key
with open('./config/credentials.json') as f:
    credentials = json.load(f)
api_key = credentials['stock_market_key']
api_url = credentials['url']

#load scraped tickers
def load_tickers_from_file(filename=ticker_file):
    with open(filename, 'r') as f:
        return json.load(f)

def get_data(ticker) -> dict:
    r = requests.get(f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/2023-01-09/2023-01-09?apiKey={api_key}")
    if not r.ok:
        raise Exception(f"Couldn't retrieve API data for ticker {ticker}")
    data = r.json()
    flattened_data = flatten_json(data)
    print(f"Retrieving data for {ticker}...")
    return flattened_data


#flatten nested json returned by api
def flatten_json(json_obj, parent_key='', sep='_') -> dict:
    items=[]
    if isinstance(json_obj, dict):
        for key, value in json_obj.items():
            new_key = f"{parent_key}{sep}{key}" if parent_key else key
            items.extend(flatten_json(value, new_key, sep=sep).items())
    elif isinstance(json_obj, list):
        for i, value in enumerate(json_obj):
            new_key = f"{parent_key}{sep}{i}" if parent_key else str(i)
            items.extend(flatten_json(value, new_key, sep=sep).items())
    else:
        items.append((parent_key, json_obj))
    return dict(items)

#Dump json to csv files
def dump_data_to_files(json_data) -> None:
    #flattened_json = flatten_json(json_data)
    with open(json_file, 'w') as json_f:
        json.dump(json_data, json_f)

    data_list=[]
    for ticker, details in json_data.items():
        data_record = {**details, 'ticker': ticker}
        data_list.append(data_record)

    df = pd.DataFrame(data_list)
    df['processed_timestamp'] = datetime.now().strftime('%Y-%m-%d %H')
    if os.path.exists(csv_file):
        df.to_csv(csv_file, index=False, mode='a', header=False)
    else:
        df.to_csv(csv_file, index=False, mode='w')

    print(f"JSON file saved at {json_file}")
    print(f"CSV file saved at {csv_file}")

if __name__ == '__main__':
    #tickers = load_tickers_from_file()
    tickers = ['AAPL', 'MSFT', 'BRK.B', 'JNJ', 'JPM']
    all_data = {}
    for ticker in tickers:
        try:
            data = get_data(ticker)
            all_data[ticker] = data
        except Exception as e:
            print(e)

    dump_data_to_files(all_data)