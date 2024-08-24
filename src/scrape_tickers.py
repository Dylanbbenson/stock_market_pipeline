import requests
from bs4 import BeautifulSoup
import json
from datetime import date, datetime
current_date = date.today().strftime('%Y-%m-%d')
current_time = datetime.now().strftime("%H")

url = 'https://www.tradingview.com/markets/stocks-usa/market-movers-active/'
json_file = f"./data/tickers_{current_date}.json"

def scrape_tickers(url):
    response = requests.get(url)
    if response.status_code != 200:
        print(f"Failed to retrieve the page: Status code {response.status_code}")
        return []

    soup = BeautifulSoup(response.text, 'html.parser')
    tickers = []
    for item in soup.find_all('a', class_='apply-common-tooltip tickerNameBox-GrtoTeat tickerName-GrtoTeat'):
        tickers.append(item.text.strip())

    return tickers


def save_tickers_to_file(tickers, filename=json_file):
    with open(filename, 'w') as f:
        json.dump(tickers, f, indent=4)


if __name__ == '__main__':
    tickers = scrape_tickers(url)
    if tickers:
        save_tickers_to_file(tickers)
        ticker_count = len(tickers)
        print(f"Successfully scraped {ticker_count} tickers")
    else:
        print("ERROR: No tickers found.")