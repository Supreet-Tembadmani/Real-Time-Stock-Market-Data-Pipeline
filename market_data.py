import yfinance as yf
import json
import time

def fetch_stock_data(ticker):
    stock = yf.Ticker(ticker)
    data = stock.history(period="1m")
    return data

if __name__ == "__main__":
    ticker = "AAPL"
    while True:
        data = fetch_stock_data(ticker)
        print(json.dumps(data.to_dict()))
        time.sleep(60)
