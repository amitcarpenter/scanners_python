import pandas as pd
import yfinance as yf
import talib

import os
import sys
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(parent_dir)

from utils.functions import get_symbols_from_csv, fetch_additional_info_from_csv, fetch_stock_data
from utils.csv import csv_urls

def calculate_technical_indicators(data, num_periods):
    if 'Volume' in data.columns and 'Volume_MA_5' not in data.columns:
        data['Volume_MA_5'] = data['Volume'].rolling(window=5, min_periods=1).mean()
    if 'Close' in data.columns and 'RSI' not in data.columns:
        data['RSI'] = talib.RSI(data['Close'], timeperiod=14)
    data['Day Change %'] = ((data['Close'] - data['Close'].shift(1)) / data['Close'].shift(1)) * 100
    data['Volume_Average'] = data['Volume'].rolling(window=num_periods).mean()
    return data


def scan_stocks_with_additional_info(data, volume_threshold, rsi_threshold, csv_file, close_number):
    results = []

    for symbol, stock_data in data.groupby('Symbol'):
        last_row = stock_data.iloc[-1]

        if last_row['Volume'] > last_row['Volume_Average']:
            if (
                last_row['Volume'] > volume_threshold and
                last_row['RSI'] > rsi_threshold and
                last_row['Close'] > close_number and
                last_row['Close'] >= stock_data['High'].shift(1).iloc[-1]  
            ):
                additional_info = fetch_additional_info_from_csv(symbol, csv_file)
                symbol = symbol.replace(".NS", "")
                result = {
                    'Symbol': symbol,
                    'Stock Name': additional_info['Company Name'],
                    'Sector': additional_info['Industry'],
                    'LTP': last_row['Close'],
                    '52W High': last_row['52W High'],
                    '52W Low': last_row['52W Low'],
                    'Volume': last_row['Volume'],
                    'RSI': last_row['RSI'],
                    'Day Change %': last_row['Day Change %'],
                    'First Appeared on': last_row['First Appeared on'],
                    'Dividend Date': last_row['Dividend Date']
                }
                results.append(result)
    results_df = pd.DataFrame(results)
    return results_df


def live_scanner_01(category, symbol, start_date, end_date, volume_threshold, rsi_threshold, close_number, num_periods):
    rsi_threshold = int(rsi_threshold)
    volume_threshold = int(volume_threshold)
    close_number = int(close_number)
    num_periods = int(num_periods)
    csv_file_url = None
    
    if category:
        csv_file_url = csv_urls.get(category)
        if csv_file_url is None:
            print(f"No CSV URL found for category: {category}. Exiting program.")
            return

        symbols = get_symbols_from_csv(csv_file_url)
        if not symbols:
            print(f"No symbols found for category: {category}. Exiting program.")
            return

        data = pd.DataFrame()
        for symbol in symbols:
            stock_data = fetch_stock_data(symbol, start_date, end_date)
            if not stock_data.empty:
                data = pd.concat([data, stock_data], axis=0)
                
        if not data.empty:
            data = calculate_technical_indicators(data, num_periods)
            scanned_stocks = scan_stocks_with_additional_info(data, volume_threshold, rsi_threshold, csv_file_url, close_number)

            if not scanned_stocks.empty:
                print("Stocks meeting Scanner 1 conditions:")
                print(scanned_stocks.to_string(index=False))
                return scanned_stocks.to_dict(orient='records')
            else:
                print("No stocks found.")
                return {'message': "No stocks found."}
        else:
            print("No data available for the selected Index.")
            return {'message': "No data available for the selected category."}
    else:
        data = fetch_stock_data(symbol, start_date, end_date)
        if not data.empty:
            data = calculate_technical_indicators(data, num_periods)
            scanned_stocks = scan_stocks_with_additional_info(data, volume_threshold, rsi_threshold, None, close_number)

            if not scanned_stocks.empty:
                print("Stocks meeting Scanner 1 conditions:")
                print(scanned_stocks)
                return scanned_stocks.to_dict(orient='records')
            else:
                print("No stocks found.")
                return {'message': "No stocks found."}
        else:
            print("No data available for the selected symbol.")
            return {'message': "No data available for the selected symbol."}
        

# index = 'Nifty 50'
# # index = None
# symbol = "MARUTI.NS"
# start_date = '2024-04-01'
# end_date = '2024-04-09'
# volume_threshold = 500000 
# rsi_threshold = 60
# close_number = 50
# num_periods = 5

# live_scanner_01(index, symbol, start_date, end_date, volume_threshold, rsi_threshold, close_number, num_periods)
