import pandas as pd
import talib
import os
import sys

parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(parent_dir)

from utils.functions import get_symbols_from_csv, fetch_additional_info_from_csv, fetch_stock_data
from utils.csv import csv_urls


def calculate_technical_indicators(data):
    data['RSI'] = talib.RSI(data['Close'], timeperiod=14)
    data['Day Change %'] = ((data['Close'] - data['Close'].shift(1)) / data['Close'].shift(1)) * 100
    return data


def scan_stocks_with_additional_info(data, csv_file, volume_threshold):
    results = []
    for symbol, stock_data in data.groupby('Symbol'):
        last_row = stock_data.iloc[-1]
        if (
            is_SMA_condition_met(stock_data, volume_threshold)
        ):
            additional_info = fetch_additional_info_from_csv(symbol, csv_file)
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


def is_SMA_condition_met(stock_data, volume_threshold):
    
    for i in range(1, 8):
        if (stock_data['High'] - stock_data['Low']).iloc[-1] <= (stock_data['High'].shift(i) - stock_data['Low'].shift(i)).iloc[-1]:
            return False

    if stock_data['Close'].iloc[-1] <= stock_data['Open'].iloc[-1]:
        return False
    
    if stock_data['Close'].iloc[-1] <= stock_data['Open'].shift(1).iloc[-1]:
        return False

    if stock_data['Close'].resample('W').last().iloc[-1] <= stock_data['Open'].resample('W').last().iloc[-1]:
        return False
    
    if stock_data['Close'].resample('ME').last().iloc[-1] <= stock_data['Open'].resample('ME').last().iloc[-1]:
        return False

    if stock_data['Volume'].shift(1).iloc[-1] <= volume_threshold:
        return False

    sma_20 = stock_data['Close'].rolling(window=20).mean()
    sma_40 = stock_data['Close'].rolling(window=40).mean()
    sma_60 = stock_data['Close'].rolling(window=60).mean()
    
    if sma_20.iloc[-1] <= sma_40.iloc[-1] or sma_40.iloc[-1] <= sma_60.iloc[-1]:

        return False
    
    return True


def live_scanner_02(index, symbol, start_date, end_date, volume_threshold):
    volume_threshold = int(volume_threshold)
    print('#' * 50)
    csv_file_url = None
    if index:
        csv_file_url = csv_urls.get(index)
        if csv_file_url is None:
            print(f"No CSV URL found for category: {index}. Exiting program.")
            return

        symbols = get_symbols_from_csv(csv_file_url)
        if not symbols:
            print(f"No symbols found for category: {index}. Exiting program.")
            return
        data = pd.DataFrame()
        for symbol in symbols:
            stock_data = fetch_stock_data(symbol, start_date, end_date)
            if not stock_data.empty:
                data = pd.concat([data, stock_data], axis=0)

        if not data.empty:
            data = calculate_technical_indicators(data)
            scanned_stocks = scan_stocks_with_additional_info(data, csv_file_url, volume_threshold)
            if not scanned_stocks.empty:
                print("Stocks meeting Scanner 2 conditions:")
                print(scanned_stocks.to_string(index=False))
                return scanned_stocks.to_dict(orient='records')
            else:
                print("No stocks found.")
                return {'message': "No stocks found."}
        else:
            print("No data available for the selected category.")
            return {'message': "No data available for the selected category."}
    else:
        data = fetch_stock_data(symbol, start_date, end_date)
        if not data.empty:
            data = calculate_technical_indicators(data)
            scanned_stocks = scan_stocks_with_additional_info(data, None, volume_threshold)

            if not scanned_stocks.empty:
                print("Stocks meeting Scanner 2 conditions:")
                print(scanned_stocks.to_string(index=False))
                return scanned_stocks.to_dict(orient='records')
            else:
                print("No stocks found.")
                return {'message': "No stocks found."}
        else:
            print("No data available for the selected symbol.")
            return {'message': "No data available for the selected symbol."}


# index = 'Nifty 50'
# # index = None
# symbol = None
# start_date = '2024-04-11'
# end_date = '2024-04-18'
# volume_threshold = 10000 

# live_scanner_02(index, symbol, start_date, end_date, volume_threshold)