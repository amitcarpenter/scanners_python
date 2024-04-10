# import pandas as pd
# import yfinance as yf
# import talib

# import os
# import sys
# parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
# sys.path.append(parent_dir)

# from utils.functions import get_symbols_from_csv, fetch_additional_info_from_csv, fetch_stock_data
# from utils.csv import csv_urls


# def calculate_technical_indicators(data):
#     if 'Volume' in data.columns and 'Volume_MA_5' not in data.columns:
#         data['Volume_MA_5'] = data['Volume'].rolling(window=5, min_periods=1).mean()

#     if 'Close' in data.columns and 'RSI' not in data.columns:
#         data['RSI'] = talib.RSI(data['Close'], timeperiod=14)

#     if 'High' in data.columns and 'Previous_High' not in data.columns:
#         data['Previous_High'] = data['High'].shift(1)
    
#     data['Day Change %'] = ((data['Close'] - data['Close'].shift(1)) / data['Close'].shift(1)) * 100
    
#     # Calculate EMAs
#     data['EMA_5'] = talib.EMA(data['Close'], timeperiod=5)
#     data['EMA_13'] = talib.EMA(data['Close'], timeperiod=13)
#     data['EMA_26'] = talib.EMA(data['Close'], timeperiod=26)
    
#     return data


# def scan_stocks_with_additional_info(data, csv_file):
#     results = []

#     for symbol, stock_data in data.groupby('Symbol'):
#         last_row = stock_data.iloc[-1]
#         if (
#             last_row['Close'] >= 50 and
#             last_row['EMA_5'] > last_row['EMA_26'] and
#             last_row['EMA_13'] > last_row['EMA_26'] and
#             (last_row['Close'] > stock_data['Close'].shift(1) * 1.03).all() and
#             last_row['Volume'] > stock_data['Volume'].rolling(window=20, min_periods=1).mean() * 1.0 and
#             last_row['EMA_5'] > last_row['EMA_13'] and
#             last_row['High'] == stock_data['High'].rolling(window=260, min_periods=1).max() and
#             last_row['High'] > last_row['Close'] and
#             stock_data['Close'].shift(1) > stock_data['Close'].shift(2) * 0.98
#         ):
#             additional_info = fetch_additional_info_from_csv(symbol, csv_file)
#             result = {
#                 'Symbol': symbol,
#                 'Stock Name': additional_info['Company Name'],
#                 'Sector': additional_info['Industry'],
#                 'LTP': last_row['Close'],
#                 '52W High': last_row['52W High'],
#                 '52W Low': last_row['52W Low'],
#                 'Volume': last_row['Volume'],
#                 'RSI': last_row['RSI'],
#                 'Day Change %': last_row['Day Change %'],
#                 'First Appeared on': last_row['First Appeared on'],
#                 'Dividend Date': last_row['Dividend Date']
#             }
#             results.append(result)
#     results_df = pd.DataFrame(results)
#     return results_df


# def live_scanner_04(category, symbol, start_date, end_date):
#     csv_file_url = None
#     if category:
#         csv_file_url = csv_urls.get(category)
#         if csv_file_url is None:
#             print(f"No CSV URL found for category: {category}. Exiting program.")
#             return

#         symbols = get_symbols_from_csv(csv_file_url)
#         if not symbols:
#             print(f"No symbols found for category: {category}. Exiting program.")
#             return

#         data = pd.DataFrame()
#         for symbol in symbols:
#             stock_data = fetch_stock_data(symbol, start_date, end_date)
#             if not stock_data.empty:
#                 data = pd.concat([data, stock_data], axis=0)

#         if not data.empty:
#             data = calculate_technical_indicators(data)
#             scanned_stocks = scan_stocks_with_additional_info(data, csv_file_url)

#             if not scanned_stocks.empty:
#                 print("Stocks meeting Scanner 4 conditions:")
#                 print(scanned_stocks)
#                 return (scanned_stocks.to_dict(orient='records'))
#             else:
#                 print("No stocks found.")
#                 return ({'message': "No stocks found."})
#         else:
#             print("No data available for the selected category.")
#             return ({'message': "No data available for the selected category."})
#     else:
#         data = fetch_stock_data(symbol, start_date, end_date)
#         if not data.empty:
#             data = calculate_technical_indicators(data)
#             scanned_stocks = scan_stocks_with_additional_info(data, None)

#             if not scanned_stocks.empty:
#                 print("Stocks meeting Scanner 4 conditions:")
#                 print(scanned_stocks)
#                 return (scanned_stocks.to_dict(orient='records'))
#             else:
#                 print("No stocks found.")
#                 return ({'message': "No stocks found."})
#         else:
#             print("No data available for the selected symbol.")
#             return ({'message': "No data available for the selected symbol."})


import pandas as pd
import yfinance as yf
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


def scan_stocks_with_additional_info(data, csv_file):
    results = []
    for symbol, stock_data in data.groupby('Symbol'):
        last_row = stock_data.iloc[-1]
        if (stock_passes_filters(stock_data)):
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


def stock_passes_filters(stock_data):
    # Condition 1: Daily Close Greater than Daily Supertrend(7,3)
    print(stock_data['Close'].isnull().sum())  # Check for NaN values in the 'Close' series
    print(stock_data['Close'].dtype)
    print(len(stock_data['Close']))


    supertrend_7_3 = talib.SMA(stock_data['Close'], timeperiod=7)
    # print(supertrend_7_3)
    if stock_data['Close'].iloc[-1] > supertrend_7_3.iloc[-1]:
        pass
    else:
        return False

    # Condition 2: Daily Close Crossed above Daily Supertrend(4,1)
    supertrend_4_1 = talib.SMA(stock_data['Close'], timeperiod=4)
    # print(supertrend_4_1)
    if stock_data['Close'].iloc[-1] > supertrend_4_1.iloc[-1] and stock_data['Close'].iloc[-2] <= supertrend_4_1.iloc[-2]:
        pass
    else:
        return False

    # Condition 3: Daily Volume Greater than 50000
    if stock_data['Volume'].iloc[-1] > 50000:
        pass
    else:
        return False

    return True


def live_scanner_04(index, symbol, start_date, end_date, volume_threshold):
    csv_file_url = None
    if index:
        csv_file_url = csv_urls.get(index)
        if csv_file_url is None:
            print(f"No CSV URL found for index: {index}. Exiting program.")
            return

        symbols = get_symbols_from_csv(csv_file_url)
        if not symbols:
            print(f"No symbols found for index: {index}. Exiting program.")
            return

        data = pd.DataFrame()
        for symbol in symbols:
            stock_data = fetch_stock_data(symbol, start_date, end_date)
            if not stock_data.empty:
                data = pd.concat([data, stock_data], axis=0)

        if not data.empty:
            data = calculate_technical_indicators(data)
            scanned_stocks = scan_stocks_with_additional_info(data, csv_file_url)

            if not scanned_stocks.empty:
                print("Stocks meeting Scanner 4 conditions:")
                print(scanned_stocks)
                return (scanned_stocks.to_dict(orient='records'))
            else:
                print("No stocks found.")
                return ({'message': "No stocks found."})
        else:
            print("No data available for the selected index.")
            return ({'message': "No data available for the selected index."})
    else:
        data = fetch_stock_data(symbol, start_date, end_date)
        if not data.empty:
            data = calculate_technical_indicators(data)
            scanned_stocks = scan_stocks_with_additional_info(data, None)

            if not scanned_stocks.empty:
                print("Stocks meeting Scanner 4 conditions:")
                print(scanned_stocks)
                return (scanned_stocks.to_dict(orient='records'))
            else:
                print("No stocks found.")
                return ({'message': "No stocks found."})
        else:
            print("No data available for the selected symbol.")
            return ({'message': "No data available for the selected symbol."})


