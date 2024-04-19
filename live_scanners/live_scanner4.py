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


def scan_stocks_with_additional_info(data, csv_file, volume_threshold):
    results = []
    for symbol, stock_data in data.groupby('Symbol'):
        last_row = stock_data.iloc[-1]
        supertrend_params_1 = (7, 3)
        supertrend_params_2 = (4, 1)
        if (stock_passes_filters(stock_data, [supertrend_params_1, supertrend_params_2])):
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


# def stock_passes_filters(stock_data, volume_threshold):
#     # Condition 1: Daily Close Greater than Daily Supertrend(7,3)
#     supertrend_7_3 = talib.SMA(stock_data['Close'], timeperiod=7)
#     if stock_data['Close'].iloc[-1] <= supertrend_7_3.iloc[-1]:
#         return False

#     # Condition 2: Daily Close Crossed above Daily Supertrend(4,1)
#     supertrend_4_1 = talib.SMA(stock_data['Close'], timeperiod=4)
#     if not (stock_data['Close'].iloc[-1] > supertrend_4_1.iloc[-1] and stock_data['Close'].iloc[-2] <= supertrend_4_1.iloc[-2]):
#         return False

#     # Condition 3: Daily Volume Greater than volume_threshold
#     if stock_data['Volume'].iloc[-1] <= volume_threshold:
#         return False

#     return True


def stock_passes_filters(stock_data, supertrend_params):
    # Extract Supertrend parameters
    period_1, multiplier_1 = supertrend_params[0]
    period_2, multiplier_2 = supertrend_params[1]

    # Condition 1: Daily Close Greater than Daily Supertrend(period_1, multiplier_1)
    supertrend_1 = talib.SMA(stock_data['Close'], timeperiod=period_1)
    if stock_data['Close'].iloc[-1] <= supertrend_1.iloc[-1]:
        return False

    # Condition 2: Daily Close Crossed above Daily Supertrend(period_2, multiplier_2)
    supertrend_2 = talib.SMA(stock_data['Close'], timeperiod=period_2)
    if not (stock_data['Close'].iloc[-1] > supertrend_2.iloc[-1] and stock_data['Close'].iloc[-2] <= supertrend_2.iloc[-2]):
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
            scanned_stocks = scan_stocks_with_additional_info(data, csv_file_url, volume_threshold)

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



# index = 'Nifty 500'
# # index = None
# symbol = None
# start_date = '2024-04-07'
# end_date = '2024-04-08'
# volume_threshold = 50000 


# live_scanner_04(index, symbol, start_date, end_date, volume_threshold)