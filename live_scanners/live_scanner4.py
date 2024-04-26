import pandas as pd
import yfinance as yf
import talib
import numpy as np

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


def calculate_super_trend(data, period, multiplier):
    high = data['High']
    low = data['Low']
    close = data['Close']
    
    atr = talib.ATR(high, low, close, timeperiod=period)

    basic_upper_band = (high + low) / 2 + multiplier * atr
    basic_lower_band = (high + low) / 2 - multiplier * atr

    super_trend = pd.Series(index=data.index)
    super_trend.iloc[0] = np.nan


    for i in range(1, len(data)):
        if close.iloc[i-1] <= basic_upper_band.iloc[i-1]:
            super_trend.iloc[i] = min(basic_upper_band.iloc[i], high.iloc[i])
        else:
            super_trend.iloc[i] = min(basic_lower_band.iloc[i], low.iloc[i])

    return super_trend


def apply_conditions(data, period1=7, multiplier1=3, period2=4, multiplier2=1):

    data['Super_Trend_1'] = calculate_super_trend(data, period=period1, multiplier=multiplier1)
    data['Super_Trend_2'] = calculate_super_trend(data, period=period2, multiplier=multiplier2)
    
    condition1 = data['Close'] > data['Super_Trend_1']
    condition2 = (data['Close'] > data['Super_Trend_2']) & (data['Close'].shift(1) <= data['Super_Trend_2'].shift(1))
    
    data['Condition_1'] = condition1
    data['Condition_2'] = condition2
    
    print(condition1 , "condition 1")
    print(condition2 , "condition 2")
    
    return data


def scan_stocks_with_additional_info(data, csv_file, volume_threshold):
    results = []
    for symbol, stock_data in data.groupby('Symbol'):
        stock_data_for_super_trade = apply_conditions(stock_data)
        if stock_data_for_super_trade['Condition_1'].iloc[-1] and stock_data_for_super_trade['Condition_2'].iloc[-1]:
            last_row = stock_data.iloc[-1]
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



# import datetime

# index = 'Nifty 50'
# symbol = None
# start_date = datetime.datetime.strptime('2024-04-03', '%Y-%m-%d')
# end_date = datetime.datetime.strptime('2024-04-04', '%Y-%m-%d')
# volume_threshold = 50000 

# while start_date <= end_date:
#     current_start_date = start_date.strftime('%Y-%m-%d')
#     current_end_date = end_date.strftime('%Y-%m-%d')
#     live_scanner_04(index, symbol, current_start_date, current_end_date, volume_threshold)
#     start_date += datetime.timedelta(days=1)
#     end_date += datetime.timedelta(days=1)

