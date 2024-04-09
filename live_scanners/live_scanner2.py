import pandas as pd
import talib
import os
import sys

parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(parent_dir)

from utils.functions import get_symbols_from_csv, fetch_additional_info_from_csv, fetch_stock_data
from utils.csv import csv_urls


def calculate_technical_indicators(data):
    # if 'Volume' in data.columns and 'Volume_MA_5' not in data.columns:
    #     data['Volume_MA_5'] = data['Volume'].rolling(window=5, min_periods=1).mean()

    # if 'Close' in data.columns and 'RSI' not in data.columns:
    #     data['RSI'] = talib.RSI(data['Close'], timeperiod=14)

    # if 'High' in data.columns and 'Previous_High' not in data.columns:
    #     data['Previous_High'] = data['High'].shift(1)

    data['Day Change %'] = ((data['Close'] - data['Close'].shift(1)) / data['Close'].shift(1)) * 100

    return data


def scan_stocks_with_additional_info(data, csv_file):
    results = []

    for symbol, stock_data in data.groupby('Symbol'):
        last_row = stock_data.iloc[-1]
        if (
            is_NR7(stock_data) and
            is_SMA_condition_met(stock_data)
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


def is_NR7(stock_data):
    # NR7 condition: range (high - low) of the last 7 days is the smallest compared to the previous 6 observations
    if len(stock_data) < 8:
        return False
    last_7_days_range = (stock_data['High'] - stock_data['Low']).tail(7)
    return last_7_days_range.min() == last_7_days_range.iloc[-1]


def is_SMA_condition_met(stock_data):
    if len(stock_data) < 60:
        return False

    # Daily high-low range comparisons
    for i in range(1, 8):
        if (stock_data['High'] - stock_data['Low']).iloc[-i] <= (
                stock_data['High'].shift(i) - stock_data['Low'].shift(i)).iloc[-1]:
            return False

    # Closing price comparisons
    if stock_data['Close'].iloc[-1] <= stock_data['Open'].iloc[-1]:
        return False
    if stock_data['Close'].iloc[-1] <= stock_data['Open'].shift(1).iloc[-1]:
        return False

    # Weekly close comparisons
    if len(stock_data.resample('W').last()) < 2:
        return False
    if stock_data['Close'].resample('W').last().iloc[-1] <= stock_data['Open'].resample('W').last().iloc[-1]:
        return False

    # Monthly close comparisons
    if len(stock_data.resample('M').last()) < 2:
        return False
    if stock_data['Close'].resample('M').last().iloc[-1] <= stock_data['Open'].resample('M').last().iloc[-1]:
        return False

    # Volume condition
    if stock_data['Volume'].shift(1).iloc[-1] <= 10000:
        return False

    # SMA conditions
    sma_20 = stock_data['Close'].rolling(window=20).mean()
    sma_40 = stock_data['Close'].rolling(window=40).mean()
    sma_60 = stock_data['Close'].rolling(window=60).mean()
    if sma_20.iloc[-1] <= sma_40.iloc[-1] or sma_40.iloc[-1] <= sma_60.iloc[-1]:
        return False

    return True


def live_scanner_02(index, symbol, start_date, end_date):
    """
    Scanner to filter stocks based on specified conditions.
    
    Conditions:
    1. (Daily 'High' - Daily 'Low') greater than (1 day ago 'High' - 1 day ago 'Low')
    2. (Daily 'High' - Daily 'Low') greater than (2 day ago 'High' - 2 day ago 'Low')
    3. (Daily 'High' - Daily 'Low') greater than (3 day ago 'High' - 3 day ago 'Low')
    4. (Daily 'High' - Daily 'Low') greater than (4 day ago 'High' - 4 day ago 'Low')
    5. (Daily 'High' - Daily 'Low') greater than (5 day ago 'High' - 5 day ago 'Low')
    6. (Daily 'High' - Daily 'Low') greater than (6 day ago 'High' - 6 day ago 'Low')
    7. (Daily 'High' - Daily 'Low') greater than (7 day ago 'High' - 7 day ago 'Low')
    8. Daily 'Close' greater than Daily 'Open'
    9. Daily 'Close' greater than 1 day ago 'Open'
    10. Weekly 'Close' greater than Weekly 'Open'
    11. Monthly 'Close' greater than Monthly 'Open'
    12. 1 day ago 'Volume' greater than '10000'
    13. Daily 'Sma(close,20)' greater than Daily 'Sma(close,40)'
    14. Daily 'Sma (close,40)' greater than Daily 'Sma(close,60)'
    """
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
            scanned_stocks = scan_stocks_with_additional_info(data, csv_file_url)

            if not scanned_stocks.empty:
                print("Stocks meeting Scanner 2 conditions:")
                print(scanned_stocks)
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
            scanned_stocks = scan_stocks_with_additional_info(data, None)

            if not scanned_stocks.empty:
                print("Stocks meeting Scanner 2 conditions:")
                print(scanned_stocks)
                return scanned_stocks.to_dict(orient='records')
            else:
                print("No stocks found.")
                return {'message': "No stocks found."}
        else:
            print("No data available for the selected symbol.")
            return {'message': "No data available for the selected symbol."}


index = 'Nifty 50'
# index = None
symbol = "MARUTI.NS"
start_date = '2024-04-01'
end_date = '2024-04-09'
volume_threshold = 500000 



live_scanner_02(index, symbol, start_date, end_date)