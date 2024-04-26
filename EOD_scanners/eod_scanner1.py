import pandas as pd
import talib

from utils.functions import get_symbols_from_csv, fetch_stock_data, fetch_additional_info_from_csv
from utils.csv import csv_urls

def calculate_technical_indicators(data):
    data['RSI'] = talib.RSI(data['Close'], timeperiod=14)
    data['Day Change %'] = ((data['Close'] - data['Close'].shift(1)) / data['Close'].shift(1)) * 100
    return data


def scan_stocks(data):
    print("Hello i am startin scan stock ----------------------------")
    data = calculate_technical_indicators(data)
    monthly_rsi_above_60 = data['RSI'].resample('ME').last().reindex(data.index) >= 60
    weekly_rsi_above_60 = data['RSI'].resample('W').last().reindex(data.index) >= 60
    daily_rsi_above_40 = data['RSI'] >= 40
    daily_close_above_10 = data['Close'] >= 10
    weekly_close_above_ema5 = data['Close'] >= data['Close'].resample('W').last().ewm(span=5).mean().reindex(data.index)
    one_week_ago_close_less_than_ema5 = data['Close'].shift(7) < data['Close'].shift(7).ewm(span=5).mean().reindex(data.index)
    weekly_close_above_daily_ema200 = data['Close'] >= data['Close'].resample('W').last().ewm(span=200).mean().reindex(data.index)
    
    print(data)

    condition_met = monthly_rsi_above_60 & weekly_rsi_above_60 & daily_rsi_above_40 & \
                    daily_close_above_10 & weekly_close_above_ema5 & \
                    one_week_ago_close_less_than_ema5 & weekly_close_above_daily_ema200

    filtered_data = data.loc[condition_met]
    print(filtered_data)
    return filtered_data


def scan_stocks_with_additional_info(data, csv_file, volume_threshold):
    results = []
    for symbol, stock_data in data.groupby('Symbol'):
        stock_data_for_super_trade = scan_stocks(stock_data)
        if not stock_data_for_super_trade.empty:
            last_row = stock_data_for_super_trade.iloc[-1]
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


def eod_scanner_01(index, symbol, start_date, end_date, volume_threshold):
    if index:
        csv_file_url = csv_urls.get(index)
        if csv_file_url is None:
            print(f"No CSV URL found for index: {index}. Exiting program.")
            return {'message': f"No CSV URL found for index: {index}."}

        symbols = get_symbols_from_csv(csv_file_url)
        if not symbols:
            print(f"No symbols found for index: {index}. Exiting program.")
            return {'message': f"No symbols found for index: {index}."}

        all_filtered_data = pd.DataFrame()
        for symbol in symbols:
            stock_data = fetch_stock_data(symbol, start_date, end_date)
            # print(stock_data)
            if not stock_data.empty:
                filtered_data = scan_stocks(stock_data)
                all_filtered_data = pd.concat([all_filtered_data, filtered_data], axis=0)

        if not all_filtered_data.empty:
            print(all_filtered_data)
            return all_filtered_data.to_dict(orient='records')
        else:
            print("No stocks found")
            return {'message': "No stocks found."}
    else:
        stock_data = fetch_stock_data(symbol, start_date, end_date)
        if not stock_data.empty:
            filtered_data = scan_stocks(stock_data)
            if not filtered_data.empty:
                print(filtered_data)
                return filtered_data.to_dict(orient='records')
            else:
                print("No stocks found")
                return {'message': "No stocks found."}
        else:
            print("Failed to fetch stock data")
            return {'message': "Failed to fetch stock data."}


index = 'Nifty 50'
symbol = None
start_date = '2024-02-26'
end_date = '2024-04-26'
volume_threshold = 10000


# eod_scanner_01(index, symbol, start_date, end_date, volume_threshold)
