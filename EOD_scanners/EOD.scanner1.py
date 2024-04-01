# import pandas as pd
# import yfinance as yf
# import talib
# from flask import Flask, jsonify

# app = Flask(__name__)

# csv_urls = {
#     "Nifty 50": "https://archives.nseindia.com/content/indices/ind_nifty50list.csv",
#     "Nifty Next 50": "https://archives.nseindia.com/content/indices/ind_niftynext50list.csv",
#     "Nifty 100": "https://archives.nseindia.com/content/indices/ind_nifty100list.csv",
#     "Nifty 200": "https://archives.nseindia.com/content/indices/ind_nifty200list.csv",
#     "Nifty 500": "https://archives.nseindia.com/content/indices/ind_nifty500list.csv",
#     "Nifty Smallcap 50": "https://archives.nseindia.com/content/indices/ind_niftysmallcap50list.csv",
#     "Nifty Smallcap 100": "https://archives.nseindia.com/content/indices/ind_niftysmallcap100list.csv",
#     "Nifty Smallcap 250": "https://archives.nseindia.com/content/indices/ind_niftysmallcap250list.csv",
#     "Nifty Midcap 50": "https://archives.nseindia.com/content/indices/ind_niftymidcap50list.csv",
#     "Nifty Midcap 100": "https://archives.nseindia.com/content/indices/ind_niftymidcap100list.csv",
#     "Nifty Midcap 150": "https://archives.nseindia.com/content/indices/ind_niftymidcap150list.csv"
# }


# def get_symbols_from_csv(csv_url):
#     try:
#         df = pd.read_csv(csv_url)
#         return list(df['Symbol'] + ".NS")
#     except Exception as e:
#         print(f"Error reading CSV from {csv_url}: {e}")
#         return []


# def fetch_stock_data(symbol):
#     try:
#         stock_data = yf.download(symbol)
#         return stock_data
#     except Exception as e:
#         print(f"Failed to download data for {symbol}: {e}")
#         return pd.DataFrame()
    

# def calculate_rsi(data):
#     data['RSI'] = talib.RSI(data['Close'])
#     return data


# def scan_stocks(data):
#     data = calculate_rsi(data)
#     # Condition for EOD Scanner 01
#     monthly_rsi_above_60 = data['RSI'].resample('M').last() > 60
#     weekly_rsi_above_60 = data['RSI'].resample('W').last() > 60
#     daily_rsi_above_40 = data['RSI'] > 40
#     daily_price_above_50_ema = data['Close'] > data['Close'].ewm(span=50, adjust=False).mean()
#     relative_strength_above_1 = data['Close'] / data['Close'].shift(1) > 1

#     # Combine conditions
#     condition_met = monthly_rsi_above_60 & weekly_rsi_above_60 & daily_rsi_above_40 & daily_price_above_50_ema & relative_strength_above_1

#     filtered_data = data[condition_met]
#     return filtered_data


# @app.route('/get_eod_scanner_01', methods=['GET'])
# def get_eod_scanner_01():
#     # symbol = 'AAPL'  # Example stock symbol
#     category = "Nifty 50"
#     csv_file_url = csv_urls.get(category)
    
#     if csv_file_url is None:
#         print(f"No CSV URL found for category: {category}. Exiting program.")
#         return

#     symbols = get_symbols_from_csv(csv_file_url)
#     if not symbols:
#         print(f"No symbols found for category: {category}. Exiting program.")
#         return

#     data = pd.DataFrame()
#     for symbol in symbols:
#         stock_data = fetch_stock_data(symbol)
#         if not stock_data.empty:
#             data = pd.concat([data, stock_data], axis=0)

#     if not data.empty:
#         scanned_stocks = scan_stocks(data)
#         if not scanned_stocks.empty:
#             print("Stocks meeting Scanner 4 conditions:")
#             print(scanned_stocks)
#             return jsonify(scanned_stocks.to_dict(orient='records'))
#         else:
#             print("No stocks found.")
#             return jsonify({'message': "No stocks found."})
#     else:
#         print("No data available for the selected category.")
#         return jsonify({'message': "No data available for the selected category."})
    
    


# if __name__ == "__main__":
#     app.run(debug=True)





import pandas as pd
import yfinance as yf
import talib
from flask import Flask, jsonify

app = Flask(__name__)

csv_urls = {
    "Nifty 50": "https://archives.nseindia.com/content/indices/ind_nifty50list.csv",
    "Nifty Next 50": "https://archives.nseindia.com/content/indices/ind_niftynext50list.csv",
    "Nifty 100": "https://archives.nseindia.com/content/indices/ind_nifty100list.csv",
    "Nifty 200": "https://archives.nseindia.com/content/indices/ind_nifty200list.csv",
    "Nifty 500": "https://archives.nseindia.com/content/indices/ind_nifty500list.csv",
    "Nifty Smallcap 50": "https://archives.nseindia.com/content/indices/ind_niftysmallcap50list.csv",
    "Nifty Smallcap 100": "https://archives.nseindia.com/content/indices/ind_niftysmallcap100list.csv",
    "Nifty Smallcap 250": "https://archives.nseindia.com/content/indices/ind_niftysmallcap250list.csv",
    "Nifty Midcap 50": "https://archives.nseindia.com/content/indices/ind_niftymidcap50list.csv",
    "Nifty Midcap 100": "https://archives.nseindia.com/content/indices/ind_niftymidcap100list.csv",
    "Nifty Midcap 150": "https://archives.nseindia.com/content/indices/ind_niftymidcap150list.csv"
}

def get_symbols_from_csv(csv_url):
    try:
        df = pd.read_csv(csv_url)
        return list(df['Symbol'] + ".NS")
    except Exception as e:
        print(f"Error reading CSV from {csv_url}: {e}")
        return []

def fetch_stock_data(symbol):
    try:
        stock_data = yf.download(symbol)
        return stock_data
    except Exception as e:
        print(f"Failed to download data for {symbol}: {e}")
        return pd.DataFrame()

def calculate_rsi(data):
    data['RSI'] = talib.RSI(data['Close'])
    return data

def scan_stocks(data):
    data = calculate_rsi(data)
    # Condition for EOD Scanner 01
    # monthly_rsi_above_60 = data['RSI'].resample('M').last() > 60
    monthly_rsi_above_60 = data['RSI'].resample('ME').last() > 60
    weekly_rsi_above_60 = data['RSI'].resample('W').last() > 60
    daily_rsi_above_40 = data['RSI'] > 40
    daily_price_above_50_ema = data['Close'] > data['Close'].ewm(span=50, adjust=False).mean()
    relative_strength_above_1 = data['Close'] / data['Close'].shift(1) > 1

    # Combine conditions
    condition_met = monthly_rsi_above_60 & weekly_rsi_above_60 & daily_rsi_above_40 & daily_price_above_50_ema & relative_strength_above_1

    filtered_data = data.loc[condition_met]  # Use .loc[] accessor for boolean indexing
    return filtered_data


@app.route('/get_eod_scanner_01', methods=['GET'])
def get_eod_scanner_01():
    category = "Nifty 50"
# symbol = "SBIN.NS"
    if category:
        csv_file_url = csv_urls.get(category)
        
        if csv_file_url is None:
            print(f"No CSV URL found for category: {category}. Exiting program.")
            return jsonify({'message': f"No CSV URL found for category: {category}."})

        symbols = get_symbols_from_csv(csv_file_url)
        if not symbols:
            print(f"No symbols found for category: {category}. Exiting program.")
            return jsonify({'message': f"No symbols found for category: {category}."})

        all_filtered_data = pd.DataFrame()
        for symbol in symbols:
            stock_data = fetch_stock_data(symbol)
            if not stock_data.empty:
                filtered_data = scan_stocks(stock_data)
                all_filtered_data = pd.concat([all_filtered_data, filtered_data], axis=0)

        if not all_filtered_data.empty:
            print(all_filtered_data)
            return jsonify(all_filtered_data.to_dict(orient='records'))
        else:
            print("No stock Found")
            return jsonify({'message': "No stocks found."})
    else:
        stock_data = fetch_stock_data(symbol)
        if not stock_data.empty:
            filtered_data = scan_stocks(stock_data)
            if not filtered_data.empty:
                print(filtered_data)
                return jsonify(filtered_data.to_dict(orient='records'))
            else:
                print("No Stock Found")
                return jsonify({'message': "No stocks found."})
        else:
            print("Failed to fetch stock data.")
            return jsonify({'message': "Failed to fetch stock data."})
   

if __name__ == "__main__":
    app.run(debug=True)
z