import pandas as pd
import yfinance as yf
import talib
import datetime
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


def fetch_stock_data(symbol, start_date, end_date):
    try:
        stock_data = yf.download(symbol, start=start_date, end=end_date)
        if not stock_data.empty:
            ticker = yf.Ticker(symbol)
            hist = ticker.history(period="max")
            first_appeared_date = hist.index[0].strftime("%d %B %Y")
            dividends = ticker.dividends
            latest_dividend_date = dividends.idxmax() if not dividends.empty else None
            formatted_dividend_date = latest_dividend_date.strftime("%d %B %Y") if latest_dividend_date else None
            stock_data['Dividend Date'] = formatted_dividend_date
            stock_data['First Appeared on'] = first_appeared_date
            stock_data['Symbol'] = symbol
            stock_data['52W High'] = stock_data['High'].rolling(window=252, min_periods=1).max()
            stock_data['52W Low'] = stock_data['Low'].rolling(window=252, min_periods=1).min()
            
            return stock_data
        else:
            print(f"No data available for {symbol}.")
            return pd.DataFrame()
    except Exception as e:
        print(f"Failed to download data for {symbol}: {e}")
        return pd.DataFrame()


def calculate_technical_indicators(data):
    if 'Volume' in data.columns and 'Volume_MA_5' not in data.columns:
        data['Volume_MA_5'] = data['Volume'].rolling(window=5, min_periods=1).mean()

    if 'Close' in data.columns and 'RSI' not in data.columns:
        data['RSI'] = talib.RSI(data['Close'], timeperiod=14)

    if 'High' in data.columns and 'Previous_High' not in data.columns:
        data['Previous_High'] = data['High'].shift(1)
    
    data['Day Change %'] = ((data['Close'] - data['Close'].shift(1)) / data['Close'].shift(1)) * 100
    
    return data


def fetch_additional_info_from_csv(symbol, csv_file):
    try:
        df = pd.read_csv(csv_file)
        symbol = symbol.replace(".NS", "")
        row = df[df['Symbol'] == symbol]
        if not row.empty:
            company_name = row.iloc[0]['Company Name']
            industry = row.iloc[0]['Industry']
            return {'Company Name': company_name, 'Industry': industry}
        else:
            print(f"No data found for symbol: {symbol}")
            return {'Company Name': None, 'Industry': None}
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return {'Company Name': None, 'Industry': None}


def scan_stocks_with_additional_info(data, volume_threshold, rsi_threshold, high_threshold, csv_file):
    results = []

    for symbol, stock_data in data.groupby('Symbol'):
        last_row = stock_data.iloc[-1]
        if (
            last_row['Volume'] > last_row['Volume_MA_5'] and
            last_row['RSI'] > rsi_threshold and
            last_row['High'] > last_row['Previous_High'] and
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
    # Daily SMA 20 greater than Daily SMA close 40
    if len(stock_data) < 40:
        return False
    sma_20 = stock_data['Close'].rolling(window=20).mean()
    sma_40 = stock_data['Close'].rolling(window=40).mean()
    if sma_20.iloc[-1] <= sma_40.iloc[-1]:
        return False

    # Daily SMA 40 greater than Daily SMA 60
    sma_60 = stock_data['Close'].rolling(window=60).mean()
    if sma_40.iloc[-1] <= sma_60.iloc[-1]:
        return False

    # Daily close greater than 1 day ago close
    if stock_data['Close'].iloc[-1] <= stock_data['Close'].shift(1).iloc[-1]:
        return False

    # Weekly close greater than Weekly close
    if len(stock_data) < 6:
        return False
    weekly_close = stock_data['Close'].resample('W').last()
    if weekly_close.iloc[-1] <= weekly_close.shift(1).iloc[-1]:
        return False

    # Monthly close greater than Monthly close
    if len(stock_data) < 22:
        return False
    monthly_close = stock_data['Close'].resample('M').last()
    if monthly_close.iloc[-1] <= monthly_close.shift(1).iloc[-1]:
        return False

    return True


@app.route('/get_stock_data', methods=['GET'])
def get_stock_data():
    category = "Nifty 500"
    # symbol = "ABB.NS"
    start_date = '2022-03-20'
    end_date = '2024-03-28'
    volume_threshold = 500000
    rsi_threshold = 60
    high_threshold = 0
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
            data = calculate_technical_indicators(data)
            scanned_stocks = scan_stocks_with_additional_info(data, volume_threshold, rsi_threshold, high_threshold, csv_file_url)

            if not scanned_stocks.empty:
                print("Stocks meeting Scanner 2 conditions:")
                print(scanned_stocks)
                return jsonify(scanned_stocks.to_dict(orient='records'))
            else:
                print("No stocks found.")
                return jsonify({'message': "No stocks found."})
        else:
            print("No data available for the selected category.")
            return jsonify({'message': "No data available for the selected category."})
    else:
        data = fetch_stock_data(symbol, start_date, end_date)
        if not data.empty:
            data = calculate_technical_indicators(data)
            scanned_stocks = scan_stocks_with_additional_info(data, volume_threshold, rsi_threshold, high_threshold, None)

            if not scanned_stocks.empty:
                print("Stocks meeting Scanner 2 conditions:")
                print(scanned_stocks)
                return jsonify(scanned_stocks.to_dict(orient='records'))
            else:
                print("No stocks found.")
                return jsonify({'message': "No stocks found."})
        else:
            print("No data available for the selected symbol.")
            return jsonify({'message': "No data available for the selected symbol."})


if __name__ == "__main__":
    app.run(debug=True)
