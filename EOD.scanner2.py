import pandas as pd
import yfinance as yf
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
    
    
def calculate_vcp_pattern(data):
    # Calculate 50-day moving average of volume
    data['Volume_MA_50'] = data['Volume'].rolling(window=50).mean()
    
    # Calculate 10-day moving average of close price
    data['Close_MA_10'] = data['Close'].rolling(window=10).mean()
    
    # Calculate 30-day moving average of close price
    data['Close_MA_30'] = data['Close'].rolling(window=30).mean()
    
    # Condition 1: Current close price > 30-day moving average of close price
    condition_1 = data['Close'] > data['Close_MA_30']
    
    # Condition 2: Current volume > 50-day moving average of volume
    condition_2 = data['Volume'] > data['Volume_MA_50']
    
    # Condition 3: Current close price < 10-day moving average of close price
    condition_3 = data['Close'] < data['Close_MA_10']
    
    # Combine conditions
    vcp_pattern = condition_1 & condition_2 & condition_3
    
    data['Day Change %'] = ((data['Close'] - data['Close'].shift(1)) / data['Close'].shift(1)) * 100
    
    return vcp_pattern


def calculate_relative_strength(data):
    # Calculate Relative Strength
    data['Relative Strength'] = data['Close'] / data['Close'].shift(1)
    return data


def calculate_rsi(data, window=14):
    delta = data['Close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()

    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi


def scan_stocks(data):
    data['RSI'] = calculate_rsi(data)
    # Filter stocks based on VCP pattern with relative strength above 1
    data = calculate_relative_strength(data)
    vcp_pattern = calculate_vcp_pattern(data)
    filtered_stocks = data[vcp_pattern & (data['Relative Strength'] > 1)]
    return filtered_stocks


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


@app.route('/get_eod_scanner_02', methods=['GET'])
def get_eod_scanner_02():
    category = "Nifty 50"
    start_date = '2022-03-20'
    end_date = '2024-03-28'
    csv_file_url = csv_urls.get(category)
    
    if csv_file_url is None:
        return jsonify({'message': f"No CSV URL found for category: {category}."})

    symbols = get_symbols_from_csv(csv_file_url)
    if not symbols:
        return jsonify({'message': f"No symbols found for category: {category}."})

    data = pd.DataFrame()
    for symbol in symbols:
        stock_data = fetch_stock_data(symbol)
        additional_info = fetch_additional_info_from_csv(symbol, csv_file_url)
        stock_data = yf.download(symbol, start=start_date, end=end_date)
        if not stock_data.empty:
            ticker = yf.Ticker(symbol)
            hist = ticker.history(period="max")
            first_appeared_date = hist.index[0].strftime("%d %B %Y")
            dividends = ticker.dividends
            latest_dividend_date = dividends.idxmax() if not dividends.empty else None
            formatted_dividend_date = latest_dividend_date.strftime("%d %B %Y") if latest_dividend_date else None
            
        if not stock_data.empty:
            high_52 = stock_data['High'].rolling(window=252, min_periods=1).max().iloc[-1]
            l_52 = stock_data['Low'].rolling(window=252, min_periods=1).min().iloc[-1]
                    
        if not stock_data.empty:
            data = pd.concat([data, stock_data], axis=0)

            
        if not stock_data.empty:
            data = pd.concat([data, stock_data], axis=0)
        
    if not data.empty:
        scanned_stocks = scan_stocks(data)
        
        if not scanned_stocks.empty:
            formatted_data = []
            for symbol, row in scanned_stocks.iterrows():
                formatted_row = {
                    'Symbol': symbol,
                    'Stock Name': additional_info['Company Name'],
                    'Sector': additional_info['Industry'],
                    'LTP': row['Close'],
                    'Volume': row['Volume'],
                    '52W High': high_52.tolist(), 
                    '52W Low': l_52.tolist(),
                    'RSI': row['RSI'],
                    'Day Change %': row['Day Change %'],
                    'First Appeared on': formatted_dividend_date,
                    'Dividend Date': first_appeared_date
                            
                }
                formatted_data.append(formatted_row)
            return jsonify(formatted_data)
        else:
            return jsonify({'message': "No stocks found."})
    else:
        return jsonify({'message': "No data available for the selected category."})


if __name__ == "__main__":
    app.run(debug=True)
