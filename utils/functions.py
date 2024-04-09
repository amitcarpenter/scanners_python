import pandas as pd
import yfinance as yf


def get_symbols_from_csv(csv_url):
    try:
        df = pd.read_csv(csv_url)
        return list(df['Symbol'] + ".NS")
    except Exception as e:
        print(f"Error reading CSV from {csv_url}: {e}")
        return []


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
