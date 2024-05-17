import requests

def get_option_expiration_dates(symbol, api_key):
    endpoint = f'https://www.alphavantage.co/query?function=OPTION_EXPIRATION_DATES&symbol={symbol}&apikey={api_key}'
    
    try:
        response = requests.get(endpoint)
        data = response.json()
        
        if 'error' in data:
            print(f"Error: {data['error']['message']}")
            return None
        
        expiration_dates = data.get('expirationDates')  # Check if the key exists
        if expiration_dates:
            return expiration_dates
        else:
            print("Expiration dates not found in response.")
            print("Full Response:")
            print(data)
            return None
    
    except Exception as e:
        print(f"Error fetching data: {e}")
        return None

# Example usage:
symbol = 'NIFTY'
api_key = 'INVRQDP1D67O2BQ8' 

expiration_dates = get_option_expiration_dates(symbol, api_key)

if expiration_dates:
    print("Expiration Dates:")
    for date in expiration_dates:
        print(date)
