from flask import Flask, jsonify, send_file, request
from datetime import datetime, timedelta


from live_scanners.live_scanner1 import live_scanner_01

app = Flask(__name__)


@app.route('/get_data_form', methods=['GET'])
def get_data_form():
    return send_file('index.html')


@app.route('/api/live_scanner_01', methods=['POST'])
def live_scanner_01_start():
    data = request.json
    print(data)
    category = data.get('category')
    symbol = data.get('symbol')
    volume_threshold = data.get('volume_threshold')
    rsi_threshold = data.get('rsi_threshold')
    
    today = datetime.today().date()
    time_period = request.json.get('time_period')
    value, unit = int(time_period[:-1]), time_period[-1]
    start_date = (today - timedelta(days=value)) if unit == 'd' else (today - timedelta(days=value*30))
    end_date = today
    response = live_scanner_01(category, symbol, start_date, end_date, volume_threshold, rsi_threshold)
    return jsonify(response)



if __name__ == "__main__":
    app.run(debug=True)
