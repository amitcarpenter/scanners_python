from flask import Flask, jsonify, send_file, request
from datetime import datetime, timedelta


from live_scanners.live_scanner1 import live_scanner_01
from live_scanners.live_scanner2 import live_scanner_02
from live_scanners.live_scanner3 import live_scanner_03
from live_scanners.live_scanner4 import live_scanner_04


app = Flask(__name__)


@app.route('/', methods=['GET'])
def get_data_form():
    return send_file('index.html')


@app.route('/api/live_scanner_01', methods=['POST'])
def live_scanner_01_start():
    data = request.json
    print(data)
    index = data.get('index')
    symbol = data.get('symbol')
    volume_threshold = data.get('volume_threshold')
    rsi_threshold = data.get('rsi_threshold')
    close_number = data.get('close_number')
    num_periods = data.get('num_periods')
    
    today = datetime.today().date()
    time_period = request.json.get('time_period')
    value, unit = int(time_period[:-1]), time_period[-1]
    
    start_date = (today - timedelta(days=value)) if unit == 'd' else (today - timedelta(days=value*30))
    end_date = today
    response = live_scanner_01(index, symbol, start_date, end_date, volume_threshold, rsi_threshold, close_number, num_periods)
    return jsonify(response)


@app.route('/api/live_scanner_02', methods=['POST'])
def live_scanner_02_start():
    data = request.json
    print(data)
    category = data.get('category')
    symbol = data.get('symbol')
    rsi_threshold = data.get('rsi_threshold')
    
    today = datetime.today().date()
    time_period = request.json.get('time_period')
    value, unit = int(time_period[:-1]), time_period[-1]
    
    start_date = (today - timedelta(days=value)) if unit == 'd' else (today - timedelta(days=value*30))
    end_date = today
    response = live_scanner_02(category, symbol, start_date, end_date, rsi_threshold)
    return jsonify(response)


@app.route('/api/live_scanner_03', methods=['POST'])
def live_scanner_03_start():
    data = request.json
    print(data)
    category = data.get('category')
    symbol = data.get('symbol')
    volume_threshold = data.get('volume_threshold')
    today = datetime.today().date()
    time_period = request.json.get('time_period')
    value, unit = int(time_period[:-1]), time_period[-1]
    
    start_date = (today - timedelta(days=value)) if unit == 'd' else (today - timedelta(days=value*30))
    end_date = today
    response = live_scanner_03(category, symbol, start_date, end_date, volume_threshold)
    return jsonify(response)


@app.route('/api/live_scanner_04', methods=['POST'])
def live_scanner_04_start():
    data = request.json
    print(data)
    category = data.get('category')
    symbol = data.get('symbol')
    today = datetime.today().date()
    time_period = request.json.get('time_period')
    value, unit = int(time_period[:-1]), time_period[-1]
    
    start_date = (today - timedelta(days=value)) if unit == 'd' else (today - timedelta(days=value*30))
    end_date = today
    
    response = live_scanner_04(category, symbol, start_date, end_date)
    return jsonify(response)


if __name__ == "__main__":
    app.run(debug=True)
