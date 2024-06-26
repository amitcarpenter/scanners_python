from flask import Flask, jsonify, send_file, request
from datetime import datetime, timedelta


from live_scanners.live_scanner1 import live_scanner_01
from live_scanners.live_scanner2 import live_scanner_02
from live_scanners.live_scanner3 import live_scanner_03
from live_scanners.live_scanner4 import live_scanner_04
# from eod_scanners.eod_scanner1 import eod_scanner_01
# from eod_scanners.eod_scanner2 import eod_scanner_02


app = Flask(__name__)


@app.route('/python/', methods=['GET'])
def get_data_form():
    return send_file('index.html')


#  Live Scanner
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

    if unit == 'd':
        start_date = today - timedelta(days=value)
        end_date = today
    else:
        start_date = today - timedelta(days=value*30)
        end_date = today
    end_date = today
    
    response = live_scanner_01(index, symbol, start_date, end_date, volume_threshold, rsi_threshold, close_number, num_periods)
    return jsonify(response)


@app.route('/api/live_scanner_02', methods=['POST'])
def live_scanner_02_start():
    data = request.json
    print(data)
    index = data.get('index')
    symbol = data.get('symbol')
    volume_threshold = data.get('volume_threshold')
    
    today = datetime.today().date()
    time_period = request.json.get('time_period')
    value, unit = int(time_period[:-1]), time_period[-1]

    if unit == 'd':
        start_date = today - timedelta(days=value)
        end_date = today
    else:
        start_date = today - timedelta(days=value*30)
        end_date = today
    end_date = today
    
    
    
    response = live_scanner_02(index, symbol, start_date, end_date, volume_threshold)
    return jsonify(response)


@app.route('/api/live_scanner_03', methods=['POST'])
def live_scanner_03_start():
    data = request.json
    print(data)
    index = data.get('index')
    symbol = data.get('symbol')
    volume_threshold = data.get('volume_threshold')
    
    today = datetime.today().date()
    time_period = request.json.get('time_period')
    value, unit = int(time_period[:-1]), time_period[-1]

    if unit == 'd':
        start_date = today - timedelta(days=value)
        end_date = today
    else:
        start_date = today - timedelta(days=value*30)
        end_date = today
    end_date = today
    
    print(start_date , end_date)
    response = live_scanner_03(index, symbol, start_date, end_date, volume_threshold)
    return jsonify(response)


@app.route('/api/live_scanner_04', methods=['POST'])
def live_scanner_04_start():
    data = request.json
    print(data)
    index = data.get('index')
    symbol = data.get('symbol')
    volume_threshold = int(data.get('volume_threshold'))
    
    today = datetime.today().date()
    time_period = request.json.get('time_period')
    value, unit = int(time_period[:-1]), time_period[-1]

    if unit == 'd':
        start_date = today - timedelta(days=value)
        end_date = today
    else:
        start_date = today - timedelta(days=value*30)
        end_date = today
    end_date = today
    
    response = live_scanner_04(index, symbol, start_date, end_date, volume_threshold)
    return jsonify(response)


# EOD Scanner (end of the day)
# @app.route('/api/eod_scanner_01', methods=['POST'])
# def eod_scanner_01_start():
#     data = request.json
#     print(data)
#     index = data.get('index')
#     symbol = data.get('symbol')
#     volume_threshold = int(data.get('volume_threshold'))
    
#     today = datetime.today().date()
#     time_period = request.json.get('time_period')
#     value, unit = int(time_period[:-1]), time_period[-1]

#     if unit == 'd':
#         start_date = today - timedelta(days=value)
#         end_date = today
#     else:
#         start_date = today - timedelta(days=value*30)
#         end_date = today
#     end_date = today
    
#     response = eod_scanner_01(index, symbol, start_date, end_date, volume_threshold)
#     return jsonify(response)


if __name__ == "__main__":
    app.run(debug=False)
