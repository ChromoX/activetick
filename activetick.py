from multiprocessing import Process, Queue
from datetime import datetime, timedelta
from collections import defaultdict
from scipy.stats import mode
from utils import TwoWay
import pandas
import asyncore
import socket
import requests

BEGINTIME_DELAY = int(365*2)

BAR_DATE_FORMAT = "%Y%m%d%H%M%S"
TICK_DATE_FORMAT = "%Y%m%d%H%M%S%f"
TICK_TRADE_FIELDS = TwoWay({
    'LastPrice': 2,
    'LastSize': 3,
    'LastExchange': 4,
    'Condition 1': 5,
    'Condition 2': 6,
    'Condition 3': 7,
    'Condition 4': 8
})
TICK_QUOTE_FIELDS = TwoWay({
    'BidPrice': 2,
    'AskPrice': 3,
    'BidSize': 4,
    'AskSize': 5,
    'BidExchange': 6,
    'AskExchange': 7,
    'Condition': 8
})
STREAM_TRADE_FIELDS = TwoWay({
    'TradeFlags': 2,
    'Condition 1': 3,
    'Condition 2': 4,
    'Condition 3': 5,
    'Condition 4': 6,
    'LastExchange': 7,
    'LastPrice': 8,
    'LastSize': 9,
    'LastDateTime': 10
})
STREAM_QUOTE_FIELDS = TwoWay({
    'QuoteCondition': 2,
    'BidExchange': 3,
    'AskExchange': 4,
    'BidPrice': 5,
    'AskPrice': 6,
    'BidSize': 7,
    'AskSize': 8,
    'QuoteDateTime': 9
})
QUOTE_DATA_URI = "http://localhost:5000/quoteData?symbol=%s&field=%s"
BAR_DATA_URI = "http://localhost:5000/barData?symbol=%s&historyType=%s&intradayMinutes=%s&beginTime=%s&endTime=%s"
TICK_DATA_URI = "http://localhost:5000/tickData?symbol=%s&trades=%s&quotes=%s&beginTime=%s&endTime=%s"
OPTION_CHAIN_URI = "http://localhost:5000/optionChain?symbol=%s"
QUOTE_STREAM_URI = "http://localhost:5000/quoteStream?symbol=%s"

symbolStatus_mapping = {
    'Success': 1,
    'Invalid': 2,
    'Unavailable': 3,
    'NoPermission': 4
}

quoteField_mapping = TwoWay({
    'Symbol': '1',
    'OpenPrice': '2',
    'PreviousClosePrice': '3',
    'ClosePrice': '4',
    'LastPrice': '5',
    'BidPrice': '6',
    'AskPrice': '7',
    'HighPrice': '8',
    'LowPrice': '9',
    'DayHighPrice': '10',
    'DayLowPrice': '11',
    'PreMarketOpenPrice': '12',
    'ExtendedHoursLastPrice': '13',
    'AfterMarketClosePrice': '14',
    'BidExchange': '15',
    'AskExchange': '16',
    'LastExchange': '17',
    'LastCondition': '18',
    'Condition': '19',
    'LastTradeDateTime': '20',
    'LastDateTime': '21',
    'DayHighDateTime': '22',
    'DayLowDateTime': '23',
    'LastSize': '24',
    'BidSize': '25',
    'AskSize': '26',
    'Volume': '27',
    'PreMarketVolume': '28',
    'AfterMarketVolume': '29',
    'TradeCount': '30',
    'PreMarketTradeCount': '31',
    'AfterMarketTradeCount': '32',
    'FundamentalEquityName': '33',
    'FundamentalEquityPrimaryExchange': '34'
})


class QuoteStream(asyncore.dispatcher):

    def __init__(self, symbols, output):
        self.output = output
        asyncore.dispatcher.__init__(self)
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connect(('127.0.0.1', 5000))
        self.buffer = 'GET %s HTTP/1.0\r\n\r\n' % ('/quoteStream?symbol=' + '+'.join(symbols))
        asyncore.loop()

    def handle_connect(self):
        pass

    def handle_close(self):
        self.close()

    def handle_read(self):
        raw_stream = self.recv(8192)
        raw_stream = raw_stream.split('\n')
        raw_stream = filter(lambda x: False if '\r' in x or x == '' else True, raw_stream)
        parsed_ticks = map(parse_stream_line, raw_stream)
        map(self.output.put, parsed_ticks)

    def writable(self):
        return (len(self.buffer) > 0)

    def handle_write(self):
        sent = self.send(self.buffer)
        self.buffer = self.buffer[sent:]


def parse_quoteData_line(line):
    line = line.split(',')
    symbol = line[0]
    symbol_status = line[1]

    if int(symbol_status) != 1:
        print("Couldn't Parse Line:", symbol_status)
        return None

    fields = map(lambda x: quoteField_mapping[x], line[2::4])
    ## fields_status = line[3::4]
    ## fields_type = line[4::4]
    fields_data = line[5::4]

    parsed_line = dict(zip(fields, fields_data))
    parsed_line['symbol'] = symbol

    return parsed_line


def parse_barData_line(line):
    line = line.split(',')
    try:
        obj = {'ts': datetime.strptime(line[0], BAR_DATE_FORMAT),
               'o': float(line[1]),
               'h': float(line[2]),
               'l': float(line[3]),
               'c': float(line[4]),
               'v': int(line[5])
               }
    except:
        obj = ''

    return obj


def parse_tickData_line(line):
    line = line.split(',')

    tick_type = line[0]
    ts = datetime.strptime(line[1], TICK_DATE_FORMAT)

    parsed_object = {'type': tick_type, 'ts': ts}

    if tick_type == 'T':
        for i, field in enumerate(line[2:]):
            parsed_object[TICK_TRADE_FIELDS[i + 2]] = field
    elif tick_type == 'Q':
        for i, field in enumerate(line[2:]):
            parsed_object[TICK_QUOTE_FIELDS[i + 2]] = field
    else:
        print("Unable to parse tickData line:", line)
        return None

    return parsed_object


def parse_stream_line(line):
    line = line.split(',')

    tick_type = line[0]
    symbol = line[1]

    parsed_object = {'symbol': symbol, 'type': tick_type}

    if tick_type == 'T':
        parsed_object['ts'] = datetime.strptime(line[STREAM_TRADE_FIELDS['LastDateTime']], TICK_DATE_FORMAT)
        for i, field in enumerate(line[2:]):
            parsed_object[STREAM_TRADE_FIELDS[i + 2]] = field
    elif tick_type == 'Q':
        parsed_object['ts'] = datetime.strptime(line[STREAM_QUOTE_FIELDS['QuoteDateTime']], TICK_DATE_FORMAT)
        for i, field in enumerate(line[2:]):
            parsed_object[STREAM_QUOTE_FIELDS[i + 2]] = field
    else:
        print("Unable to parse stream line:", line)
        return None

    return parsed_object


def parse_request(request, parse_func, stream=False):
    if request.status_code != 200:
        print("Error:", request.status_code)
        return None

    raw_data = request.text.split('\r\n')
    raw_data = filter(lambda s: True if s != '' else False, raw_data)
    parsed_objects = map(parse_func, raw_data)

    if stream:
        return parsed_objects
    else:
        return request_to_dataframe(parsed_objects)


def request_to_dataframe(objs, normalize=False):
    ts_list = []
    data_dict = defaultdict(list)

    for row in objs:
        try:
            ts_list.append(row['ts'])
            data_dict['open'].append(row['o'])
            data_dict['close'].append(row['c'])
            data_dict['low'].append(row['l'])
            data_dict['high'].append(row['h'])
            data_dict['volume'].append(row['v'])
        except:
            print("No Data")
            return pandas.DataFrame()

    if normalize:
        ## Find most often dt
        time_deltas = []
        for i in range(1, len(ts_list)):
            time_deltas.append(ts_list[i] - ts_list[i - 1])
        most_common_dt = mode(time_deltas)[0][0]
        if most_common_dt.days > 0:
            pandas_dt = str(most_common_dt.days) + 'B'
        else:
            pandas_dt = str(most_common_dt.seconds) + 'S'
        dr = pandas.date_range(datetime(ts_list[0].year, ts_list[0].month, ts_list[0].day, 9, 30),
                               datetime(ts_list[0].year, ts_list[0].month, ts_list[0].day, 16), freq=pandas_dt)
        return pandas.DataFrame(data_dict, index=ts_list).reindex(index=dr, method='ffill')
    else:
        return pandas.DataFrame(data_dict, index=ts_list)


def quoteData(symbols, fields, stream=False):
    ## Feature Mapping
    mapped_fields = map(lambda x: quoteField_mapping[x], fields)
    ## Make Request
    r = requests.get(QUOTE_DATA_URI % ('+'.join(symbols), '+'.join(mapped_fields)))

    return parse_request(r, parse_quoteData_line, stream)


def barData(symbol,
            beginTime=datetime.now() - timedelta(BEGINTIME_DELAY),
            endTime=datetime.now(), dt='D', stream=False):
    if beginTime is None:
        beginTime = datetime.now() - timedelta(BEGINTIME_DELAY)
    if endTime is None:
        endTime = datetime.now()

    ## Parse dt
    intradayMinutes = '1'
    if dt == 'D':
        dt = '1'
    elif dt == 'W':
        dt = '2'
    else:
        intradayMinutes = dt
        dt = '0'

    ## Make Request
    r = requests.get(BAR_DATA_URI %
                    (symbol, dt, intradayMinutes,
                        beginTime.strftime(BAR_DATE_FORMAT),
                        endTime.strftime(BAR_DATE_FORMAT)))

    return parse_request(r, parse_barData_line, stream)


def tickData(symbol, trades=True, quotes=True,
             beginTime=datetime.now() - timedelta(BEGINTIME_DELAY),
             endTime=datetime.now(), stream=False):

    r = requests.get(TICK_DATA_URI % (symbol, '1' if trades else '0',
                                      '1' if quotes else '0',
                                      beginTime.strftime(TICK_DATE_FORMAT),
                                      endTime.strftime(TICK_DATE_FORMAT)))

    return parse_request(r, parse_tickData_line)


def optionChain(symbol, stream=False):
    ##### DEFUNKT
    r = requests.get(OPTION_CHAIN_URI % (symbol))

    if r.status_code != 200:
        print("Error:", r.status_code)
        return None

    ## Parse Data
    print(r.text)
    raw_data = r.text.split('\r\n')
    raw_data = ifilter(lambda s: True if s != '' else False, raw_data)
    if 'OPTION:' in symbol:
        pass
    else:
        parsed_objects = raw_data

    if stream:
        return parsed_objects
    else:
        return list(parsed_objects)


def main():
    q = Queue(10000)
    qs = Process(target=QuoteStream, args=(['AAPL', 'GOOG'], q))
    qs.start()


if __name__ == '__main__':
    main()
