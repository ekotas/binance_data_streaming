import websocket
import datetime
from classes.kafka_producer import KafkaInstance

topic = "binance_data_streaming"

kafka_instance = KafkaInstance()
producer = kafka_instance.create_producer()

def on_message(ws, message):
    print()
    print(str(datetime.datetime.now()) + ": ")
    print(message)
    kafka_instance.produce_data(topic, message)

def on_error(ws, error):
    print(error) 

def on_close(close_msg):
    print("### closed ###" + close_msg)

def streamKline(symbol, interval):
    websocket.enableTrace(False)
    socket = f'wss://data-stream.binance.vision/ws/{symbol}@aggTrade'
    print(socket)
    ws = websocket.WebSocketApp(socket,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)
    
    ws.run_forever()

streamKline('bnbusdt', '1m')