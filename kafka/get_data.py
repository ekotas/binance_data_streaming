import websocket
import datetime
from confluent_kafka import Producer
import json

topic = "binance_data_streaming"

# Optional per-message delivery callback (triggered by poll() or flush())
# when a message has been successfully delivered or permanently
# failed delivery (after retries).
def delivery_callback(err, msg):
	if err:
		print('ERROR: Message failed delivery: {}'.format(err))
	else:
		print("Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
			topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))

def on_message(ws, message):
    print()
    print(str(datetime.datetime.now()) + ": ")
    print(message)
    producer.produce(topic, message, callback=delivery_callback)

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

# Create Producer instance
conf = {
	'bootstrap.servers': 'localhost:9092'
}

producer = Producer(conf)

streamKline('bnbusdt', '1m')
