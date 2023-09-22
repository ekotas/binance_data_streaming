import websocket
import datetime
from confluent_kafka import Producer
import json
import time

test_json = json.loads(
'''
{
    "e":"aggTrade",
    "E":1695311088498,
    "s":"SOLUSDT",
    "a":254508441,
    "p":"19.54000000",
    "q":"1.00000000",
    "f":378965630,
    "l":378965630,
    "T":1695311088497,
    "m":true,
    "M":true
}
'''
)

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
    time.sleep(30) # Since my computer can't handle GBs of data  

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
topic = "my_project_topic"

streamKline('solusdt', '1m')