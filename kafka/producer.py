import sys
from random import choice
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Producer
import json

def json_serializer(obj):
	return json.dumps(obj).encode('ascii')

# Create Producer instance
сonfig = {
	'bootstrap.servers': 'localhost:9092'
}
producer = Producer(сonfig)

my_json = '''
{
	"ordertime": 1497014222380,
	"orderid": 18,
	"itemid": "Item_184",
	"address": {
		"city": "Mountain View",
		"state": "CA",
		"zipcode": 94041
	}
}
'''     


# Optional per-message delivery callback (triggered by poll() or flush())
# when a message has been successfully delivered or permanently
# failed delivery (after retries).
def delivery_callback(err, msg):
	if err:
		print('ERROR: Message failed delivery: {}'.format(err))
	else:
		print("Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
			topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))

# Produce data by selecting random values from these lists.
topic = "purchases"
user_ids = ['eabara', 'jsmith', 'sgarcia', 'jbernard', 'htanaka', 'awalther']
products = ['book', 'alarm clock', 't-shirts', 'gift card', 'batteries']

count = 0
for _ in range(10):

	user_id = choice(user_ids)
	product = choice(products)
	producer.produce(topic, json_serializer(my_json), user_id, callback=delivery_callback)
	count += 1

# Block until the messages are sent.
producer.poll(10000)
producer.flush()

