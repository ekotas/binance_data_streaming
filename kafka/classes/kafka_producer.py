from confluent_kafka import Producer

class KafkaInstance:
	def __init__(self):
		self.producer = None
		self.conf = {
			'bootstrap.servers': 'localhost:9092'
		}
    
	# Method for creating producer instance
	def create_producer(self):
		self.producer = Producer(self.conf)
		return self.producer

	# Optional per-message delivery callback (triggered by poll() or flush())
	# when a message has been successfully delivered or permanently
	# failed delivery (after retries).
	def delivery_callback(self, err, msg):
		if err:
			print('ERROR: Message failed delivery: {}'.format(err))
		else:
			print("Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
				topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))
			
	# Method for sending data to Kafka
	def produce_data(self, topic, message):
		print('pusing data')
		self.producer.produce(topic, message, callback=self.delivery_callback)