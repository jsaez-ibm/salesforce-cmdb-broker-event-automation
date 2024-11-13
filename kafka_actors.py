import pykka
from confluent_kafka import Producer, Consumer, KafkaException
import logging
import json
import time

# Kafka SASL SCRAM-SHA-512 configuration
KAFKA_BROKER = 'es-demo-kafka-bootstrap-tools.apps.672ceaf50c7a71b728e5270a.ocp.techzone.ibm.com:443'
KAFKA_USERNAME = 'kafka-connect-user'
KAFKA_PASSWORD = '' # TODO: Provide creds
KAFKA_TOPIC = 'CMDB.EVENTS'

SASL_CONFIG = {
    'bootstrap.servers': KAFKA_BROKER,
    'security.protocol': 'SASL_SSL',  # Use SASL_PLAINTEXT or SASL_SSL based on your setup
    'sasl.mechanism': 'SCRAM-SHA-512',  # Use SCRAM-SHA-512 for authentication
    'sasl.username': KAFKA_USERNAME,
    'sasl.password': KAFKA_PASSWORD,
    # 'group.id': 'kafka-consumer-group',
    # 'auto.offset.reset': 'earliest',  # Adjust depending on your requirements
    'ssl.ca.location': '' # TODO: Provide PEM file
}

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class KafkaProducerActor(pykka.ThreadingActor):
    def __init__(self, producer_config, topic):
        super().__init__()
        self.topic = topic
        self.producer = Producer(producer_config)
        logger.info(f'Kafka Producer actor created for topic {self.topic}')

    def on_receive(self, message):
        if message.get('type') == 'send':
            payload = message.get('payload', {})
            self.send_message(payload)

    def send_message(self, message):
        try:
            payload = message # TODO: Do any necessary data prep for kafka payload to publish
            logger.info(f"Producing message to {self.topic}: {payload}")
            self.producer.produce(self.topic, key=payload.get('key', None), value=json.dumps(payload))
            self.producer.flush()  # Ensure messages are sent
        except KafkaException as e:
            logger.error(f"Error producing message: {e}")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")

class KafkaConsumerActor(pykka.ThreadingActor):
    def __init__(self, consumer_config, topic):
        super().__init__()
        self.topic = topic
        self.consumer = Consumer(consumer_config)
        self.consumer.subscribe([self.topic])
        logger.info(f'Kafka Consumer actor created for topic {self.topic}')

    def on_receive(self, message):
        if message.get('type') == 'consume':
            self.consume_messages()

    def consume_messages(self):
        try:
            msg = self.consumer.poll(timeout=1.0)  # Poll every 1 second
            if msg is None:
                logger.info("No message received within the timeout period")
            elif msg.error():
                logger.error(f"Consumer error: {msg.error()}")
            else:
                logger.info(f"Consumed message: {msg.value().decode('utf-8')}")
        except KafkaException as e:
            logger.error(f"Error consuming message: {e}")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")

    def on_stop(self):
        self.consumer.close()  # Clean up when the actor stops
        logger.info("Kafka Consumer actor stopped.")
