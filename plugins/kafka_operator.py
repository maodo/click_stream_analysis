from airflow.models.baseoperator import BaseOperator
from kafka import KafkaProducer
from kafka import KafkaConsumer
from airflow.utils.decorators import apply_defaults
import json
from click_stream_data import generate_clickstream_event
from database import Database

class KafkaProduceOperator(BaseOperator):
    @apply_defaults
    def __init__(self,kafka_broker, kafka_topic, num_records=100, *args, **kwargs):
        super(KafkaProduceOperator, self).__init__(*args, **kwargs)
        self.kafka_broker =kafka_broker
        self.kafka_topic = kafka_topic
        self.num_records = num_records

    def execute(self, context):
        producer = KafkaProducer(
            bootstrap_servers=self.kafka_broker,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        row = 0;
        while row < self.num_records:
            event = generate_clickstream_event()
            producer.send(self.kafka_topic, event)
            row+=1
        # Typically, flush() should be called prior to shutting down the producer 
        # to ensure all outstanding/queued/in-flight messages are delivered.
        producer.flush()
        self.log.info(f'{self.num_records} events has been sent to Kafka topic : {self.kafka_topic}')

class KafkaConsumeOperator(BaseOperator):
    @apply_defaults
    def __init__(self,kafka_broker,kafka_topic, *args, **kwargs):
        super(KafkaConsumeOperator,self).__init__(*args, **kwargs)
        self.kafka_broker = kafka_broker
        self.kafka_topic = kafka_topic
    
    def execute(self, context):
        consumer = KafkaConsumer(
            self.kafka_topic,
            bootstrap_servers= self.kafka_broker,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            consumer_timeout_ms=1000 # Stop iteration after 1 sec
        )
        for message in consumer:
            consumer.commit()
            print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                          message.offset, message.key,
                                          message.value))
            message_value = json.loads(message.value())
            print(f"Mes : {message_value}")
        consumer.close()
        print(f"consumer finished consuming lol !")