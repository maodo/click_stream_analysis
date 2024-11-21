from airflow.models.baseoperator import BaseOperator
from kafka import KafkaProducer
from airflow.utils.decorators import apply_defaults
import json
from click_stream_data import generate_clickstream_event

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