from airflow.models.baseoperator import BaseOperator
from kafka import KafkaProducer
from kafka import KafkaConsumer
from airflow.utils.decorators import apply_defaults
import json
from click_stream_data import generate_clickstream_event
from database import Database
from psycopg2 import sql



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
    
    def insert_clickstream(self,db,message):
       
        query = sql.SQL("""
            INSERT INTO events (
                user_id, session_id, timestamp, event_type,
                url, referrer_url, user_agent, device_type, location
            )
            VALUES (
                %(user_id)s, %(session_id)s, %(timestamp)s, %(event_type)s,
                %(url)s, %(referrer_url)s, %(user_agent)s, %(device_type)s, %(location)s
            );
        """)
        db.connect()
        with db.conn.cursor() as cursor:
            cursor.execute(query, message)
            db.conn.commit()
            print(f"Events inserted : {message['user_id']}")
    def execute(self, context):
        consumer = KafkaConsumer(
            self.kafka_topic,
            bootstrap_servers= self.kafka_broker,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            group_id='event-consumers',
            consumer_timeout_ms=1000 # Stop iteration after 1 sec
        )
        db_config = {
            "host":"postgres-db", # Docker service name
            "port":5432, # docker internal port
            "dbname":"clickstream",
            "user":"maodo",
            "password":"pass123"
            }
        db = Database(**db_config)
        db.connect()
        for message in consumer:
            consumer.commit()
            print ("value=%s" % ( message.value))
            data = message.value
            self.insert_clickstream(db,data)
        consumer.close()
        db.close()
        print(f"consumer finished consuming lol !")