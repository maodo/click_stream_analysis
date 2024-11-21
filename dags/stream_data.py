from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from kafka_operator import KafkaProduceOperator
from datetime import datetime, timedelta

start_date = datetime(2024,11,21)
topic_name = 'clickstream'

with DAG(
    dag_id='data_stream_to_kafka',
    start_date=start_date,
    schedule_interval=timedelta(days=1),
    description='Data Stream to Kafka',
    tags=['kafka','clickstream'],
    default_args={
        'owner': 'maodo',
        'depends_on_past':False,
        'backfill':False,
    }
) as dag:
    start = EmptyOperator(
        task_id='start'
    )
    stream_data = KafkaProduceOperator(
        task_id='stream_data',
        kafka_broker='redpanda:9092',
        kafka_topic=topic_name,
        num_records=100
    )
    end = EmptyOperator(
        task_id='end'
    )