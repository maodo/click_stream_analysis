from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from kafka_operator import KafkaConsumeOperator
from datetime import datetime, timedelta

start_date = datetime(2024,11,21)
topic_name = 'clickstream'
broker='redpanda:9092'

with DAG(
    dag_id='consume_data_from_kafka',
    start_date=start_date,
    schedule_interval=timedelta(days=1),
    description='Consume Data from Kafka',
    tags=['kafka','clickstream'],
    default_args={
        'owner': 'maodo',
        'depends_on_past':False,
        'backfill':False,
    }
) as dag:
    start=EmptyOperator(task_id='start')
    consume_data = KafkaConsumeOperator(
        task_id='consume_data',
        kafka_broker=broker,
        kafka_topic=topic_name
        )
    end=EmptyOperator(task_id='end')

    start >> consume_data >> end