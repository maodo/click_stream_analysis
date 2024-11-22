from database import Database
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


# Use the docker information 
db_config = {
    "host":"postgres-db", # Docker service name
    "port":5432, # docker internal port
    "dbname":"clickstream",
    "user":"maodo",
    "password":"pass123"
}
db = Database(**db_config)

def create_events_table():
    create_table_query = """
    CREATE TABLE IF NOT EXISTS events(
        user_id VARCHAR(40),
        session_id VARCHAR(40),
        timestamp VARCHAR(255),
        event_type VARCHAR(15),
        url VARCHAR(40),
        referrer_url VARCHAR(40),
        user_agent VARCHAR(40),
        device_type VARCHAR(40),
        location VARCHAR(40)
    )
    """
    db.connect()
    cursor = db.conn.cursor()
    cursor.execute(create_table_query)
    db.conn.commit()
    print(f"Created events table !")
    db.close()
with DAG(
    dag_id='events_table_creation',
    schedule_interval=timedelta(days=1),
    description="Events table creation DAG",
    start_date=datetime(2024,11,22),
    default_args={
        'owner':'maodo',
        'depends_on_past': False,
        'backfill': False
    }
) as dag:
    start = EmptyOperator(task_id='start')
    create_events_table = PythonOperator(
        task_id='create_events_table',
        python_callable=create_events_table
        )
    end = EmptyOperator(task_id='end')

    start >> create_events_table >> end