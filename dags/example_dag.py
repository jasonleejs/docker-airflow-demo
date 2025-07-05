from airflow import DAG 
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def say_hello():
    print("Hello from Airflow!")

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(dag_id='hello_airflow',
         default_args=default_args,
         description='A simple hello DAG',
         schedule='* * * * *',
         start_date=datetime(2025,1,1),
         catchup=False,
) as dag:
    task = PythonOperator(
        task_id='say_hello',
        python_callable=say_hello,
    )
    
    task