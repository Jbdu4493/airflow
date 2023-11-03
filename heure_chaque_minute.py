from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
import datetime

my_dag = DAG(
    dag_id='minutely_dag_rattrapage',
    description='My first DAG created with DataScientest',
    tags=['tutorial', 'datascientest'],
    schedule_interval="* * * * *",
    catchup=False,
    default_args={
        'owner': 'airflow',
        'start_date': days_ago(0,minute=2),
    }
)

def print_hour():
    print("Il est: ", datetime.datetime.now())


my_task = PythonOperator(
    task_id="heure",
    python_callable=print_hour,
    dag=my_dag
)