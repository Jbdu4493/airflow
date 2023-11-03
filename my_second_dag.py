from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
import datetime
import time

my_second_dag = DAG(
    dag_id='my_second_dag',
    description='My second DAG created alone',
    tags=['tutorial',"by_me"],
    schedule_interval=None,
    default_args={
        'owner': 'airflow',
        'start_date': days_ago(2),
    }
)

# definition of the function to execute
def print_hello_task1():
    time.sleep(30)
    int('AZS')
    print('Hello from tack1')

def print_hello_task2():
    print('Hello from tack2')



my_task1 = PythonOperator(
    task_id='second_dag_task1',
    python_callable=print_hello_task1,
    dag=my_second_dag
)

my_task2 = PythonOperator(
    task_id='second_dag_task2',
    python_callable=print_hello_task2,
    dag=my_second_dag
)

my_task1 >> my_task2