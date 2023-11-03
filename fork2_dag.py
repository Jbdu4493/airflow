import random
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
import datetime

my_dag = DAG(
    dag_id='fork2_dag',
    tags=['tutorial', 'datascientest'],
    schedule_interval=datetime.timedelta(seconds=10),
    default_args={
        'owner': 'airflow',
        'start_date': days_ago(0, minute=1)
    },
    catchup=False
)


def successful_task():
    print('success')


def failed_task():
    raise Exception('This task did not work!')


def random_fail_task():
    a = random.choice([0,1]) 
    print(a)
    if a == 1:
        raise Exception('This task randomly failed')


task1 = PythonOperator(
    task_id='task1',
    python_callable=random_fail_task,
    dag=my_dag
)

task2 = PythonOperator(
    task_id='task2',
    python_callable=successful_task,
    dag=my_dag,
    trigger_rule='all_failed'
)

task3 = PythonOperator(
    task_id='task3',
    python_callable=successful_task,
    dag=my_dag,
    trigger_rule='all_success'
)

task4 = PythonOperator(
    task_id='task4',
    python_callable=successful_task,
    dag=my_dag,
    trigger_rule='all_done'
)

task1 >> [task2, task3]
[task2, task3] >> task4