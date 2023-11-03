import random
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

my_dag = DAG(
    dag_id='fork1_dag',
    tags=['tutorial', 'datascientest'],
    schedule_interval=None,
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
    random.seed()
    if random.random() < .9:
        raise Exception('This task randomly failed')


task1 = PythonOperator(
    task_id='task1',
    python_callable=successful_task,
    dag=my_dag
)

task2 = PythonOperator(
    task_id='task2',
    python_callable=failed_task,
    dag=my_dag
)

task3 = PythonOperator(
    task_id='task3',
    python_callable=successful_task,
    dag=my_dag,
    trigger_rule = 'all_done'
)

[task1, task2] >> task3