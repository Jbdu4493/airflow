from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import random

my_dag = DAG(
    dag_id='simple_xcom_dag',
    schedule_interval=None,
    start_date=days_ago(0),
     tags=['datascientest',"by_me"],
)


def function_with_return(task_instance):
    task_instance.xcom_push(
        key="my_xcom_value",
        value={"name":"Jonathan","age":19.}
    )


def get_value(task_instance):
    data = task_instance.xcom_pull(
            key="my_xcom_value",
            task_ids="python_task1"
        )
    print("#######",data['name'],data['age'])


my_task1 = PythonOperator(
    task_id='python_task1',
    dag=my_dag,
    python_callable=function_with_return
)

my_task2 = PythonOperator(
    task_id='python_task2',
    dag=my_dag,
    python_callable=get_value
)
my_task1 >> my_task2
