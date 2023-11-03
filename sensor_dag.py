from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator


my_dag = DAG(
    dag_id='sensor_dag',
    schedule_interval=None,
    start_date=days_ago(0),
    tags=['datscientest','by_me']
)


my_sensor = FileSensor(
    task_id="check_file",
    fs_conn_id="my_filesystem_connection",
    filepath="my_file.txt",
    poke_interval=30,
    dag=my_dag,
    timeout=5 * 30,
    mode='reschedule'
)

my_task = BashOperator(
    task_id="print_file_content",
    bash_command="cat /tmp/my_file.txt",
    dag=my_dag
)

my_task >> my_sensor