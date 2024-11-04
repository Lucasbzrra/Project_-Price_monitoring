from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator

with DAG(
    'dummy',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False
) as dag:
    
    task1=BashOperator(task_id='task1',bash_command='sleep 1', dag=dag)
    task2=BashOperator(task_id='task2',bash_command='sleep 1', dag=dag)
    task3=BashOperator(task_id='task3',bash_command='sleep 1', dag=dag)
    task4=BashOperator(task_id='task4',bash_command='sleep 1', dag=dag)
    task5=BashOperator(task_id='task5',bash_command='sleep 5')

    taskdummy = DummyOperator(task_id='taskdummy')

    [task1,task2,task3]>> taskdummy >> [task4,task5]