from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.email_operator import EmailOperator


#Pool e utilizado para prioriza recursos e dar prioridade a uma DAG

with DAG(
    'pool',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False
) as dag:
    
    task1=BashOperator(task_id='tsk1',bash_command='sleep 1', pool='PoolTeste',priority_weight=1)
    task2=BashOperator(task_id='tsk2',bash_command='sleep 1')
    task3=BashOperator(task_id='tsk3',bash_command='sleep 1',pool='PoolTeste',priority_weight=10)
