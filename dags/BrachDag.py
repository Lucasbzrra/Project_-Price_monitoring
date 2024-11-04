from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.python_operator import PythonOperator
import random

with DAG(
    'branchtest',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False
) as dag:
    
    def random_number():
        return random.randint(1,100)
    
    NumberAleatorioTask = PythonOperator(
        task_id='NumberAleatorioTask',
        python_callable = random_number
    )

    def check_number(**context):
        number = context['task_instance'].xcom_pull(task_ids='NumberAleatorioTask')

        if number % 2 ==0:
            return 'par_task'
        if number % 2!=0:
            return 'impar_task'

    branch_task = BranchPythonOperator(
        task_id='branch_task',
        python_callable=check_number,
        provide_context=True,
    )

    par_task = BashOperator(task_id='par_task',bash_command='echo Número Par')
    impar_task = BashOperator(task_id='impar_task',bash_command='echo Número Impar')


    NumberAleatorioTask >> branch_task
    branch_task >> par_task
    branch_task >> impar_task
