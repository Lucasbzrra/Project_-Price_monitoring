import sys
sys.path.append('/usr/local/airflow/custom_pasta')
from airflow.models import DAG
from airflow import Dataset
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import pandas as pd

mydataset = Dataset('/local do dataset')

with DAG('consumer',
         schedule_interval=[mydataset],
         catchup=False,
         start_date=days_ago(1)


) as dag:

    # Datasets: Agendamento baseado em uma tarefa que atualiza um dataset
    # Basicamente a dag produce (Atualiza os dados)
    def my_file():
        dataset = pd.read_csv('loca da do dataset')
        dataset.to_csv('opt')

    t1=PythonOperator(task_id='t1',python_callable=my_file, dag=dag, outlest[mydataset])

    t1
