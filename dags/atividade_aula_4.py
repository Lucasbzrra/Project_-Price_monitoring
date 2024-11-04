from airflow.models import DAG
from airflow.operators.python import PythonOperator
import pendulum

with DAG(
    'Desafio',
    start_date=pendulum.today('UTC').add(days=-1),
    schedule_interval='@daily'
) as dag:
    
    def cumprimentos():
        print('Boas-vindas ao Airflow!')

    task=PythonOperator(task_id='cumprimentos',
                        python_callable=cumprimentos)
