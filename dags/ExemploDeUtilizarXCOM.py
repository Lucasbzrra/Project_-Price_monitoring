import sys
sys.path.append('/usr/local/airflow/custom_pasta')
from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator


with DAG('exemploXCOM',
         schedule_interval=None

) as dag:
    
    # Somente utilizar XCOM para trocar pequenas informações sobre as task
    def task_write(**kwarg):
        kwarg['ti'].xcom_push(key='valorxcom1',value=10200) #<=== Defini oq vai retornar

    def task_read(**kwarg):
        valor = kwarg['ti'].xcom_pull(key='valorxcom1') #<==== Recuperar o valor do xcom
        print(f'valor recuperado: {valor}')

    task2=PythonOperator(task_id='tsk2',python_callable=task_read, dag=dag)

    task2