from airflow.models import DAG
from airflow import Dataset
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hook.postgres import PostgressHook
from airflow.operators.python_operator import PythonOperator


with DAG('hook',
         schedule_interval=None,
         catchup=False,
         start_date=days_ago(1)
) as dag:

    def create_table():
        pg_hook=PostgressHook(postgres_conn_id='postgres')
        pg_hook.run('create table if not exists teste2(id int);', autocommit=True)

    def insert_data():
        pg_hook=PostgressHook(postgres_conn_id='postgres')
        pg_hook.run('insert into teste2 values(1);', autocommit=True)

    def select_data(**kwargs):
        pg_hook=PostgressHook(postgres_conn_id='postgres')
        records = pg_hook.run('select * from teste2;', autocommit=True)
        kwargs['ti'].xcom_push(key='query_result',value=records)

    def print_data(ti):
        task_instance = ti.xcom_pull(key='query_result', task_ids='select_data_task')
        for row in task_instance:
            print(row)

    create_table_task = PythonOperator(task_id='create_table_task',
                                       python_callable='create_table')
    
    create_table_task = PythonOperator(task_id='insert_data_task',
                                       python_callable='insert_data')
    
    select_task = PythonOperator(task_id='select_data_task',
                                       python_callable='select_data')