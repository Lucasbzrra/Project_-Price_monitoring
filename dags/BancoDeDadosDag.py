from airflow.models import DAG
from airflow import Dataset
from airflow.utils.dates import days_ago
from airflow.provider.postgres.operator.postgress import PostgresOperator
from airflow.operators.python_operator import PythonOperator



### Utilizando PROVIDERS (Modulos python que estendem a funcionalidade do airflow)


def print_result(ti):
    task_instance= ti.xcom_pull(task_ids='query_data')
    for row in task_instance:
        print(row)

with DAG('bancodedados',
         schedule_interval=None,
         catchup=False,
         start_date=days_ago(1)
) as dag:


    cretae_table= PostgresOperator(task_id='create_tagble',
                                postegres_conn_id='postgress',
                                sql='create table if not exists teste(id int)')

    insert_data = PostgresOperator(task_id='insert_data',
                                postegres_conn_id='postgress',
                                sql='insert into teste values(1)')

    query_data = PostgresOperator(task_id='query_data',
                                postegres_conn_id='postgress',
                                sql='select * from teste')

    print_result_task = PythonOperator(
    task_id='print_result_task',
    python_callable=print_result,
    provide_context=True
    )

cretae_table>> insert_data >> query_data