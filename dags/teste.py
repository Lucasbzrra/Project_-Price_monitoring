from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 8),
    'retries': 1,
}

with DAG('load_parquet_with_fdw', default_args=default_args, schedule_interval='@daily') as dag:

    # 1. Criar a extensão parquet_fdw
    create_extension = PostgresOperator(
        task_id='create_parquet_fdw_extension',
        postgres_conn_id='postgres-airflow',
        sql='CREATE EXTENSION IF NOT EXISTS parquet_fdw;'
    )

    # 2. Criar o servidor FDW
    create_fdw_server = PostgresOperator(
        task_id='create_fdw_server',
        postgres_conn_id='postgres-airflow',
        sql='CREATE SERVER IF NOT EXISTS parquet_srv FOREIGN DATA WRAPPER parquet_fdw;'
    )

    # 3. Criar o mapeamento do usuário
    create_user_mapping = PostgresOperator(
        task_id='create_user_mapping',
        postgres_conn_id='postgres-airflow',
        sql='CREATE USER MAPPING IF NOT EXISTS FOR postgres SERVER parquet_srv OPTIONS (user ''postgres'');'
    )

    # 4. Criar a tabela externa
    create_foreign_table = PostgresOperator(
        task_id='create_foreign_table',
        postgres_conn_id='postgres-airflow',
        sql="""
        CREATE FOREIGN TABLE IF NOT EXISTS public.tb_dim_site (
            id_site int,
            name_site varchar(255),
            ...
        )
        SERVER parquet_srv
        OPTIONS (
            filename '/opt/airflow/datalake/silverzone/pichau_2024-10-08/dim_site.parquet',
            format 'parquet'
        );
        """
    )

    # 5. Executar a carga de dados (opcional)
    load_data = PostgresOperator(
        task_id='load_data_to_postgres',
        postgres_conn_id='postgres-airflow',
        sql='INSERT INTO public.tb_dim_site SELECT * FROM public.tb_dim_site;'
    )

    # Definindo a ordem das tarefas
    create_extension >> create_fdw_server >> create_user_mapping >> create_foreign_table >> load_data
