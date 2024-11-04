from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 9, 22),
    'retries': 1,
}

with DAG('spark_job_dag',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    submit_spark_job = SparkSubmitOperator(
        task_id='submit_spark_job',
        application='/opt/bitnami/spark/src/scripts_spark/WebScraping_Transformation.py',  # Ajuste para o caminho correto
        conn_id='spark_default',
        deploy_mode='client',
        executor_memory='1G',
        num_executors=2,
        driver_memory='512M', # Use 'spark://spark:7077' ou o nome do container
    )

    submit_spark_job
