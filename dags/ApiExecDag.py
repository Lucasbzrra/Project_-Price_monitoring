import sys
sys.path.append('/usr/local/airflow/custom_pasta')
from airflow.models import DAG
from datetime import datetime, timedelta
from operators.OperatorApiExterna import ApiExternaOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit  import SparkSubmitOperator
import os
from airflow.utils.dates import days_ago

with DAG(dag_id="ApiExec",start_date=days_ago(2),schedule_interval='@daily') as dag:

    TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"
    query = "datascience"
    end_time = "{{data_interval_end.strftime('%Y-%m-%dT%H:%M:%S.00Z')}}"
    start_time = "{{data_interval_start.strftime('%Y-%m-%dT%H:%M:%S.00Z')}}"
    file_path = f"datalake/twitter_datascience-extract_date={{ ds }}/{{ ds_nodash }}.json"

    
    to = ApiExternaOperator(query=query,file_path=file_path,start_time=start_time,end_time=end_time,task_id="ApiExec_dataScience")
    output_path = '/opt/airflow/datalake/rawzone/meus_dados.csv'
    

    twitter_operator = TwitterOperator(file_path=join("datalake/twitter_datascience/extract_date={{ data_interval_start.strftime('%Y-%m-%d') }}",
                                        "datascience_{{ ds_nodash }}.json"),
                                        query=query,
                                        start_time="{{ data_interval_start.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}",
                                        end_time="{{ data_interval_end.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}",
                                        task_id="twitter_datascience")


    # def save_data(to,**kwargs):
    #     # Aqui você pode acessar os dados que a task `to` processou
    #     # Para fins de exemplo, vamos escrever uma string no arquivo
    #     os.makedirs(os.path.dirname(output_path), exist_ok=True)
    #     with open(output_path, 'w') as f:
    #         f.write(to)

    # save_data_task = PythonOperator(
    #     task_id='save_data_task',
    #     python_callable=save_data
    # )




    # to >> save_data_task