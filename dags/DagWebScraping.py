from airflow.models import DAG
from airflow.utils.dates import days_ago
from operators.OperatorCreateContainer import CreateContainerInDataLake 
from operators.OperatorWebScraping import WebScraping_K
from operators.OperatorSpark import OperatorSubmitSpark
from operators.OperatorFileSensor import OperatorFileSensor
from operators.OperatorSiteB import OperatorSiteB
from operators.OperatorSiteA import OperatorSiteA
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.dummy import DummyOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

def task_file_exists(**kwargs):
  
   file_found_A = kwargs['ti'].xcom_pull(task_ids='Check_for_file_pichau', key='file_sensor_pichau')

   file_found_B = kwargs['ti'].xcom_pull(task_ids='Check_for_file_kabum', key='file_sensor_kabum')
   
   if file_found_A and file_found_B:
      return 'FileToDW'
   else:
      return 'Task_Submission_With_Error'



with DAG(
    "pipeline_webscraping",
    start_date=days_ago(1),
    #schedule_interval='@daily',
    schedule_interval=None,
    description='Dag de Pipeline de Dados de Web Scraping com processos ETL',
    catchup=False,
    #max_active_tasks=1 # <===  (se for True) Se dentro do meu intervalo de datas tipo, ele vai executar os dias dentro desse intervalo
) as dag:
   
  
   sites = ['pichau','kabum']
   taskgroups=[]

   start=DummyOperator(task_id='start')

   for site in sites:

      with TaskGroup(f'{site}_tasks') as site_group:
         
         if site=='pichau':
            Web_Extraction_site  = OperatorSiteA(
            task_id =f'Web_Extraction_{site}',
            site=site,
            IdtaskContainer=f'Create_Container_Raw_{site}')
         if site=='kabum':
          Web_Extraction_site = OperatorSiteB(
            task_id =f'Web_Extraction_{site}',
            site=site,
            IdtaskContainer=f'Create_Container_Raw_{site}')

         Spark_Transformation = OperatorSubmitSpark(
         site_name=site,
         task_id=f'Spark_Transformation_{site}',
         application='/opt/bitnami/spark/src/scripts_spark/WebScraping_Transformation.py',
         conn_id='spark_default',
         packages='org.apache.hadoop:hadoop-aws:3.3.1',
         application_args=None)

         Web_Extraction_site  >> Spark_Transformation
         taskgroups.append(site_group)

   start >> taskgroups[0]  

   for i in range(len(taskgroups) - 1):
    taskgroups[i] >> taskgroups[i + 1]

   File_Sensor_A = OperatorFileSensor(
      task_id=f'Check_for_file_kabum',
      fs_conn_id='fs_default',
      site_name='kabum',
      filepath=None)
   
   File_Sensor_B = OperatorFileSensor(
      task_id=f'Check_for_file_pichau',
      fs_conn_id='fs_default',
      site_name='pichau',
      filepath=None)


   Branching  = BranchPythonOperator(
      task_id='Branch',
      python_callable=task_file_exists,
      provide_context=True)


   Task_Submission_With_Error = EmailOperator(
      task_id='Task_Submission_With_Error',
      to='alves.lucas876@gmail.com',
      subject='AIRFLOW ERROR IN TASK',
      html_content=""" <h3> Ocorreu um erro na DAG. </h3>
        <p> Dag: Ocorreu error na tarefa em que o operator de checar os arquivos </p>""",
      trigger_rule='one_failed'
   )

   taskfinal=TriggerDagRunOperator(task_id='FileToDW',trigger_dag_id='LoadWebScraping')

   taskgroups[-1] >> [File_Sensor_A, File_Sensor_B] >> Branching  >>  [ taskfinal , Task_Submission_With_Error ]