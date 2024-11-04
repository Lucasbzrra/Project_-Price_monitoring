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



default_args = {
    'depends_on_past':False,
    'email':['alves.lucas896@gmail.com']
}

with DAG(
    'windturbine',
    start_date=days_ago(1),
    catchup=False

)as dag:
    
    group_check_temp = TaskGroup('group_check_tempo', dag=dag)
    group_database = TaskGroup('group_database',dag=dag)


    file_sensor_task = FileSensor(
        task_id = 'file_sensor_task',
        filepath = Variable.get('path_file'),
        fs_conn_id = 'fs_default',
        poke_interval =10,
        dag=dag
    )

    def process_file(**kwargs):
        with open(Variable.get('path_file')) as f:
            data = json.load(f)
            kwargs['ti'].xcom_push(key='idtemp', value=data['idtemp'])
            kwargs['ti'].xcom_push(key='powerfactor', value=data['powerfactor'])
            kwargs['ti'].xcom_push(key='hydraulicressure', value=data['hydraulicressure'])
            kwargs['ti'].xcom_push(key='temperature', value=data['temperature'])
            kwargs['ti'].xcom_push(key='timestamp', value=data['timestamp'])
            os.remove(Variable.get('path_file'))


    get_data = PythonOperator(
        task_id ='get_data',
        python_callable = process_file,
        provide_context = True,
        dag=dag
    )

    create_table = PostgressOperator(task_id='create_table',
                                     postgress_conn_id='postgres',
                                     sql=''' create table if not exists sensors (itemp varchar, powerfactor varchar)''',
                                     task_group=group_database,)

    insert_table = PostgressOperator(task_id='insert_table',
                                     postgress_conn_id='postgres',
                                     parameters=(
                                         '{{ti.xcom_pull(task_ids=get_data),key=timestamp}}'
                                     )
                                     sql=''' insert into sensors (idtemp, powerfactor, hydraulicpressure, temperature, timestamp)
                                     values (%s,null,null,null,null)''',
                                     task_group=group_database,)
    
