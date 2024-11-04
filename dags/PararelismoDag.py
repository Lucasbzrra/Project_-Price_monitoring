import sys
sys.path.append('/usr/local/airflow/custom_pasta')
from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.dagrun_operator import TriggerDagRunOperator


default_args = {
    'depends_in_past':False,
    'start_date':datetime(2024,9,27),
    'email':['test@test.com'],
    'email_on_failure':False,
    'email_on_retry':False,
    'retries':1,
    'retry_delay':timedelta(seconds=10)
}
    

# Parâmetros de uma DAG

# dag_id = idnetificador único para a DAG No cluster
# description = Descrição da DAG
# schedule_interval = o intervalo de tempo no qual a dag será executada
# start_date = a data e hora em que a dag deve começar a ser executada
# end_date = a data e hora em que a dag não deve mais ser esecutada
# catchup = determina se a DAG deve executar todas as tarefas que deveriam ter sido executada 
# dese a data de star_date até o momento atual.
# default_view = visualização padrão da interfacedo airflow para esta DAG
# max_active_runs = maximo de execuções ativas da DAG permitidas
# concurrency = maximo de tarefas que podem ser executadas siumltaneamente
# tags = uma lista de tags para marcar a dag e suas execuções
# default_args = dicionario de argumentos padrão que serão aplicados a todas as tarefas da dag
# a menos que sejam especificamente subtituídos 
# depends_on_past só inicia se no passado executou com sucesso. Padrão true
# email = endereço de email para failure ou retry
# email_on_failure =  True/false
# email_on_retry = true/false
# retries = numero padrão de novas tentativas
# retry_delay = timedelta(minutes=5)

# Parâmetros de Trigger RULE

# all_success = a tarefa é executada se todas anteriores foram concluídas com sucesso
# all_failed = a tarefa é executada se todas as tarefas anteriores falharam
# all_done = a tarefa é executada quando todas as tarefas anteriores foram concluídas, independenmente do status
# one_sucess = a tarefa é executada se pelo menos umadas tarefas anteriores foi concluída com sucesso
# one_failed = a tarefa é executada se pelo menos uma das tarefas anteriores falhou
# none_failed = a tarefa é executada se nenhuma das tarefas anteriores falhou 
# none_skipped = a tarefa é executada se nenhuma das tarefas anteriores foi pulada
# dummy = a tarefa é sempre executada, independemente do status das tarefas anteriores



with DAG (
    'Pararelismo',
    start_date =days_ago(1),
    schedule_interval=None,
    description ='Aprendendo a executar duas task ao mesmo tempo',
    catchup=False,
    default_view='graph',
    tags=['TEXT','tag','pipeline']
) as dag:
    

    task1=BashOperator(task_id='tsk1',bash_command='sleep 5')
    task2=BashOperator(task_id='tsk2',bash_command='sleep 5')
    task3=BashOperator(task_id='tsk3',bash_command='sleep 5')
    task4=BashOperator(task_id='tsk4',bash_command='sleep 5')
    task5=BashOperator(task_id='tsk5',bash_command='sleep 5',trigger_rule='all_success') ## <==== Passando os parâmetros de regra da triggers
    task6=BashOperator(task_id='tsk6',bash_command='sleep 5')
    
    #Instanciando a Classe TaskGroup para agrupar as task
    tsk_group= TaskGroup('tsk_group',dag=dag)


    task7=BashOperator(task_id='tsk7',bash_command='sleep 5',task_group=tsk_group) 
    task8=BashOperator(task_id='tsk8',bash_command='sleep 5',task_group=tsk_group) 
    task9=BashOperator(task_id='tsk9',bash_command='sleep 5',task_group=tsk_group)  

    task_filha=TriggerDagRunOperator(task_id='task_dependente',trigger_dag_id='DependenteDaDag_Paralelismo',dag=dag) ## <== ESSA DAG E DEPENDENTE DA DAG PARALELISMOO

    #task1>>[task2,task3] #Paralelismo
    #[task2,task3]>>task1  #Paralelismo
    
    #task1.set_upstream(task2) # mesmo formato de execução das tarefas utilizando setas '>>' só muda que e mais formal 
    #task2.set_upstream(task3) 
    
    #[task1,task2]>>task3

    #Organizando task, vamos supor que tem varias task e um nível de complexdidade maior, poderá utilizar a quebra  de linha 
    # Pode vê que serão executadas as task1 até task 4 paralelamente, depois se task2 e task4 foram executadas  a 5 e 6 podem executar e depois em diante
    
    # task1>>task2
    # task3>>task4

    # [task2,task4]>>task5>>task6

    # task6>>[task7,task8,task9]

    # Tem outra forma, conforme mostrado com exemplo anterior, agora com task group

    task1>>task2
    task3>>task4

    [task2,task4]>>task5>>task6

    task6>>tsk_group>>task_filha
