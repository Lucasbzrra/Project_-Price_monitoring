import sys
sys.path.append('/usr/local/airflow/custom_pasta')
from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator


with DAG(
    'DependenteDaDag_Paralelismo',
    start_date=days_ago(1),
    schedule_interval=None,
    description='Dependente da dag PAI'
) as dag:
    task100=BashOperator(task_id='task100',bash_command='sleep 5')