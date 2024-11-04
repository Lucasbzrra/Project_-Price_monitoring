from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.macros import ds_add
from os.path import join 
import pandas as pd
import pendulum

#Quando queremos definir intervalos de tempo um pouco mais complexos para a execução do nosso DAG, 
#o Airflow permite a utilização das chamadas Cron Expressions.

# Sendo os valores possíveis para cada um desses componentes:

# minuto: 0-59;
# hora: 0-23;
# dia do mês: 1-31;
# mês: 1-12;
# dia da semana: 0-6 representando de domingo a sábado.

with DAG(
    "dados_climaticos",
    start_date=pendulum.datetime(2024, 8, 21, tz="UTC"),
    schedule_interval='0 0 * * 1', # executar toda segunda feira
) as dag:

    tarefa_1 = BashOperator(
        task_id = 'cria_pasta',
        bash_command = 'mkdir -p /usr/local/airflow/custom_pasta/semana={{data_interval_end.strftime("%Y-%m-%d")}}'
    )

    def extrai_dados(data_interval_end):
        city = 'Boston'
        key = 'ANZQ5K8QQP8BXZ85F4ZEQ2FPK'

        URL = join('https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/',
            f'{city}/{data_interval_end}/{ds_add(data_interval_end, 7)}?unitGroup=metric&include=days&key={key}&contentType=csv')

        dados = pd.read_csv(URL)

        file_path=f'/usr/local/airflow/custom_pasta/semana={{data_interval_end}}/'
        
        dados.to_csv(file_path + 'dados_brutos.csv')
        dados[['datetime','tempmin', 'temp', 'tempmax']].to_csv(file_path + 'temperaturas.csv')
        dados[['datetime', 'description', 'icon']].to_csv(file_path + 'condicoes.csv')

    tarefa_2 = PythonOperator(
        task_id = 'extrai_dados',
        python_callable = extrai_dados,
        op_kwargs = {'data_interval_end': '{{data_interval_end.strftime("%Y-%m-%d")}}'}
    )

    tarefa_1 >> tarefa_2
