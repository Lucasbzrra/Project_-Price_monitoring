from airflow.operators.bash import BashOperator


class CreateContainerInDataLake(BashOperator):
    
    count_instance = 0

    def __init__(self, Layer,NameContainer, **kwargs):
        
        bash_command = f'mkdir -p /opt/airflow/datalake/{Layer}/{NameContainer}_{{{{data_interval_end.strftime("%Y-%m-%d")}}}}'

        self.path=None

        super().__init__(
            bash_command=bash_command,
            do_xcom_push=True,
            **kwargs
        )

    def execute(self, context):
        super().execute(context)
        path = self.bash_command.replace('mkdir -p ', '')
        context['ti'].xcom_push(key='path_container', value=path)
        