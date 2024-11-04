from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


class OperatorSubmitSpark(SparkSubmitOperator):

    def __init__(self, site_name,application_args=None, *args,**kwargs):
        super().__init__(*args, **kwargs)
        self.application_args = application_args or []
        self.site=site_name

    def execute(self,context):
    
        web_extraction_path=context['ti'].xcom_pull(task_ids=f'{self.site}_tasks.Web_Extraction_{self.site}',key='pathToData')
        
        self.application_args = ['--json_file_path', f'{web_extraction_path}']
        super().execute(context)