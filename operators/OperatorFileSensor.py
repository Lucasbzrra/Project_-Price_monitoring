from airflow.sensors.filesystem import FileSensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import re 

class OperatorFileSensor(FileSensor):
    
    def __init__(self,site_name,*args,**kwargs):
        super().__init__(*args,**kwargs)
        self.site=site_name

    def poke(self,context):

        path=context['ti'].xcom_pull(key='pathToData')
        date = path.split('_')[1].split('/')[0]

        bucketname='silverzone'
        s3_hook = S3Hook(aws_conn_id='s3_connect')
        files=s3_hook.list_keys(bucket_name=bucketname,prefix=f'{self.site}/{self.site}_{date}')

        if len(files)>=4:
            #context['ti'].xcom_push(key='path_container',value=self.path)
            context['ti'].xcom_push(key=f'file_sensor_{self.site}',value=True)
            return True
        return False
