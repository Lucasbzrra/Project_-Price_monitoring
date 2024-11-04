import sys
import os
sys.path.append('/usr/local/airflow/custom_pasta')
from airflow.models import BaseOperator, DAG, TaskInstance
from Hook.ApiExterna import ApiExterna
import json
from pathlib import Path 
from datetime import datetime, timedelta

class ApiExternaOperator(BaseOperator):

    template_fields =["query","file_path","start_time","end_time"]

    def __init__(self, end_time,start_time,query,file_path,**kwargs):
        self.end_time = end_time
        self.start_time=start_time
        self.query=query
        self.file_path=file_path
        super().__init__(**kwargs)

    def create_parent_folder(self):
        (Path(self.file_path).parent).mkdir(parents=True, exist_ok=True)
    
    def execute(self, context):
        end_time=self.end_time
        start_time = self.start_time
        query = self.query 

        self.create_parent_folder()

        with open("self.file_path","w") as output_file: 
            for pg in ApiExterna(end_time,start_time,query).run():
                json.dump(pg,output_file)
                output_file.write("\n")

if __name__ == "__main__":
    #montando url
    TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"

    end_time = datetime.now().strftime(TIMESTAMP_FORMAT)
    start_time = (datetime.now() + timedelta(-1)).date().strftime(TIMESTAMP_FORMAT)
    query = "datascience"

    for pg in ApiExterna(end_time, start_time, query).run():
        print(json.dumps(pg, indent=4, sort_keys=True))