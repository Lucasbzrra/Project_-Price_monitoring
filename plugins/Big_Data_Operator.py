import pandas as pd
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


## Plugins são basicamentes a classes que crie, como operators

class BigDataOperator(BaseOperator):
    
    @apply_defaults
    def __init__(self, path_to_csv_file,path_to_save_file,separator=';',
                 file_type='parquet',*args,**kwargs) ->None:
        super().__init__(*args,**kwargs)
        self.path_to_csv_file = path_to_csv_file
        self.path_to_save_file = path_to_save_file
        self.file_type=file_type
        self.separtor=separator

    def execut(self,contex):

        df =pd.read_csv(self.path_to_csv_file, sep=self.separtor)
        
        if self.file_type=='parquet':
            df.to_parquet(self.path_to_save_file)
        else:
            df.to_json(self.path_to_save_file)