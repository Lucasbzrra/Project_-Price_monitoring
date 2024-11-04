from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.remote.remote_connection import RemoteConnection
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from airflow.models import BaseOperator, DAG, TaskInstance
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import json
import os
import time
import datetime
from tempfile import NamedTemporaryFile
from abc import ABC, abstractmethod

class OperatorSiteBase(BaseOperator, ABC):

    def __init__(self, site,IdtaskContainer, **kwargs):
        super().__init__(**kwargs)
        self.IdtaskContainer = IdtaskContainer
        self.PathToData = None
        self.ListProdutctKabum = []
        self.site=site
        self.ListUrl = ['placas-mae', 'processadores', 'memoria-ram', 'placa-de-video-vga']

    @abstractmethod
    def _Urls(self, produto, numpage): 
        pass
    
    @abstractmethod
    def _ProcessamentoBeautifulSoup(self, html, url):
        pass
    @abstractmethod
    def ExtractDates(self):
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.121 Safari/537.36")

        
        hub_url = "http://selenium:4444/wd/hub"
    
        driver = webdriver.Remote(
            command_executor=hub_url,
            options=chrome_options
        )

        for url in self.ListUrl:
            for numpage in range(1, 6):
                base_url = self._Urls(produto=url, numpage=numpage)
                driver.get(base_url)
                time.sleep(5)
                html = driver.page_source
                self._ProcessamentoBeautifulSoup(html, url)
        driver.quit()
        return self.ListProdutctKabum
        
    
    def SaveFile(self, context, **kwargs):
    
        ti = context['ti']

        data = self.ListProdutctKabum
        
        if isinstance(data, set):
            data = list(data)
        
        if isinstance(data, list) and all(isinstance(produto, list) for produto in data):
            data = [[item if item is not None else '' for item in produto] for produto in data]

        s3_hook = S3Hook(aws_conn_id='s3_connect')

        with NamedTemporaryFile(delete=False, suffix='.json') as temp_file:
            
            json_data = json.dumps(data)

            temp_file.write(json_data.encode('utf-8'))  
            temp_file_path = temp_file.name  

        if self.site=='kabum':

            filepath = os.path.join(f'kabum_{datetime.date.today().isoformat()}/', 'data.json')

            s3_hook.load_file(   
               filename=temp_file_path,
                key=f'kabum/{filepath}',
                bucket_name='rawzone')
        if self.site=='pichau':

            filepath = os.path.join(f'pichau_{datetime.date.today().isoformat()}/', 'data.json')


            s3_hook.load_file(   
                filename=temp_file_path,
                key=f'pichau/{filepath}',
                bucket_name='rawzone')
            
        os.remove(temp_file_path)
        ti.xcom_push(key='pathToData', value=filepath)
        self.PathToData = filepath
    

    def execute(self, context):
        self.ExtractDates()
        self.SaveFile(context)