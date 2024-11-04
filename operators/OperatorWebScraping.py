from selenium import webdriver
from bs4 import BeautifulSoup
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.remote.remote_connection import RemoteConnection
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from airflow.models import BaseOperator, DAG, TaskInstance
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import re
import json
import os


class WebScraping_K(BaseOperator):

    def __init__(self,IdtaskContainer,**kwargs):
        
        self.IdtaskContainer=IdtaskContainer
        self.PathToData=None
        super().__init__(**kwargs,do_xcom_push=True)
        
        self.ListProdutctKabum=[]
        self.ListUrl=['placas-mae','processadores','memoria-ram','placa-de-video-vga']


    def __Urls(self,produto,numpage): 
        
        if produto.lower()=='placas-mae':
            url_placa_mae=f'https://www.kabum.com.br/hardware/placas-mae?page_number={numpage}&page_size=100&facet_filters=&sort=most_searched'
            return url_placa_mae
        elif produto.lower()=='placa-de-video-vga':
            url_placa_video=f'https://www.kabum.com.br/hardware/placa-de-video-vga?page_number={numpage}&page_size=100&facet_filters=&sort=most_searched' 
            return url_placa_video
        elif produto.lower()=='processadores':
            url_processador=f'https://www.kabum.com.br/hardware/processadores?page_number={numpage}&page_size=100&facet_filters=&sort=most_searched'
            return url_processador
        else:
            url_memoria=f'https://www.kabum.com.br/hardware/memoria-ram?page_number={numpage}&page_size=100&facet_filters=&sort=most_searched'
            return url_memoria
    
    def __ProcessamentoBeautifulSoup(self,html,url):

        soup = BeautifulSoup(html, 'html.parser')
        container=soup.find(id='font-container', class_='__className_49bc20')
        maincontainer=container.find(id='main-content')
        clas=maincontainer.find('div', class_="sc-gsTEea sc-202cc1e9-2 ezCvIu SzkqH")
        clas2=clas.find('div', class_='sc-hKgJUU hzqTWi', style="align-content: start;")
        main=clas2.find('main',class_='sc-68afdc85-13 eTXlUT')
        ProductsAll=main.find_all('div',class_='p-[3px] rounded-4 group bg-white shadow-[0_0_1px_rgba(40,41,61,0.08),0_0.5px_2px_rgba(96,97,112,0.16)] hover:shadow-lg')

        for product in ProductsAll:

            name = None
            value = None
            href = None

            product_link = product.find('a', class_="productLink")
            if product_link:
                href = product_link.get('href')
                print(href)

            product_name = product.find('img', class_='imageCard')
            if product_name:
                name = product_name.get('title')

            current_price = product.find('span', class_='sc-84f95ca7-2 jJMtJn priceCard')
            if current_price:
                value=current_price.text.strip()
                value_clean= re.sub(r'[^\d,]','',value)
                value=value_clean.replace(',','')

           
             
            self.ListProdutctKabum.append({
                'category':url,
                'name':name,
                'value':value,
                'link':href
            })
    def ExtractDates(self):

        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        
        hub_url = "http://selenium:4444/wd/hub"
    
        driver = webdriver.Remote(
            command_executor=hub_url,
            options=chrome_options
        )

        
        for url in self.ListUrl:

            for numpage in range(1,6):
                # Define o URL base
                base_url = self.__Urls(produto=url,numpage=numpage)
                driver.get(base_url)
                html = driver.page_source
                self.__ProcessamentoBeautifulSoup(html, url)
        
        driver.quit()
        return self.ListProdutctKabum
    
    def SaveFile(self,context,**kwargs):
        ti = context['ti']

        # Pega o caminho do XCom
        path = ti.xcom_pull(task_ids=self.IdtaskContainer, key='path_container')
        
        data =self.ListProdutctKabum
        
        # Verifica se os dados são um set e converte para lista, se necessário
        if isinstance(data, set):
            data = list(data)
        
        # Verifica se self.data é uma lista de listas

        if isinstance(data, list) and all(isinstance(produto, list) for produto in data):
            # Processa os dados substituindo None por string vazia
            data = [[item if item is not None else '' for item in produto] for produto in data]

        # Serializa os dados para JSON
        json_str = json.dumps(data, ensure_ascii=False)

        # Cria o caminho completo para salvar o arquivo
        filepath = os.path.join(path, 'data.json')

        ti.xcom_push(key='pathToData',value=filepath)
        self.PathToData=filepath
        # Escreve o JSON no arquivo
        with open(filepath, 'w',encoding='utf-8') as arquivo:
            arquivo.write(json_str)    


    def execute(self,context):
        self.ExtractDates()
        self.SaveFile(context)
        
