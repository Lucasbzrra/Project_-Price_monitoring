from selenium import webdriver
from bs4 import BeautifulSoup
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.remote.remote_connection import RemoteConnection
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from airflow.models import BaseOperator, DAG, TaskInstance
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import re
from operators.OperatorSiteBase import OperatorSiteBase
import time

class OperatorSiteB(OperatorSiteBase):
    
    def __init__(self,site, **kwargs):

        kwargs['IdtaskContainer'] = f'Create_Container_Raw_{site}'
        super().__init__(site=site,**kwargs)

    def _Urls(self, produto, numpage): 
        if produto.lower() == 'placas-mae':
            return f'https://www.kabum.com.br/hardware/placas-mae?page_number={numpage}&page_size=100&facet_filters=&sort=most_searched'
        elif produto.lower() == 'placa-de-video-vga':
            return f'https://www.kabum.com.br/hardware/placa-de-video-vga?page_number={numpage}&page_size=100&facet_filters=&sort=most_searched'
        elif produto.lower() == 'processadores':
            return f'https://www.kabum.com.br/hardware/processadores?page_number={numpage}&page_size=100&facet_filters=&sort=most_searched'
        else:
            return f'https://www.kabum.com.br/hardware/memoria-ram?page_number={numpage}&page_size=100&facet_filters=&sort=most_searched'

    def ExtractDates(self):
        return super().ExtractDates()



    def _ProcessamentoBeautifulSoup(self, html, url):
        soup = BeautifulSoup(html, 'html.parser')
        produtos = soup.find_all('article', class_=re.compile('productCard'))
        value_clean=0

        for produto in produtos:
            name = None
            linkfull = None
            price = None

            link = produto.find('a', class_=re.compile('productLink'))
            if link:
                linkfull = 'https://www.kabum.com.br' + link['href']  

            name = produto.find('span', class_=re.compile('nameCard'))
            if name:
                name = name.get_text().strip()
                print(name)

            price_element = produto.find('span', class_=re.compile('priceCard'))
            if price_element:
                price = price_element.get_text().strip()
                price = price.replace('.', '').replace(',', '.')
                value_clean=price.replace('R$','')
                print(value_clean)

            self.ListProdutctKabum.append({
                'category': url,
                'name': name,
                'value': value_clean,
                'link': linkfull
                })