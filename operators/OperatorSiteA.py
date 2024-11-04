from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from operators.OperatorSiteBase import OperatorSiteBase
import re
import os
import time

class OperatorSiteA(OperatorSiteBase):
    
    def __init__(self,site, **kwargs):
        
        kwargs['IdtaskContainer']=f'Create_Container_Raw_{site}'
        super().__init__(site=site,**kwargs)

    def init_driver(self):
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
        return driver
    
    def clean_price(self,price):
        return re.sub(r'[^\d,]', '', price).replace(',', '.') if price else None
    
    def ExtractDates(self):
         pass

    def _Urls(self, produto, numpage):
        if produto.lower() == 'placas-mae':
            return f'https://www.pichau.com.br/hardware/placa-m-e?page={numpage}'
        elif produto.lower() == 'placa-de-video-vga':
            return f'https://www.pichau.com.br/hardware/placa-de-video?page={numpage}'
        elif produto.lower() == 'processadores':
            return f'https://www.pichau.com.br/hardware/processadores?page={numpage}'
        else:
            return f'https://www.pichau.com.br/hardware/memorias?page={numpage}'
    

    def _ProcessamentoBeautifulSoup(self):

        for produto in self.ListUrl:
                
                for numpage in range(1, 4):

                    url=self._Urls(produto,numpage)  
                    driver = self.init_driver()

                    try:
                        driver.get(url)
                        # Aguardar até que os produtos estejam presentes na página
                        WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, '.MuiGrid-item'))
                        )
                        time.sleep(5)  # Tempo extra para garantir que a página esteja carregada completamente

                        products = driver.find_elements(By.CSS_SELECTOR, '.MuiGrid-item')

                        for product in products:
                            try:
                                link_element = product.find_element(By.XPATH, './/a')
                                link = link_element.get_attribute('href') if link_element else None

                                # Tenta encontrar o nome do produto
                                name_element = product.find_element(By.XPATH, './/h2')  # Tentativa mais genérica
                                name = name_element.text.strip() if name_element else None
                                price_element = product.find_element(By.XPATH, './/div[contains(text(), "R$")]')
                                price = price_element.text.strip() if price_element else None
                                cleaned_price = self.clean_price(price)

                                self.ListProdutctKabum.append({
                                'category': produto,
                                'name': name,
                                'value': cleaned_price ,
                                'link': link if link else 'Não encontrado'
                                
                                })

                            except Exception as e: 
                                None
                    finally:
                            driver.quit()  # Fecha o navegador

    def execute(self, context):
        self._ProcessamentoBeautifulSoup()
        self.SaveFile(context)