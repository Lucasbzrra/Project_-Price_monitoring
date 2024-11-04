from airflow.models import *
import json
import os
import sys
sys.path.append('/usr/local/airflow/custom_pasta')

class operatorSaveFile(BaseOperator):
    
    def __init__(self,**kwargs):
        super().__init__(**kwargs)
    
    def execute(self, context):
    # Obtém o contexto da tarefa
        ti = context['ti']

    # Pega o caminho do XCom
        self.path = ti.xcom_pull(task_ids='create_container_kabum', key='path')
    
    # Pega os dados do XCom (verifique o nome correto do task_id)
        self.data = ti.xcom_pull(task_ids='WebScrapint')

    # Verifica se os dados são um set e converte para lista, se necessário
        if isinstance(self.data, set):
            self.data = list(self.data)
        
        # Verifica se self.data é uma lista de listas
        if isinstance(self.data, list) and all(isinstance(produto, list) for produto in self.data):
            # Processa os dados substituindo None por string vazia
            self.data = [[item if item is not None else '' for item in produto] for produto in self.data]

    # Serializa os dados para JSON
        json_str = json.dumps(self.data, ensure_ascii=False)

    # Cria o caminho completo para salvar o arquivo
        filepath = os.path.join(self.path, 'data.json')

    # Escreve o JSON no arquivo
        with open(filepath, 'w',encoding='utf-8') as arquivo:
            arquivo.write(json_str)    
