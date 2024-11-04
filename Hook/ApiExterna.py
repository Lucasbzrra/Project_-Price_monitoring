from airflow.providers.http.hooks.http import HttpHook
from datetime import datetime, timedelta
import requests
import json

## Para executar o script tem acessar onde está no container e executar pelo nome do scprit

class ApiExterna(HttpHook):

    def __init__(self,end_time, start_time,query ,conn_id=None):
        self.end_time=end_time
        self.start_time = start_time
        self.query=query
        self.conn_id=conn_id or "Api_Externa" #<== Onde criamos o nome da conexão no connections do airflow
        super().__init__(http_conn_id=self.conn_id)

    def create_url(self):
        TIMESTAMP_FORMAT="%Y-%m-%dT%H:%M:%S.00Z"

        end_time=self.end_time
        start_time=self.start_time
        query=""
        tweet_fields = "tweet.fields=author_id,conversation_id,created_at,id,in_reply_to_user_id,public_metrics,lang,text"
        user_fields = "expansions=author_id&user.fields=id,name,username,created_at"

        url_raw=f"{self.base_url}/2/tweets/search/recent?query={query}&{tweet_fields}&{user_fields}&start_time={start_time}&end_time={end_time}"
        return url_raw

    
    def connect_to_endpoint(self,url,session):
        request = requests.Request("GET", url)
        prep = session.prepare_request(request)
        self.log.info(f"URL: {url}")
        return self.run_and_check(session, prep, {})
    
    def paginate(self,url_raw,session):
        lista_json_response=[]
        response=self.connect_to_endpoint(url_raw,session)
        json_response=response.json()
        lista_json_response.append(json_response)
        
        while "next_token" in json_response.get("meta",{}):
            next_token = json_response['meta']['next_token']
            url = f"{url_raw}&next_token={next_token}"
            response = self.connect_to_endpoint(url,session)
            json_response = response.json()
            lista_json_response.append(json_response)
    
        return lista_json_response

    def run(self): #<= Chama o método run do HTTPHOOK que e responsável por chamar a conexão
        session = self.get_conn()
        url_raw=self.create_url()
        return self.paginate(url_raw,session)
    