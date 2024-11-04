from datetime import datetime, timedelta
import requests

TIMESTAMP_FORMT = "%Y-%m-%dT%H:%M:%S.00Z"

end_time= datetime.now().strftime(TIMESTAMP_FORMT)
start_time = (datetime.now()+timedelta(-1)).date().strftime(TIMESTAMP_FORMT)
query = "data science"
tweet_fields = "tweet.fields=author_id,conversation_id,created_at,id,in_reply_to_user_id,public_metrics,lang,text"
user_fields = "expansions=author_id&user.fields=id,name,username,created_at"


url_raw = f"https://labdados.com/2/tweets/search/recent?query={query}&{tweet_fields}&{user_fields}&start_time={start_time}&end_time={end_time}"

response=requests.get(url_raw)
if response==200:
    Data=response.json()
else:
    print('errorc')