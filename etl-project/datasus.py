#!/usr/bin/env python3

import requests
import json
import boto3
import awswrangler as wr
import pandas as pd 


from requests.auth import HTTPBasicAuth

url_datasus = "https://imunizacao-es.saude.gov.br/_search?scroll=1m"

payload = json.dumps({
  "size": 10000
})

headers = {
  'Content-Type': 'application/json',
  'Authorization': 'Basic aW11bml6YWNhb19wdWJsaWM6cWx0bzV0JjdyX0ArI1Rsc3RpZ2k='
}

all_data = []

s3_client = boto3.client('s3')
file_name = ""
file_number = 0
while True:
    response = requests.request(
            "POST",
            url_datasus, 
            auth=HTTPBasicAuth('imunizacao_public','qlto5t&7r_@+#Tlstigi'),
            data = payload,
            headers = headers
    )
    
    # Check the response status
    if response.status_code == 200:
        json_scroll_id = response.json()['_scroll_id']
        json_hits_content = response.json()['hits']['hits']
        
        if json_hits_content:
            all_data.extend(json_hits_content)

            url_datasus = "https://imunizacao-es.saude.gov.br/_search/scroll"
            payload = json.dumps({
                "scroll": "1m",
                "scroll_id":json_scroll_id
            })

            file_number +=1
            df_covid = pd.DataFrame(all_data)
            file_name = f"datasus_imunizacao_{file_number:05d}"
            
            print(f'upload file {file_name}')
            wr.s3.to_parquet(
                df = df_covid,
                path = f"s3://data-lake-fia-tier-1/raw-data/datasus-imunizacao/{file_name}")
            
        else:
            break
    else:
        print('Error occurred:', response.status_code)
        break

