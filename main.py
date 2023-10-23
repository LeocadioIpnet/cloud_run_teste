from flask import Flask, request, make_response, jsonify
import pandas as pd
import requests
import gcsfs
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas_gbq
import os
import json
 
app = Flask(__name__)
app.config["DEBUG"] = True
 
@app.route("/", methods=["GET"])
 
 
def open_now_deals_products():
    
    API_KEY='c106e4c813a5cd657f8c17987ea81be6b9f137c2'
    
    client = bigquery.Client()
    query = """
    SELECT id FROM `ipnet-data-cr.pipe_drive.deals` where status='open' 
    """
 
    Query_Results = client.query(query)
    
    df = Query_Results.to_dataframe()
    lista_id=list(df['id'])
    lista_id = lista_id[0:10]
    
    params = (('api_token', API_KEY),)
 
    url ='https://api.pipedrive.com/v1/deals/'+str(lista_id[0])+'/products'
    response = requests.get(url, params=params)
    json= response.json() 
    result=json['data']
    products=pd.DataFrame(result)
    
    for i in range(1,len(lista_id)):
        url = 'https://api.pipedrive.com/v1/deals/'+str(lista_id[i])+'/products'
        response = requests.get(url, params=params)
        json= response.json() 
        result=json['data']   
        products = pd.concat([products, pd.DataFrame(result)])
        
    products.reset_index(inplace=True)
    products.drop(['index'],axis=1,inplace=True)

    for col in list(products.columns):
        products[col]= products[col].astype(str)
     
    project_id = 'ipnet-data-cr'
    table_id = 'teste.open_now_deals_products_copy'
    pandas_gbq.to_gbq(products, table_id, project_id=project_id, if_exists='replace') 
 
    return "A tabela foi atualizada"
