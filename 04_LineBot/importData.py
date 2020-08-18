import pandas as pd
from elasticsearch import Elasticsearch
import json

def connect_elasticsearch():
    es = None
    es = Elasticsearch(['localhost:9200'])
    if es.ping():
        print("Connected to Elasticsearch!")
    else:
        print("Connection Failed")
    return es

def create_index(es, index_name):
    # index settings
    settings = {
        "settings": {
            "analysis": {
                "analyzer" : {
                    "product_text_analyzer": {
                        "type": "standard",
                        "stopwords" : "_english_"
                    }
                }
            }
        },
        "mappings":{
            "properties": {
                "product_id" : {
                    "type": "integer"
                },
                "product_name": {
                    "type": "text"
                },
                "product_info": {
                    "type": "text",
                    "analyzer": "product_text_analyzer"
                },
                "info_for_line": {
                    "type":"nested"
                        }
                    } 
                }
            }
    try:
        if not es.indices.exists(index_name):
            # Ignore 400 means to ignore "Index Already Exist" error.
            es.indices.create(index=index_name, ignore=400, body=settings)
            print('{} Created'.format(index_name))
    except Exception as ex:
        print(ex)
        print('Error When Creating {}: {}'.format(index_name, str(es)))

def insert_doc(es, index_name ,doc):
    try:
        result = es.index(index=index_name, body=doc)
    except Exception as ex:
        print(str(ex))
        print(doc)
    return result

def search(es, index_name, query):
    result = es.search(index=index_name, body=query)
    items = result['hits']['hits']
    return items

if __name__ == '__main__':
    
    # import product info 到 Elasticsearch
    es = connect_elasticsearch()
    create_index(es, 'product')
    
    with open("product_data.json",'r') as f:
        result = json.load(f)
        for doc in result:
            res = insert_doc(es, "product", doc)
    
    # 嘗試用 birthday gift 當作關鍵字查詢商品
    query = {
        "query": {
            "match": {
                "product_info": {
                    "query": "birthday gift"
                }
            }
        }
    }
    
    # 打印搜尋到的 product_id 和 LineBot 回覆所需的資訊格式
    items = search(es, "test", query)
    for item in items:
        print(item['_source']['product_id'])
        print(item['_source']['info_for_line'])
        print('\n')
        
    es.close()