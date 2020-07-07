import pymongo
import json

def ImportData(filename):
    # 讀取json檔案
    with open(filename,'r', encoding='utf-8') as data:
    result_data = json.load(data)
    
    # 建立資料庫連線
    client = pymongo.MongoClient("localhost", 27017)
    
    db = client.ProductRawData
    collection = db.productInfo
    
    collection.insert_many(result_data)
    
    client.close()
    
if __name__ == "__main__":
    # json檔案位置
    filename = "product_info_NY2"
    ImportData()
