import pymongo
import json

def ImportData(filename):
    # 讀取json檔案
    with open(filename,'r', encoding='utf-8') as data:
        result_data = json.load(data)
    
    # 建立資料庫連線
    client = pymongo.MongoClient("localhost", 27017)
    
    # 建立／選擇database
    db = client.testDB
    
    # 建立／選擇collection
    collection = db.productInfo
    
    # insert資料至collection
    collection.insert_many(result_data)
    
    client.close()
    
if __name__ == "__main__":
    # json檔案位置
    filename = "product_info_NY2.json"
    ImportData(filename)
