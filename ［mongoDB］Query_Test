# !pip install pymongo

import pymongo

client = pymongo.MongoClient("localhost", 27017)

db = client.ProductRawData
collection = db.productInfo

cursor = collection.find({"category":"Breakfast & Cereal"})
productlist = list(cursor)
print("Result: {}".format(productlist))
