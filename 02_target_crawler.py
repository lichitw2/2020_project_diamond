import requests
from bs4 import BeautifulSoup
import json
import time
import pandas as pd
import re

df = pd.read_csv('keywords.csv')

Row_list =[] 
for index, row in df.iterrows(): 
    my_list =[row.product_name, row.aisle] 
    Row_list.append(my_list) 

product_info = {}
product_result = []

for row in Row_list:
    if type(row[1]) != str:
        keywords = row[0]
    else:
        keywords = row[0]+" "+ row[1]
    print(Row_list.index(row),keywords)

    url = "https://redsky.target.com/v2/plp/search/?channel=web&count=6&default_purchasability_filter=true&facet_recovery=false&fulfillment_test_mode=grocery_opu_team_member_test&isDLP=false&keyword={}&offset=0&pageId=%2Fs%2F{}&pricing_store_id=1357&store_ids=1357&visitorId=01731365EC690201848CEBB8FAEBCD10&include_sponsored_search_v2=false&ppatok=AOxT33a&platform=desktop&useragent=Mozilla%2F5.0+%28Windows+NT+10.0%3B+Win64%3B+x64%29+AppleWebKit%2F537.36+%28KHTML%2C+like+Gecko%29+Chrome%2F83.0.4103.116+Safari%2F537.36&excludes=available_to_promise_qualitative%2Cavailable_to_promise_location_qualitative&key=eb2551e4accc14f38cc42d32fbc2b2ea".format(keywords,keywords)
    headers = {"User-agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36"}

    res = requests.get(url, headers = headers)

    json_res = json.loads(res.text)
    json_data = json_res["search_response"]["items"]["Item"]
    

    for data in json_data:
        product_info['keyword'] = row[0] # 商品關鍵字
        
        if 'title' in data.keys():
            product_info['name'] = data["title"] # 商品名稱
            
        if 'url' in data.keys():
            product_info['url'] = "https://target.com"+ data["url"] # 商品連結
            print(product_url)
            
        if 'images' in data.keys():
            product_info['pic'] = data["images"][0]["base_url"] + data["images"][0]["primary"]# 商品圖片
            
        if "description" in data.keys():
            product_info['Description'] = data["description"].replace('<br /><br />', ' ') # 商品描述
            
        if "brand" in data.keys():
            product_info['brand'] = data["brand"] # 商品品牌
            
        if "average_rating" in data.keys():
            product_info['star_ratings'] = [data["average_rating"]] # 平均評分
            # product_info['star_ratings'] = [product_ratings]
            
        if "total_reviews" in data.keys():
            product_info['star_ratings'].append(data["total_reviews"]) # 評分人數
            
        if "price" in data.keys():
            product_info['price'] = data["price"]['formatted_current_price'] #商品價格
            
        # at a glance(list)
        if "wellness_merchandise_attributes" in data.keys():
            product_info['At_a_glance'] = [x['value_name'] for x in data["wellness_merchandise_attributes"]]

        # Hightlights(list)
        if "bullets" in data.keys():
            product_info['Hightlights'] = data["soft_bullets"]["bullets"]

        # specifications(dict)
        spec_list = ["Contains", "Form", "State of Readiness", "Store", "Package Quantity", "Package type", "Net weight"]
        specifications = {}
        if "bullet_description" in data.keys():
            for spec_a in spec_list:
                for spec_b in data["bullet_description"]:
                    spec_b = spec_b.replace("<B>", "").replace("</B>", "")

                    if re.match(spec_a, spec_b):
                        specifications[spec_a] = spec_b.split(": ")[1]
            product_info['Specifications'] = specifications

        # reviews(list)
        if "top_reviews" in data.keys():
            product_info['Reviews'] = [x["review_text"] for x in data["top_reviews"]]

        # 其他評分項目(dict)
        other_ratings = {}
        if "secondary_ratings_averages" in data.keys():
            for rating in data["secondary_ratings_averages"].values():
                other_ratings[rating["Id"]] = rating['AverageRating']
            product_info['other_ratings'] = other_ratings # 其他評分

        # 取catagory
        res_cat = requests.get(product_info['url'], headers = headers)
        soup_cat = BeautifulSoup(res_cat.text,'html.parser')

        product_cats = soup_cat.select('span[itemprop]')
        product_info['category'] = [x.text for x in product_cats]

        print(product_info)
        product_result.append(product_info)
        print("============")
        
with open('product_info.json', 'w') as outfile:
    json.dump(product_result, outfile,ensure_ascii=False)
