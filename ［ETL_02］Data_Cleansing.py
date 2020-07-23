import json
from bs4 import BeautifulSoup
import re
import pandas as pd

def NameETL(product):
    if "&#" in product.get("name"):
        product_name = BeautifulSoup(product.get("name"), 'html.parser')
        product['name'] = str(product_name).replace("&amp;", "&")
    return product['name']
    
def PriceETL(product):
    if '-' in product['price']:
        x = product['price'].replace('$','').replace(',','').split(' - ')
        product['price'] = "{:.2f}".format((float(x[0])+float(x[1]))/2)
    elif "See low price in cart" in product['price']:
        product['price'] = "See low price in cart"
    else:
        product['price'] = product['price'][1:]
    return product['price']
    
# star_ratings 轉為 tuple，將後面評分人數改為 float
def Ratings(product):
    product["star_ratings"][1] = float(product["star_ratings"][1])
    product["star_ratings"] = tuple(product["star_ratings"])
    return product["star_ratings"]

# 文字處理：刪除「\n」、「●」、「*」、「｜」、「\」、「-」
def TextClean(text):
    return text.replace("\n","").replace("●"," ").replace("*"," ").replace("｜"," ").replace("\\"," ").strip("-")

def Highlights(product):
    if product['highlights'] != "NA":
        sentence = " (see nutrition information for Saturated Fat, and Sodium content)"
        product["highlights"] = list(map(lambda x: x.replace(sentence, ""), product["highlights"]))
        product["highlights"] = list(map(TextClean, product["highlights"]))
        return product["highlights"]
        
def SpecTextClean(spec):
    k, v = spec[0], spec[1].strip() # 去除每項前面的空白
    if k == "Net weight": # 單位小寫
        v = v.split(" ",1)
        if len(v) == 2:
            v[1] = v[1].lower()
        elif len(v) < 2:
            v.append("NA")
        v = tuple(v)
    return k, v

def Specifications(product):
    if product["specifications"] != "NA":
        product["specifications"] = dict(map(SpecTextClean, product["specifications"].items()))
        return product["specifications"]
        
def Description(product):
    if product['description'] != 'NA':
        list_des = []
        list_des.append(product['description'])
        product['description'] = list(map(TextClean, list_des))[0]
        if "PACKAGING MAY VARY BY LOCATION" or "½" in product['description'] :
            product['description'] = product['description'].replace("PACKAGING MAY VARY BY LOCATION","").replace("½","half")
        return product['description']
    
# review_Clean
def ReviewTextClean(text):
    sentence = "This review was collected as part of a promotion."
    text = text.replace("*","").replace("❁","").replace("🤔","").replace(sentence,"")
    text = re.sub(r"w/o\w*","without",text)
    text = re.sub(r"w/","with",text)
    text = re.sub(r"❤️+","❤️",text)
    x = re.compile(r'[❤️]+')
    text = x.sub("❤️",text)
    return text

def Review(product):
    if product["reviews"] != "NA":
        product["reviews"] = list(map(ReviewTextClean, product["reviews"]))
    return (product["reviews"])

def main():
    data = []
    for index, product in enumerate(result):
        NameETL(product)
        PriceETL(product)
        Ratings(product)
        Highlights(product)
        Specifications(product)
        Description(product)
        Review(product)
        print(index,' completed')
        
        data.append(product)
    df = pd.DataFrame(data)
    df.to_csv('product_info_NY2_clensend.csv', encoding='utf-8',index = False)
        
        
if __name__ == "__main__":    
    with open('product_info_NY2_all.json', 'r', encoding="utf-8") as r:
        result = json.load(r)
    main()
