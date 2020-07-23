import json
from bs4 import BeautifulSoup
import re
import pandas as pd
import requests

def NameETL(product):
    product_name = BeautifulSoup(product.get("name"), 'html.parser')
    product['name'] = str(product_name).replace("&amp;", "&").replace('™', '').replace('”', '"').replace('’', '\'').replace('\s',"").strip()
    return product['name']
    
def PriceETL(product):
    if '-' in product['price']:
        x = product['price'].replace('$','').replace(',','').split(' - ')
        product['price'] = "{:.2f}".format((float(x[0])+float(x[1]))/2)
        
    elif "See low price in cart" in product['price']:
        tcin = product['url'].rsplit("-")[-1]
        
        url = 'https://redsky.target.com/web/pdp_location/v1/tcin/{}?pricing_store_id=3277&key=eb2551e4accc14f38cc42d32fbc2b2ea'.format(tcin)
        headers = {"User-agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36"}

        res = requests.get(url, headers=headers)
        json_res = json.loads(res.text)
        json_data = list(json_res.values())

        if 'child_items' in json_res.keys():
            product['price'] = json_data[3][-1]['price']['current_retail']
        else:
            product['price'] = json_data[1].get('current_retail')
    else:
        product['price'] = product['price'][1:]
        
    return product['price']
    
def Ratings(product):
    product["star_ratings"][1] = float(product["star_ratings"][1])
    product["star_ratings"] = tuple(product["star_ratings"])
    return product["star_ratings"]

# text cleansing_for general use
def TextClean(text):
    return text.replace("\n","").replace("●"," ").replace("*"," ").replace("｜"," ").replace("\\"," ").replace("™","").replace('\r', '').strip("-")

def Highlights(product):
    if product['highlights'] != "NA":
        sentence = " (see nutrition information for Saturated Fat, and Sodium content)"
        product["highlights"] = list(map(lambda x: x.replace(sentence, ""), product["highlights"]))
        product["highlights"] = list(map(TextClean, product["highlights"]))
        return product["highlights"]
        
def SpecTextClean(spec):
    k, v = spec[0], spec[1].strip()
    if k == "Net weight":
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
        product['description'] = TextClean(product['description'])
        if "PACKAGING MAY VARY BY LOCATION" or "½" in product['description'] :
            product['description'] = product['description'].replace("PACKAGING MAY VARY BY LOCATION","").replace("½","half")
        return product['description']

def ReviewTextClean(text):
    sentence = "This review was collected as part of a promotion."
    text = text.replace("*","").replace("❁","").replace("🤔","").replace(sentence,"").replace("™","").replace('\s',"")
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
        data.append(product)
        print(index,' completed')

    df = pd.DataFrame(data)
    df.to_csv('product_info_NY2_clensend.csv', encoding='utf-8',index = False)
        
if __name__ == "__main__":    
    with open('product_info_NY2_all.json', 'r', encoding="utf-8") as r:
        result = json.load(r)
    main()
