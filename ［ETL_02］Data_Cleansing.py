import requests, json, re
from bs4 import BeautifulSoup
import pandas as pd

def NameETL(product):
    product_name = BeautifulSoup(product.get("name"), 'html.parser')
    product['name'] = str(product_name).replace("&amp;", "&").replace('™', '').replace('”', '"').replace('’', '\'').replace('\s',"").strip()
    return product['name']

def PriceETL(product):
    tcin = product['url'].rsplit("-")[-1]

    url = 'https://redsky.target.com/web/pdp_location/v1/tcin/{}?pricing_store_id={}&key=eb2551e4accc14f38cc42d32fbc2b2ea'.format(tcin, store_id)
    headers = {"User-agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36"}
    
    if '-' in product['price']:
        x = product['price'].replace('$','').replace(',','').split(' - ')
        product['price'] = "{:.2f}".format((float(x[0])+float(x[1]))/2)
    
    elif "See low price in cart" in product['price']:
        res = requests.get(url, headers=headers)
        json_res = json.loads(res.text)
        json_data = list(json_res.values())

        if 'child_items' in json_res.keys():
            product['price'] = json_data[3][-1]['price']['current_retail']
        else:
            product['price'] = json_data[1].get('current_retail')
    
    elif product['price'] == "NA":
        res = requests.get(url, headers=headers)
        json_res = json.loads(res.text)
        json_data = list(json_res.values())
        product['price'] = json_data[1].get('current_retail')
             
    elif product['price'] == "Price Varies":
        product['price'] == "Price Varies"

    else:
        product['price'] = product['price'][1:]

    return product['price']

def CategoryFillNa(product):
    # 若category為NA或空list，則嘗試重新爬網取得
    if (product['category'] == 'NA') or (len(product['category']) == 0):
        url = product['url']
        headers = {"User-agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36"}
            
        res = requests.get(url, headers=headers)
        soup = BeautifulSoup(res.text, 'html.parser')
        product['category'] = [x.text for x in soup.select('a.Link-sc-1khjl8b-0.jNuccf')[1:]]
        
        # 若category依然為空值，則填入既有資料中相同keyword者的category眾值（若結果依然為空，後續再人為判斷處理方式）
        if (product['category'] == 'NA') or (len(product['category']) == 0):
            df_cat = pd.DataFrame(result)
            df_cat2 = df_cat[df_cat['keyword'] == product['keyword']]
            product['category'] = df_cat2['category'].value_counts().keys()[0]

        return product['category']
    
def Ratings(product):
    product["star_ratings"][1] = float(product["star_ratings"][1])
    product["star_ratings"] = tuple(product["star_ratings"])
    return product["star_ratings"]

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
        text = product['description'].strip()
        # define desired replacements
        rep = {
            '*': "", 
            '\n': " ",
            '●':"", 
            '½':'half',
            '|':'', 
            'PACKAGING MAY VARY BY LOCATION':"",
            'Packaging may vary by location.':'',
            '\r':""
        } 
        
        rep = dict((re.escape(k), v) for k, v in rep.items()) 
        pattern = re.compile("|".join(rep.keys()))
        product['description'] = pattern.sub(lambda m: rep[re.escape(m.group(0))], text)
        return product['description']

def ReviewTextClean(text):
    text = re.sub('\[.*promotion\.\]','', text)
    text = text.replace("*","").replace("❁","").replace("🤔","").replace("™","").replace('\s',"")
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
        CategoryFillNa(product)
        Ratings(product)
        Highlights(product)
        Specifications(product)
        Description(product)
        Review(product)
        data.append(product)
        print(index,' completed')
              
    df = pd.DataFrame(data)
    df.to_csv('product_info_NY2_clensend.csv', encoding='utf-8',index = False) #（1）請修改csv檔案路徑 
        
if __name__ == "__main__":
    with open('Raw_Data/product_info_NY2.json', 'r', encoding="utf-8") as r: #（2）請修改檔案路徑
        result = json.load(r)
      
    store_id = 3277  # (3）請修改store_id 
    main()
