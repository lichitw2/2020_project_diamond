import json
import pandas as pd

def BrandCheck(result):
    brand_count = 0
    for product in result:
        if product['brand'] == "NA":
            brand_count += 1
    print("product without brand: %s" %brand_count, '\n')
    print("===========")

def PriceCheck(result):
    price_count = 0
    product_index = []
    product_name = []
    product_url = []
    for count, product in enumerate(result):
        if 'See low price in cart' == product['price']:
            product_index.append(count)
            product_name.append(product['name'])
            product_url.append(product['url'])
            df = pd.DataFrame({'product_index':product_index,'product_name':product_name,'product_url':product_url})
            df.to_csv("price_url.csv",index = False, sep=',')
        
        elif product['price'] == "NA":
            price_count += 1
            print(product['name'])
            print(product['url'],'\n')
                     
    print("===========")
    print("product without price: %s" %price_count, '\n')
    print("===========")        
    
def CategoryCheck(result):
    cat_count = 0
    for product in result:
        if len(product.get('category')) == 0:
            cat_count += 1
            print(product['keyword'])
            print(product['name'])
            print(product['url'],'\n')
    print("product without category: %s" %cat_count, '\n')
    print("===========")

def AtaGlance(result):
    ata_count = 0
    for product in result:
        if product['at_a_glance'] == 'NA':
            ata_count += 1
    print("product without at_a_glance: %s" %ata_count, '\n')


if __name__ == "__main__":    
    with open('product_info_NY2_all.json', 'r', encoding="utf-8") as r:
        result = json.load(r)
        
    BrandCheck(result)
    PriceCheck(result)
    CategoryCheck(result)
    AtaGlance(result)
