import requests
import json
import pandas as pd

def crawler(tcin):
    url = 'https://redsky.target.com/web/pdp_location/v1/tcin/{}?pricing_store_id=3277&key=eb2551e4accc14f38cc42d32fbc2b2ea'.format(tcin)
    headers = {"User-agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36"}
    
    res = requests.get(url, headers=headers)
    json_res = json.loads(res.text)
    json_data = list(json_res.values())
    
    if 'child_items' in json_res.keys():
        price = json_data[3][-1]['price']['current_retail']
        
    else:
        price = json_data[1].get('current_retail')
    return price

if __name__ == "__main__":
    # 讀取 url 列表，並取出商品 tcin
    df = pd.read_csv('product_info_NY2_clensend.csv')
    df2 = df[df['price'] == 'See low price in cart']
    url_dict = df2['url'].to_dict()
    
    for key in url_dict:
    url_dict[key] = url_dict[key].rsplit("-")[-1]

    result = [crawler(tcin) for tcin in url_dict.values()]
    print(result)


## 尚未完成寫回csv檔
