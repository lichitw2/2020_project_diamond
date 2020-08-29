from flask import Flask, request, abort # 引用Web Server套件
from linebot import LineBotApi, WebhookHandler # 從linebot 套件包裡引用 LineBotApi 與 WebhookHandler 類別
from linebot.exceptions import InvalidSignatureError # 引用無效簽章錯誤

import gensim
from gensim.test.utils import datapath
from gensim.models import KeyedVectors

from linebot.models import TextSendMessage
from linebot.models import MessageEvent, TextMessage
from linebot.models.template import *
from linebot.models import FollowEvent

from linebot.models import PostbackEvent
from urllib.parse import parse_qs

from importData import connect_elasticsearch, search, insert_doc
import pandas as pd
import json
import time

# 讀取分析結果
model_ALS_rank = pd.read_csv("./model_results/model_ALS_rank.csv")
model_itembased_rank = pd.read_csv("./model_results/model_itembased_rank.csv")
alsResult_allPrediction = pd.read_csv("./model_results/alsResult_allPrediction.csv")

itembased_product_list = model_itembased_rank['product_id'].value_counts().keys().to_list()
avg_rating = alsResult_allPrediction.groupby("product_id")["prediction"].mean().sort_values(ascending=False)[:10].keys()

# 建立 Elasticsearch 連線
es = connect_elasticsearch()

# 讀取資料庫熱門商品
hot_query = {"aggs": {"product": {"terms": { "field": "product_id" }}}}
hot_result = es.search(index='orders', body=hot_query)["aggregations"]['product']['buckets']
hot_query_id = [item.get('key') for item in hot_result] 

#載入模型
model = gensim.models.Word2Vec.load("./model_results/product2vec.model")
#載入向量模型
wv_from_bin = KeyedVectors.load_word2vec_format("./model_results/product2vec.model.bin", binary=True)

# 訊息回覆function
def general_carousel_query(es, query_id):
    # 產生query列表
    tmp = [{"match": {"product_id": {"query": query_id[i]}}} for i in range(10)]
    query = {"query": {"bool": {"should": tmp}}}
    
    # query商品    
    items = search(es, "products", query)    
    item_list = [item['_source']['info_for_line'] for item in items]
    return item_list

def text_carousel_reply(event, text_message, item_list):
    if len(item_list) != 0:
        response = [
            {
                "type": "text",
                "text": text_message
            },{
                "type": "template",
                "altText": "this is a carousel template",
                "template": {
                    "type": "carousel",
                    "actions": [],
                    "columns": item_list
                }
            }]
    else:
        response = [
            {
                "type": "text",
                "text": text_message
            }
        ]
        
    result_message_array =[]

    for res in response:
        message_type = res.get('type')

        if message_type == 'text':
            result_message_array.append(TextSendMessage.new_from_json_dict(res))
        elif message_type == 'template':
            result_message_array.append(TemplateSendMessage.new_from_json_dict(res))

    line_bot_api.reply_message(event.reply_token, result_message_array)


# 載入基礎設定檔
secretFileContentJson=json.load(open("./line_secret_key",'r',encoding='utf8'))
server_url=secretFileContentJson.get("server_url")

# 設定Server啟用細節
app = Flask(__name__,static_url_path = "/material" , static_folder = "./material/")

# 生成實體物件
line_bot_api = LineBotApi(secretFileContentJson.get("channel_access_token"))
handler = WebhookHandler(secretFileContentJson.get("secret_key"))

# 啟動server對外接口，使Line能丟消息進來
@app.route("/", methods=['POST'])
def callback():
    # get X-Line-Signature header value
    signature = request.headers['X-Line-Signature']

    # get request body as text
    body = request.get_data(as_text=True)
    app.logger.info("Request body: " + body)

    # handle webhook body
    try:
        handler.handle(body, signature)
    except InvalidSignatureError:
        abort(400)
    return 'OK'

# 關注事件處理
@handler.add(FollowEvent)
def process_follow_event(event):
    
    # 將菜單綁定在用戶身上
    linkRichMenuId = secretFileContentJson.get("rich_menu_id")
    line_bot_api.link_rich_menu_to_user(event.source.user_id,linkRichMenuId)
    
    # 轉換回覆消息
    response = [{
        "type": "text",
        "text": "Welcome to The Diamond Store! Please select your preferred service from the menu below."
    }]
    
    result_message_array =[]
    result_message_array.append(TextSendMessage.new_from_json_dict(response[0]))    

    # 消息發送
    line_bot_api.reply_message(event.reply_token, result_message_array)


# 文字消息處理
@handler.add(MessageEvent,message=TextMessage)
def process_text_message(event):
    keywords = event.message.text

    query = {"query": {"match": {"product_info": {"query": keywords}}}}
    items = search(es, "products", query)

    item_list = []
    for item in items:
        item_list.append(item['_source']['info_for_line'])

    response = [
        {
            "type": "text",
            "text": "Here are the results for {}".format(keywords)
        },{
            "type": "template",
            "altText": "this is a carousel template",
            "template": {
                "type": "carousel",
                "actions": [],
                "columns": item_list
            }
        }]

    result_message_array =[]

    for res in response:

        message_type = res.get('type')

        if message_type == 'text':
            result_message_array.append(TextSendMessage.new_from_json_dict(res))

        elif message_type == 'template':
            result_message_array.append(TemplateSendMessage.new_from_json_dict(res))

    line_bot_api.reply_message(event.reply_token, result_message_array)


@handler.add(PostbackEvent)
def process_postback_event(event):

    query_string_dict = parse_qs(event.postback.data)
    print(query_string_dict)
    
    # (1)商品關鍵字搜尋
    if ('button' in query_string_dict) and (query_string_dict.get('button')[0]=='search'):
        response = [{
            "type": "text",
            "text": "Let’s find something for you! What’s on your list today?"
             }]
        
        result_message_array =[TextSendMessage.new_from_json_dict(response[0])]
        line_bot_api.reply_message(event.reply_token, result_message_array)
        
    # (2)使用者專屬優惠
    elif ('button' in query_string_dict) and (query_string_dict.get('button')[0]=='coupon'):

        #user_profile = line_bot_api.get_profile(event.source.user_id)
        #user_profile_dict = vars(user_profile)
        #user_id = user_profile_dict.get('user_id')
        
        query_id = model_ALS_rank[model_ALS_rank['user_id']==99522].loc[0:].iloc[0].to_list()
        text_message = "20% off items only for you! Use your ‘Diamond is the BEST -20’ coupon when checking out to get your discount 😚"
        
        item_list = general_carousel_query(es, query_id)
        text_carousel_reply(event, text_message, item_list)
    
    # (3)熱銷商品    
    elif ('button' in query_string_dict) and (query_string_dict.get('button')[0]=='hot'):
        
        #query = {"aggs": {"product": {"terms": { "field": "product_id" }}}}
        #result = es.search(index='orders', body=query)["aggregations"]['product']['buckets']
        #hot_query_id = [item.get('key') for item in result]
        
        query_id = hot_query_id 
        text_message = "We’ve listed the most popular products for you, please take a look!"
        
        item_list = general_carousel_query(es, query_id)
        text_carousel_reply(event, text_message, item_list)
            
    # (4)Shopping List
    elif ('button' in query_string_dict) and (query_string_dict.get('button')[0]=='list'):
        user_profile = line_bot_api.get_profile(event.source.user_id)
        user_profile_dict = vars(user_profile)
        
        user_id = user_profile_dict.get('user_id')
        
        query = {"size": 100,"query": {"match": {"user_id": {"query": user_id}}}}
        items = search(es, 'shopping_list', query)
        item_list = [item['_source']['info_for_line'] for item in items]
        
        if len(item_list) != 0:
            text_message = "My Shopping List : "
        else:
            text_message = "Add Something to the Shopping List!"
        text_carousel_reply(event, text_message, item_list)
        
    # （5）相關商品推薦（You might also like）    
    elif 'id' in query_string_dict:
        product_id = int(query_string_dict.get('id')[0])
        
        # item-based
        if (product_id in itembased_product_list) == True:
            query_id = model_itembased_rank[model_itembased_rank['product_id']== product_id].iloc[0,1:].to_list()
            print(query_id)
            print("item-based")
            if 0 in query_id:
                query_id = [product for product in query_id if (product != 0)]+ list(avg_rating)  # ALS preditons average
                print("item-based+ALS")

        # word2vec 
        else:
            query_id = [int(product[0]) for product in model.wv.most_similar(str(product_id))]
            print('word2vec')
            
        text_message = "You may also like the products below! Check it out!"
        item_list = general_carousel_query(es, query_id)
        text_carousel_reply(event, text_message, item_list)
    
    # (6)Add to List    
    elif 'add' in query_string_dict:

        user_profile = line_bot_api.get_profile(event.source.user_id)
        user_profile_dict = vars(user_profile)
        user_id = user_profile_dict.get('user_id')
        
        product_id = int(query_string_dict.get('add')[0])
        product_name = str(query_string_dict.get('name')[0])
        
        query = {"query": {"bool": {"should": [{"match": {"product_id": {"query": product_id}}}]}}}
        
        result = search(es, 'products', query)[0]['_source']['info_for_line']
        
        info_for_line = {
            
            "thumbnailImageUrl": result['thumbnailImageUrl'],
            "title": product_name,
            "text": result['text'],
            "actions": [{
                "type": "uri",
                "label": "More Info",
                "uri": result['actions'][0]['uri']
            },{
                "type": "postback",
                "label": "You May Also Like",
                "data": "id="+ str(product_id)
            },{
                "type": "postback",
                "label": "Remove from List",
                "data": "remove="+ str(product_id)
            }]
        }
        
        add = {"user_id": user_id, "product_id": product_id, "product_name":product_name, "info_for_line": info_for_line}
        insert_doc(es, "shopping_list", add)
        
        response = [
            {
                "type": "text",
                "text": "'{}' added to your list ".format(product_name)
            }]
        
        result_message_array =[TextSendMessage.new_from_json_dict(response[0])]
        line_bot_api.reply_message(event.reply_token, result_message_array)
        
    # (7) 移除收藏
    elif 'remove' in query_string_dict:
        user_profile = line_bot_api.get_profile(event.source.user_id)
        user_profile_dict = vars(user_profile)
        user_id = user_profile_dict.get('user_id')
        
        product_id = query_string_dict.get('remove')[0]
        
        # 刪除清單中該筆商品資料
        query = {
            "size": 1, 
            "query": {
                "bool": {
                    "should": [
                        {"match": { "product_id": { "query": product_id}}},
                        {"match": { "user_id": { "query": user_id}}}
                    ]
                }
            }
        }
        
        delete_id = search(es,'shopping_list', query)[0]['_id']
        es.delete(index='shopping_list', id=delete_id)
        
        time.sleep(1)
        
        # 回覆使用者更新後的Shopping List
        query = {"size": 100,"query": {"match": {"user_id": {"query": user_id}}}}
        items = search(es, 'shopping_list', query)
        item_list = [item['_source']['info_for_line'] for item in items]
        
        if len(item_list) != 0:
            text_message = "My New Shopping List : "
        else:
            text_message = "Add Something to the Shopping List!"
        text_carousel_reply(event, text_message, item_list)

if __name__ == "__main__":
    app.run(host='0.0.0.0')