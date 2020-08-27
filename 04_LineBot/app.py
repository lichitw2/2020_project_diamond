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


# 讀取分析結果
model_ALS_rank = pd.read_csv("./material/model_ALS_rank.csv")
model_itembased_rank = pd.read_csv("./material/model_itembased_rank.csv")
alsResult_allPrediction = pd.read_csv("./material/alsResult_allPrediction.csv")

itembased_product_list = model_itembased_rank['product_id'].value_counts().keys().to_list()
avg_rating = alsResult_allPrediction.groupby("product_id")["prediction"].mean().sort_values(ascending=False)[:10].keys()

# 建立 Elasticsearch 連線
es = connect_elasticsearch()

#載入模型
model = gensim.models.Word2Vec.load("product2vec.model")
#載入向量模型
wv_from_bin = KeyedVectors.load_word2vec_format("product2vec.model.bin", binary=True)

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
    items = search(es, "product", query)

    item_list = []
    for item in items:
        item_list.append(item['_source']['info_for_line'])
        if len(item_list)==5:
            break

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
    
    if ('button' in query_string_dict) and (query_string_dict.get('button')[0]=='search'):
        response = [
             {
                 "type": "text",
                 "text": "Let’s find something for you! What’s on your list today?"
             }]
        
        result_message_array =[TextSendMessage.new_from_json_dict(response[0])]
        line_bot_api.reply_message(event.reply_token, result_message_array)
        
    
    elif ('button' in query_string_dict) and (query_string_dict.get('button')[0]=='coupon'):

        user_profile = line_bot_api.get_profile(event.source.user_id)
        user_profile_dict = vars(user_profile)
        print(user_profile_dict)
        
        user_id = user_profile_dict.get('user_id')
        query_id = model_ALS_rank[model_ALS_rank['user_id']==206198].loc[0][1:].to_list() # 148替換成假資料line_id
        
        query = {
            "query": {
                "bool": {
                    "should": [
                        { "match": { "product_id": { "query": query_id[0]} } },
                        { "match": { "product_id": { "query": query_id[1]} } },
                        { "match": { "product_id": { "query": query_id[2]} } },
                        { "match": { "product_id": { "query": query_id[3]} } },
                        { "match": { "product_id": { "query": query_id[4]} } }
                    ]
                }
            }
        }
        
        items = search(es, "product", query)
        
        item_list = []
        for item in items:
            item_list.append(item['_source']['info_for_line'])
            if len(item_list)==5:
                break
        
        response = [
            {
                "type": "text",
                "text": "20% off items only for you! Use your ‘Diamond is the BEST -20’ coupon when checking out to get your discount 😚"
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
    
    # 熱銷商品    
    elif ('button' in query_string_dict) and (query_string_dict.get('button')[0]=='hot'):
        
        query = {"aggs": {"product": {"terms": { "field": "product_id" }}}}
        result = es.search(index='orders', body=query)["aggregations"]['product']['buckets']

        query_id =[]
        for item in result:
            query_id.append(item.get('key'))
            
        query = {
            "query": {
                "bool": {
                    "should": [
                        { "match": { "product_id": { "query": query_id[0]} } },
                        { "match": { "product_id": { "query": query_id[1]} } },
                        { "match": { "product_id": { "query": query_id[2]} } },
                        { "match": { "product_id": { "query": query_id[3]} } },
                        { "match": { "product_id": { "query": query_id[4]} } }
                    ]
                }
            }
        }
        
        items = search(es, "product", query)
        
        item_list = []
        for item in items:
            item_list.append(item['_source']['info_for_line'])
            if len(item_list)==5:
                break
        
        response = [
            {
                "type": "text",
                "text": "We’ve listed the most popular products for you, please take a look!"
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
        
    # Shopping List
    elif ('button' in query_string_dict) and (query_string_dict.get('button')[0]=='list'):
        user_profile = line_bot_api.get_profile(event.source.user_id)
        user_profile_dict = vars(user_profile)
        
        user_id = user_profile_dict.get('user_id')
        
        query = {"query": {"match": {"user_id": {"query": user_id}}}}
        items = search(es, "shopping_list", query)
        
        item_list = ''
        for index, item in enumerate(items):
            item_list += '({}) {}\n'.format(index+1, item['_source']['product_name'])

        response= [
            {
                "type": "text",
                "text": "My Shopping List :\n\n{}".format(item_list)
            }]
        
        result_message_array =[TextSendMessage.new_from_json_dict(response[0])]
        line_bot_api.reply_message(event.reply_token, result_message_array)
        
    # You might also like    
    elif 'id' in query_string_dict:
        product_id = int(query_string_dict.get('id')[0])
        
        # item-based
        if (product_id in itembased_product_list) == True:
            query_id = model_itembased_rank[model_itembased_rank['product_id']== product_id].iloc[0,1:].to_list()
            print("item-based")
            print(query_id)
            if ( 0 in query_id) == True:
                query_id = [product for product in query_id if (product != 0)]+ avg_rating  # ALS preditons average
                print(query_id)
                print("item-based+ALS")

        # word2vec 
        else:
            query_id = [int(product[0]) for product in model.wv.most_similar(str(product_id))]
            print('word2vec')

        query = {
            "query": {
                "bool": {
                    "should": [
                        { "match": { "product_id": { "query": query_id[0]} } },
                        { "match": { "product_id": { "query": query_id[1]} } },
                        { "match": { "product_id": { "query": query_id[2]} } },
                        { "match": { "product_id": { "query": query_id[3]} } },
                        { "match": { "product_id": { "query": query_id[4]} } }
                    ]
                }
            }
        }
        
        items = search(es, "product", query)
        
        item_list = []
        for item in items:
            item_list.append(item['_source']['info_for_line'])
            if len(item_list)==5:
                break
        
        response = [
            {
                "type": "text",
                "text": "You may also like the products below! Check it out!"
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
    
    # Add to List    
    elif 'add' in query_string_dict:
        
        user_profile = line_bot_api.get_profile(event.source.user_id)
        user_profile_dict = vars(user_profile)
        print(user_profile_dict)
        user_id = user_profile_dict.get('user_id')
        
        product_id = int(query_string_dict.get('add')[0])
        product_name = str(query_string_dict.get('name')[0])
        
        add = {"user_id": user_id, "product_id": product_id, "product_name":product_name}
        insert_doc(es, "shopping_list", add)
        
        response = [
            {
                "type": "text",
                "text": "'{}' added to your list ".format(product_name)
            }]
        
        result_message_array =[TextSendMessage.new_from_json_dict(response[0])]
        line_bot_api.reply_message(event.reply_token, result_message_array)

if __name__ == "__main__":
    app.run(host='0.0.0.0')