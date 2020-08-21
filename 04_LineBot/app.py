from flask import Flask, request, abort # 引用Web Server套件
from linebot import (LineBotApi, WebhookHandler) # 從linebot 套件包裡引用 LineBotApi 與 WebhookHandler 類別
from linebot.exceptions import (InvalidSignatureError) # 引用無效簽章錯誤

from linebot.models import (ImagemapSendMessage,TextSendMessage,ImageSendMessage,LocationSendMessage,FlexSendMessage,VideoSendMessage)
from linebot.models.template import *

from linebot.models import (FollowEvent)
from linebot.models import (MessageEvent, TextMessage)

from importData import connect_elasticsearch, search
import pandas as pd

from linebot.models import (PostbackEvent)
from urllib.parse import parse_qs 

import json

# 載入基礎設定檔
secretFileContentJson=json.load(open("./line_secret_key",'r',encoding='utf8'))
server_url=secretFileContentJson.get("server_url")

# 設定Server啟用細節
app = Flask(__name__,static_url_path = "/material" , static_folder = "./material/")

# 生成實體物件
line_bot_api = LineBotApi(secretFileContentJson.get("channel_access_token"))
handler = WebhookHandler(secretFileContentJson.get("secret_key"))

# 建立 Elasticsearch 連線
es = connect_elasticsearch()

# 讀取所需資料檔
model_ALS_rank = pd.read_csv("model_ALS_rank.csv")
model_itembased_rank = pd.read_csv("model_itembased_rank.csv")
alsResult_allPrediction = pd.read_csv("alsResult_allPrediction.csv")

itembased_product_list = model_itembased_rank['product_id'].value_counts().keys().to_list()
avg_rating = alsResult_allPrediction.groupby("product_id")["prediction"].mean().sort_values(ascending=False)[:10].keys()

def detect_json_array_to_new_message_array(fileName):
    
    #開啟檔案，轉成json
    with open(fileName) as f:
        jsonArray = json.load(f)
    
    # 解析json
    returnArray = []
    for jsonObject in jsonArray:

        # 讀取其用來判斷的元件
        message_type = jsonObject.get('type')
        
        # 轉換
        if message_type == 'text':
            returnArray.append(TextSendMessage.new_from_json_dict(jsonObject))
        elif message_type == 'imagemap':
            returnArray.append(ImagemapSendMessage.new_from_json_dict(jsonObject))
        elif message_type == 'template':
            returnArray.append(TemplateSendMessage.new_from_json_dict(jsonObject))
        elif message_type == 'image':
            returnArray.append(ImageSendMessage.new_from_json_dict(jsonObject))
        elif message_type == 'sticker':
            returnArray.append(StickerSendMessage.new_from_json_dict(jsonObject))  
        elif message_type == 'audio':
            returnArray.append(AudioSendMessage.new_from_json_dict(jsonObject))  
        elif message_type == 'location':
            returnArray.append(LocationSendMessage.new_from_json_dict(jsonObject))
        elif message_type == 'flex':
            returnArray.append(FlexSendMessage.new_from_json_dict(jsonObject))  
        elif message_type == 'video':
            returnArray.append(VideoSendMessage.new_from_json_dict(jsonObject))    
    return returnArray

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
    linkRichMenuId = open("material/rich_menu_1/rich_menu_id", 'r').read()
    line_bot_api.link_rich_menu_to_user(event.source.user_id,linkRichMenuId)
    
    # 讀取並轉換
    result_message_array =[]
    replyJsonPath = "material/welcome/reply.json"
    result_message_array = detect_json_array_to_new_message_array(replyJsonPath)

    # 消息發送
    line_bot_api.reply_message(event.reply_token, result_message_array)

# 文字消息處理
@handler.add(MessageEvent,message=TextMessage)
def process_text_message(event):
    
    if event.message.text == "Search Products":
        # 讀取本地檔案，並轉譯成消息
        result_message_array =[]
        replyJsonPath = "material/"+event.message.text+"/reply.json"
        result_message_array = detect_json_array_to_new_message_array(replyJsonPath)
        
        # 發送訊息給使用者
        line_bot_api.reply_message(event.reply_token, result_message_array)
    
    # elif event.message.text == "Buy Bithday Persent":
        
    elif event.message.text not in ["Search Products","Special Price","Buy Birthday Present"]:
        keywords = event.message.text
        
        query = {
            "query": {
                "match": {
                    "product_info": {
                        "query": keywords
                    }
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
                "text": "Results for ' "+ keywords + " '..."
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

# Postback Event處理
@handler.add(PostbackEvent)
def process_postback_event(event):

    query_string_dict = parse_qs(event.postback.data)

    print(query_string_dict)
    if ('folder' in query_string_dict) and (query_string_dict.get('folder')[0]=='Special Products'):

        user_profile = line_bot_api.get_profile(event.source.user_id)
        user_profile_dict = vars(user_profile)
        print(user_profile_dict)
        
        user_id = user_profile_dict.get('user_id')
        
        query_id = model_ALS_rank[model_ALS_rank['user_id']==148].loc[0][1:].to_list() # 148替換成假資料line_id(user_id)
        
        query = {
            "query": {
                "bool": {
                    "should": [
                        { "match": { "product_id": { "query": query_id[0]} } },
                        { "match": { "product_id": { "query": query_id[1]} } },
                        { "match": { "product_id": { "query": query_id[2]} } },
                        { "match": { "product_id": { "query": query_id[3]} } },
                        { "match": { "product_id": { "query": query_id[4]} } },
                    ]
                }
            }
        }
        
        items = search(es, "test2", query)
        
        item_list = []
        for item in items:
            item_list.append(item['_source']['info_for_line'])
            if len(item_list)==5:
                break
        
        response = [
            {
                "type": "text",
                "text": "Buy these goods with SPECIAL PRICE! Enter 'diamond_coupon' when check out!"
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
    
    if 'id1' in query_string_dict:
        product_id = int(query_string_dict.get('id1')[0])
        
        # ALS preditons average
        if (product_id in itembased_product_list) == True:
            query_id = model_itembased_rank[model_itembased_rank['product_id']== product_id].iloc[0,1:].to_list()
            print("item-based")

        # item-based
        else:
            query_id = avg_rating
            print("ALS preditons average")

        query = {
            "query": {
                "bool": {
                    "should": [
                        { "match": { "product_id": { "query": query_id[0]} } },
                        { "match": { "product_id": { "query": query_id[1]} } },
                        { "match": { "product_id": { "query": query_id[2]} } },
                        { "match": { "product_id": { "query": query_id[3]} } },
                        { "match": { "product_id": { "query": query_id[4]} } },
                    ]
                }
            }
        }
        
        items = search(es, "test2", query)
        
        item_list = []
        for item in items:
            item_list.append(item['_source']['info_for_line'])
            if len(item_list)==5:
                break
        
        response = [
            {
                "type": "text",
                "text": "You may also like ..."
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
        
if __name__ == "__main__":
    app.run(host='0.0.0.0')