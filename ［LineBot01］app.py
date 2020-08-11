from flask import Flask, request, abort # 引用Web Server套件
from linebot import (LineBotApi, WebhookHandler) # 從linebot 套件包裡引用 LineBotApi 與 WebhookHandler 類別
from linebot.exceptions import (InvalidSignatureError) # 引用無效簽章錯誤

from linebot.models import (ImagemapSendMessage,TextSendMessage,ImageSendMessage,LocationSendMessage,FlexSendMessage,VideoSendMessage)
from linebot.models.template import *

from linebot.models import (FollowEvent)
from linebot.models import (MessageEvent, TextMessage)

from elasticsearch import Elasticsearch

from confluent_kafka import Producer
import sys
import time

from linebot.models import (PostbackEvent)
from urllib.parse import parse_qs 

import json

# 載入基礎設定檔
secretFileContentJson=json.load(open("./line_secret_key",'r',encoding='utf8'))
server_url=secretFileContentJson.get("server_url")

# 設定Server啟用細節
app = Flask(__name__,static_url_path = "/素材" , static_folder = "./素材/")

# 生成實體物件
line_bot_api = LineBotApi(secretFileContentJson.get("channel_access_token"))
handler = WebhookHandler(secretFileContentJson.get("secret_key"))

# 建立Elasticsearch資料庫連線
es = Elasticsearch(['localhost:9200'])

# 建立Kafka連線
def error_cb(err):
    print('Error: %s' % err)

def delivery_callback(err, msg):
    if err:
        sys.stderr.write('%% Message failed delivery: %s\n' % err)

props = {
        # Kafka集群在那裡?
        'bootstrap.servers': 'localhost:9092',     # <-- 置換成要連接的Kafka集群
        'error_cb': error_cb                       # 設定接收error訊息的callback函數
    }

# 步驟2. 產生一個Kafka的Producer的實例
producer = Producer(props)

# 步驟3. 指定想要發佈訊息的topic名稱
topicName = 'linebot_test'

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
    
    # 讀取並轉換
    result_message_array =[]
    replyJsonPath = "素材/關注/reply.json"
    result_message_array = detect_json_array_to_new_message_array(replyJsonPath)

    # 消息發送
    line_bot_api.reply_message(
        event.reply_token,
        result_message_array
    )

# 文字消息處理
@handler.add(MessageEvent,message=TextMessage)
def process_text_message(event):
    
    if event.message.text not in ["價格","逛一下好康A"]:
        # 讀取本地檔案，並轉譯成消息
        result_message_array =[]
        replyJsonPath = "素材/"+event.message.text+"/reply.json"
        result_message_array = detect_json_array_to_new_message_array(replyJsonPath)
        
        # 發送
        line_bot_api.reply_message(
        event.reply_token,
        result_message_array
    )

# Postback Event處理
@handler.add(PostbackEvent)
def process_postback_event(event):

    query_string_dict = parse_qs(event.postback.data)

    print(query_string_dict)
    if 'folder' in query_string_dict: # 舊顧客
        
        # 1.提取user_id，進入資料庫抓消費資料
        user_profile = line_bot_api.get_profile(event.source.user_id)
        user_profile_dict = vars(user_profile)
        user_id = user_profile_dict.get('user_id')

        # 2. 從分析結果/模型獲得推薦給舊顧客的product_id，並至資料庫query商品資料
        
        # 3. query後得到推薦商品資訊(商品資訊schema參見"./ES_product_data_schema.json")
        A = es.get(index="product_test",id=1)['_source'] #id為實際product_id
        B = es.get(index="product_test",id=2)['_source']
        C = es.get(index="product_test",id=3)['_source']
        
        response = [
            {
                "type": "text",
                "text": "登登登，最新一期的好康A推薦在這裡！輸入折扣碼：diamond 即可享限時優惠喔！"
            },{
                "type": "template",
                "altText": "this is a carousel template",
                "template": {
                    "type": "carousel",
                    "actions": [],
                    "columns": [A,B,C]
                }
            }]
        
        # 4. 將最終product資訊回覆給使用者      
        result_message_array =[]
        
        for res in response:
            
            message_type = res.get('type')
            
            if message_type == 'text':
                result_message_array.append(TextSendMessage.new_from_json_dict(res))
            
            elif message_type == 'template':
                result_message_array.append(TemplateSendMessage.new_from_json_dict(res))
        
        line_bot_api.reply_message(
            event.reply_token,
            result_message_array
        )
        
    if 'tag' in query_string_dict: # 新顧客
        A = es.get(index="product_test",id=1)['_source'] #id為實際product_id
        B = es.get(index="product_test",id=2)['_source']
        C = es.get(index="product_test",id=3)['_source']
        
        response = [{
            "type": "text",
            "text": "依鑽石小隊看來，你可能感興趣的有...\n"
        },{
            "type": "template",
            "altText": "this is a carousel template",
            "template": {
                "type": "carousel",
                "actions": [],
                "columns": [A,B,C]
                }
            }]
        
        result_message_array =[]
        
        for res in response:
            
            message_type = res.get('type')
            
            if message_type == 'text':
                result_message_array.append(TextSendMessage.new_from_json_dict(res))
            
            elif message_type == 'template':
                result_message_array.append(TemplateSendMessage.new_from_json_dict(res))
        
        line_bot_api.reply_message(
            event.reply_token,
            result_message_array
        )
        
        # 將新顧客的點選紀錄/tag送入Kafka（kafka producer）
        try:
            user_profile = line_bot_api.get_profile(event.source.user_id)
            user_profile_dict = vars(user_profile)
            user_id = user_profile_dict.get('user_id')

            msg = str(query_string_dict.get('tag'))

            producer.produce(topicName, key = str(user_id),value=msg, callback=delivery_callback)
            producer.poll(0)  # 呼叫poll來讓client程式去檢查內部的Buffer, 並觸發callback

        except BufferError as e:
            # 錯誤處理
            sys.stderr.write('%% Local producer queue is full ({} messages awaiting delivery): try again\n'
                             .format(len(producer)))
        except Exception as e:
            print(e)
         
    if 'menu' in query_string_dict:
 
        linkRichMenuId = open("素材/"+query_string_dict.get('menu')[0]+'/rich_menu_id', 'r').read()
        line_bot_api.link_rich_menu_to_user(event.source.user_id,linkRichMenuId)
        
if __name__ == "__main__":
    app.run(host='0.0.0.0')
