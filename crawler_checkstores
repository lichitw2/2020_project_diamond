import pandas as pd
from selenium.webdriver import Chrome
from selenium.common.exceptions import NoSuchElementException
import time

# 讀取商品列表
df = pd.read_csv('products.csv')
product_list = df['product_name']

# selenium定位變數設定
HOME_URL = 'https://www.instacart.com/'

LOGIN_BUTTON = "//button[contains(text(),'Log in')]"
EMAIL_FIELD = "nextgen-authenticate.all.log_in_email" 
PＷD_FIELD = "nextgen-authenticate.all.log_in_password"
LOGIN_BUTTON_2 = "//div[3]//button[1]"

ACCOUNT_EMAIL = "XXXXXX"
ACCOUNT_PWD = "XXXXXX"

SEARCH_RESULT = "//span[contains(text(),'No search results for')]"

# 登入網站
driver = Chrome('./chromedriver')
driver.get(HOME_URL)
time.sleep(3)

login_btn = driver.find_element_by_xpath(LOGIN_BUTTON)
login_btn.click()
time.sleep(3)

email = driver.find_element_by_id(EMAIL_FIELD)
email.send_keys(ACCOUNT_EMAIL)
pwd = driver.find_element_by_id(PWD_FIELD)
pwd.send_keys(ACCOUNT_PWD)
time.sleep(2)

login_btn = driver.find_element_by_xpath(LOGIN_BUTTON_2)
login_btn.click()
time.sleep(10)

# 以specs為店家嘗試
result = {}
store = []
product_results = []

for i in range(len(product_list)):
    PRODUCT_NAME = product_list[i]
    CHECK_URL = "https://www.instacart.com/store/{}/search_v3/{}".format("specs",PRODUCT_NAME)
    driver.get(CHECK_URL)
    time.sleep(3)

    try:
        search_result = driver.find_element_by_xpath(SEARCH_RESULT)
        product_results.append(0)
        print("THERE IS NO RESUlTS OF " + PRODUCT_NAME)
    except NoSuchElementException as exception:
        product_results.append(1)

store.append("specs")
result["specs"] = product_results

result_df = pd.DataFrame(result, columns = store ,index = product_list[0:3])
result_df.to_csv('dfnew.csv')
