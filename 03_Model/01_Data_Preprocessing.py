import pandas as pd

# 準備欄位為 user_id、product_id、reordered、product_name 的 data
train_orders = pd.read_csv("instacart_2017_05_01/order_products__train.csv")
prior_orders = pd.read_csv("instacart_2017_05_01/order_products__prior.csv")
orders = pd.read_csv("instacart_2017_05_01/orders.csv")
products = pd.read_csv("instacart_2017_05_01/products.csv").set_index('product_id')

orders_product = pd.concat([train_orders, prior_orders], axis=0, ignore_index=True)
join_orders = pd.merge(orders, orders_product, on = "order_id", how = 'inner')

product = products.drop(['aisle_id','department_id'],axis=1) 
rating = join_orders[["user_id","product_id","reordered"]].groupby(['user_id','product_id'],as_index=False).sum('reordered')

data = pd.merge(rating,product, on = "product_id", how='inner')
# data.head()

data.to_csv("data.csv", index=False)