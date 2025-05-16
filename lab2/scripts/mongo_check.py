from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")
db = client["sales_reports"]

for doc in db["top10_products"].find().limit(5):
    print(doc)
