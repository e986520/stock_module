import time
import pandas as pd
from datetime import datetime, timedelta
from pymongo import MongoClient

db = MongoClient(
    "mongodb+srv://Edward:ZdqXe5n8CGj9Rlut@stocks.6ow1n.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"
).stock_data


def save_to_mongo(func, coll, date_range=False):
    collection = db[coll]
    if date_range:
        for i, date in enumerate(date_range):
            print(f"crawling {coll} {date} ({i + 1}/{len(date_range)})")
            data = func(pd.to_datetime(date))
            if data is None:
                print("Fail! maybe it's a holiday.")
            else:
                collection.insert_many(data, ordered=True)
            time.sleep(10)
    else:
        data = func
        collection.insert_many(data, ordered=True)

    print("Success update new data!")

    if coll == "margin_trading" or coll == "legal_person":
        days = 1
        delete_data(days, collection)
    elif coll == "ADL":
        days = 365
        delete_data(days, collection)
    elif coll == "price" or coll == "monthly_revenue":
        days = 1111
        delete_data(days, collection)


def get_data_by_stock_id(coll, stock_id, item=False, days=False):
    coll = db[coll]
    if days:
        timestamp = to_timestamp(datetime.today() - timedelta(days=days + 1))
        condition1 = {"stock_id": stock_id}
        condition2 = {"date": {"$gte": timestamp}}
        datas = coll.find({"$and": [condition1, condition2]})
    else:
        datas = coll.find({"stock_id": stock_id})

    df = pd.DataFrame(datas).drop(columns=["_id"])
    df["date"] = pd.to_datetime(df["date"], unit="ms")
    df = df.set_index("date")
    if item:
        df = df[item]

    return df


def get_data(coll, item=False, days=False):

    coll = db[coll]

    if days:
        timestamp = to_timestamp(datetime.today() - timedelta(days=days + 1))
        datas = coll.find({"date": {"$gte": timestamp}})
    else:
        datas = coll.find()

    df = pd.DataFrame(datas).drop(columns="_id")

    try:
        df["date"] = pd.to_datetime(df["date"], unit="ms")
    except:
        return df

    if item:
        try:
            df = df.set_index(["date", "stock_id"])
            df = pd.DataFrame(df[item]).T.stack(level=0)
        except:
            df = df[item]

    return df


def delete_data(days, collection):
    timestamp = to_timestamp(datetime.today() - timedelta(days=days + 1))
    collection.delete_many({"date": {"$lt": timestamp}})
    print("Success delete old data!")


def to_timestamp(dt):
    epoch = datetime.utcfromtimestamp(0)
    return (dt - epoch).total_seconds() * 1000.0
