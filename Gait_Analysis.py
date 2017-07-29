from multiprocessing import Pool
from pymongo import MongoClient
import Data_Tools
import pandas as pd

dbClient = MongoClient()
db = dbClient.alcosensing

def generate_features(db):
    periods = db.sensingperiods.find({"completeMotionData": True})
    pool = Pool()
    pool.map(get_stats_wrapped, periods)
    pool.close()
    pool.join()


def get_stats_wrapped(period):
    try:
        get_stats(period)

    except Exception as ex:
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        print(message)


def get_stats(period):
    id = period["_id"]
    userID = period["user"]
    userInfo = db.users.find_one({"_id": userID})["body"]
    dfs_walking, dfs_non_walking = Data_Tools.get_data_split_by_walking(db, id)
    walking_data = Data_Tools.get_walking_statistics(dfs_walking, period, userInfo)
    if len(walking_data) > 0:
        df = pd.DataFrame(walking_data)
        df = df[df["cadence"] < 9999]
        df = df[df["duration"] > 30]
        df = df[df["step_count"] > 15]
        print("data processed")
        if len(df.index) > 0:
            dicts = df.to_dict(orient="records")
            i = 0
            main_dict = {}
            for dict in dicts:
                name = "gait" + str(i)
                main_dict[name] = dict
                i += 1
            db.sensingperiods.update_one({"_id": id}, {"$set":{"gait_stats": main_dict}}, upsert=False)


generate_features(db)