import random
from multiprocessing import Pool

import numpy as np
import Data_Tools
import pandas as pd


# Method to recalculate gait analysis data if methodology changes. Parallelised.
def generate_features(db):
    periods = db.sensingperiods.find({"completeMotionData": True})
    pool = Pool()
    pool.map(get_stats_wrapped, periods)
    pool.close()
    pool.join()


# Try/except wrapper for get_stats
def get_stats_wrapped(period):
    try:
        get_stats(period)

    except Exception as ex:
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        print(message)


# Method to generate gait analysis data from a given sensing period. Used by generate_features. Updates gait
# analysis data into the db
def get_stats(db, period):
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


# Method to take a random split of valid and pre-generated gait analysis data, splitting into training data
# and validation data
def sample_data(db):
    PERCENT_VALIDATION = 0.5
    training_periods = []
    validation_periods = []
    all_data = []
    periods = db.sensingperiods.find({"completeMotionData": True})
    for period in periods:
        if "gait_stats" in period.keys():
            for key in period["gait_stats"].keys():
                all_data.append((period, period["gait_stats"].get(key)))
    for data in all_data:
        num = random.uniform(0, 1)
        if num > PERCENT_VALIDATION:
            training_periods.append(data)
        else:
            validation_periods.append(data)
    return training_periods, validation_periods


# Method to take data (at both training and validation stage), as lists of dictionaries, filter the data and return
# arrays which can be fed into sklearn model
def generate_model_inputs(data):
    data_list = []
    for d in data:
        period = d[0]
        stats = d[1]

        survey = period["survey"]
        if survey is not None:
            stats["didDrink"] = survey["didDrink"]
            stats["drinkUnits"] = survey["units"]
            stats["drinkFeeling"] = survey["feeling"]

            data_list.append(stats)

    df = pd.DataFrame(data_list)
    #print(df)

    df = df[df["cadence"] < 9999]
    df = df[df["duration"] > 30]
    df = df[df["step_count"] > 15]

    df["drunk"] = np.where((df["drinkFeeling"] < 2), 0, 1)

    features = df.as_matrix(["cadence", "step_time", "gait_stretch", "skewness", "kurtosis"])
    targets = df.as_matrix(["drunk"]).ravel()

    return features, targets