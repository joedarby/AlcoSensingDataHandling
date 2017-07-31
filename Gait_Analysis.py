import random
from multiprocessing import Pool

import numpy as np
import Data_Tools
import pandas as pd
from pymongo import MongoClient


# Method to recalculate gait analysis data if methodology changes or new data received. Parallelised.
def generate_features():
    periods = db.sensingperiods.find({"completeMotionData": True})
    pool = Pool()
    pool.map(get_stats_wrapped, periods)


# Try/except wrapper for get_stats
def get_stats_wrapped(period):
    try:
        get_stats_for_period(period)

    except Exception as ex:
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        print(message)


# Method to generate gait analysis data from a given sensing period. Used by generate_features. Updates gait
# analysis data into the db
def get_stats_for_period(period):
    id = period["_id"]
    dfs_walking, dfs_non_walking = Data_Tools.get_data_split_by_walking(db, id)
    walking_data = Data_Tools.get_walking_statistics(dfs_walking)
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
    PERCENT_VALIDATION = 0.15
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
def generate_model_inputs(data, selected_features):
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

    #print_summary_statistics(df)


    features = df.as_matrix(selected_features)
    targets = df.as_matrix(["drunk"]).ravel()

    return features, targets


#  Method to print summary stats of gait analysis data
def print_summary_statistics(df):
    df_no_drink = df[df["drunk"] == 0]
    #df_moderate_drink = df[(df["didDrink"] == True) & (df["drinkFeeling"] < 2)]
    df_high_drink = df[df["drunk"] == 1]

    print("No drink count:" + str(len(df_no_drink.index)))
    print("Drink count:" + str(len(df_high_drink.index)))

    no_drink_stats = df_no_drink.mean()
    #moderate_drink_stats = df_moderate_drink.mean()
    high_drink_stats = df_high_drink.mean()

    stats = pd.concat([no_drink_stats, high_drink_stats], axis=1)
    stats.columns = ["none", "high"]

    print(stats)



def check_all_data(db):
    periods = db.sensingperiods.find()
    good = 0
    no_good = 0
    sections = 0
    for period in periods:
        id = period["_id"]
        if "gait_stats" in period.keys():
            print(id + ": " + str(len(period["gait_stats"])))
            good += 1
            sections += len(period["gait_stats"])
        else:
            print(id + ": no valid walking sections")
            no_good += 1
    print(good, no_good)
    print(sections)

if __name__ == "__main__":
    dbClient = MongoClient()
    db = dbClient.alcosensing
    generate_features()
