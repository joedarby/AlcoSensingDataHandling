import random
from multiprocessing import Pool

import numpy as np
import Data_Tools
import pandas as pd
from pymongo import MongoClient

import sys
import os
import traceback

sd_val = 0
prt_val = 0
db = None

# Method to recalculate gait analysis data if methodology changes or new data received. Parallelised.
def generate_features(sd, prt):
    dbClient = MongoClient()
    global db
    db = dbClient.alcosensing
    global sd_val
    sd_val = sd
    global prt_val
    prt_val = prt
    db.sensingperiods.update({}, {"$unset": {"features": 1}}, multi=True)
    periods = db.sensingperiods.find({"completeMotionData": True})

    pool = Pool()
    pool.map(get_stats_wrapped, periods)
    pool.close()
    pool.join()



# Try/except wrapper for get_stats
def get_stats_wrapped(period):
    try:
        get_stats_for_period(period)

    except Exception as ex:
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        print(message)
        traceback.print_tb(ex.__traceback__)


# Method to generate gait analysis data from a given sensing period. Used by generate_features. Updates gait
# analysis data into the db
def get_stats_for_period(sensing_period):
    s_period_id = sensing_period["_id"]
    raw_dataframe = Data_Tools.get_accel_and_motion(db, s_period_id)
    walking_dfs = Data_Tools.split_out_walking_periods(raw_dataframe)
    filtered_dfs = Data_Tools.filter_walking_periods(walking_dfs, sd_val)

    sub_period_features = []
    for df in filtered_dfs:
        gait_stats = Data_Tools.get_walking_statistics(df, prt_val)
        if gait_stats is not None:
            location_stats = "to-do"
            if location_stats is not None:
                sub_period_features.append((gait_stats, location_stats))

    features = {}
    for i, stats in enumerate(sub_period_features):
        sub_period_string = "sub_period_" + str(i)
        features[sub_period_string] = {}
        features[sub_period_string]["gait"] = stats[0]
        features[sub_period_string]["location"] = stats[1]

    db.sensingperiods.update_one({"_id": s_period_id}, {"$set": {"features": features}}, upsert=False)
    res = db.sensingperiods.find_one({"_id": s_period_id})
    print(res)


# Method to take a random split of valid and pre-generated gait analysis data, splitting into training data
# and validation data
def sample_data(db):
    PERCENT_VALIDATION = 0.15
    training_periods = []
    validation_periods = []
    all_data = []
    #periods = db.sensingperiods.find({"completeMotionData": True})
    periods = db.sensingperiods.find({"$and":[{"completeMotionData": True}, {"completeLocationData":True}]})
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
            stats["drinkRating"] = survey["drinkRating"]

            data_list.append(stats)

    df = pd.DataFrame(data_list)
    #print(df)

    df = df[df["cadence"] < 9999]
    df = df[df["duration"] > 30]
    df = df[df["step_count"] > 15]

    df["drunk"] = np.where((df["didDrink"] == False), 0, np.where((df["drinkRating"] <= 8), 1,  2))



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



def summarise_data(db):
    periods = db.sensingperiods.find()
    total_periods = periods.count()
    total_with_motion = db.sensingperiods.find({"completeMotionData": True}).count()
    good = 0
    no_good = 0
    sections = 0
    drunk_sections = 0
    some_drink_sections = 0
    sober_sections = 0
    for period in periods:
        id = period["_id"]
        print(period)
        if "gait_stats" in period.keys():
            print(id + ": " + str(len(period["gait_stats"])))
            good += 1
            sections += len(period["gait_stats"])
            if ("survey" in period.keys()) and period["survey"] is not None:
                survey = period["survey"]
                if "feeling" in survey.keys():
                    drunk = survey["feeling"] >= 2
                    some_drink = survey["didDrink"] and survey["feeling"] < 2
                    if drunk:
                        drunk_sections += len(period["gait_stats"])
                    elif some_drink:
                        some_drink_sections += len(period["gait_stats"])
                    else:
                        sober_sections += len(period["gait_stats"])
        else:
            print(id + ": no valid walking sections")
            no_good += 1
    print(total_periods)
    print(total_with_motion)
    print(good, no_good)
    print(sections, drunk_sections, some_drink_sections, sober_sections)

if __name__ == "__main__":
    generate_features(1.3, 6)
    #summarise_data(db)
