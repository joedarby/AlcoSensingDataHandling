import traceback
from multiprocessing import Pool
from pprint import pprint

import numpy as np
import pandas as pd
from pymongo import MongoClient

import Data_Tools

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
    #db.sensingperiods.update({}, {"$unset": {"features": 1}}, multi=True)
    periods = db.sensingperiods.find({"$and": [
        {"completeMotionData": True},
        {"completeLocationData": True},
        {"completeAudioData": True},
        #{"completeScreenData": True},
        #{"completeBatteryData": True},
        {"completeGyroscopeData": True}
    ]})


    pool = Pool()
    pool.map(get_stats_wrapped, periods)
    pool.close()
    pool.join()


# Try/except wrapper for get_stats
def get_stats_wrapped(period):
    try:
        #update_walking_stats_for_period(period)
        #update_location_stats_for_period(period)
        #update_audio_stats_for_period(period)
        #update_screen_stats_for_period(period)
        #update_battery_stats_for_period(period)
        update_gyroscope_stats_for_period(period)


    except Exception as ex:
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        print(message)
        traceback.print_tb(ex.__traceback__)


# Method to generate gait analysis data from a given sensing period. Used by generate_features. Updates gait
# analysis data into the db
def update_walking_stats_for_period(sensing_period):
    s_period_id = sensing_period["_id"]
    raw_dataframe = Data_Tools.get_accel_and_motion(db, s_period_id)
    walking_dfs = Data_Tools.split_out_walking_periods(raw_dataframe)
    filtered_dfs = Data_Tools.filter_walking_periods(walking_dfs, sd_val)

    sub_period_features = []
    for df in filtered_dfs:
        gait_stats = Data_Tools.get_walking_statistics(df, prt_val)
        sub_period_features.append(gait_stats)

    features = {}
    for i, stats in enumerate(sub_period_features):
        sub_period_string = "sub_period_" + str(i)
        features[sub_period_string] = {}
        features[sub_period_string]["gait"] = stats

    db.sensingperiods.update_one({"_id": s_period_id}, {"$set": {"features": features}}, upsert=False)
    print("period processed")


def update_location_stats_for_period(sensingperiod):

    sp_list = sensingperiod["features"]
    s_period_id = sensingperiod["_id"]
    for subperiod in sp_list.keys():

        data = sp_list.get(subperiod)
        start = data["gait"]["start"]
        raw_dataframe = Data_Tools.get_subperiod_location(db, s_period_id, start)
        location_features = Data_Tools.get_location_features(db, s_period_id, raw_dataframe)

        db_string = "features." + subperiod + ".location"
        print(location_features)

        db.sensingperiods.update_one({"_id": s_period_id}, {"$set": {db_string: location_features}}, upsert=False)

def update_audio_stats_for_period(sensingperiod):
    sp_list = sensingperiod["features"]
    s_period_id = sensingperiod["_id"]

    for subperiod in sp_list.keys():

        data = sp_list.get(subperiod)
        start = data["gait"]["start"]
        end = data["gait"]["end"]

        raw_dataframe = Data_Tools.get_subperiod_audio(db, s_period_id, start, end)
        audio_features = Data_Tools.get_audio_features(raw_dataframe)

        db_string = "features." + subperiod + ".audio"
        print(audio_features)

        db.sensingperiods.update_one({"_id": s_period_id}, {"$set": {db_string: audio_features}}, upsert=False)

    print("period processed")

def update_screen_stats_for_period(sensingperiod):
    sp_list = sensingperiod["features"]
    s_period_id = sensingperiod["_id"]

    for subperiod in sp_list.keys():
        data = sp_list.get(subperiod)
        start = data["gait"]["start"]
        end = data["gait"]["end"]

        raw_dataframe = Data_Tools.get_sub_period_screen(db, s_period_id, start, end)
        screen_features = Data_Tools.get_screen_features(raw_dataframe, start, end)

        db_string = "features." + subperiod + ".screen"
        print(screen_features)

        db.sensingperiods.update_one({"_id": s_period_id}, {"$set": {db_string: screen_features}}, upsert=False)

    print("period processed")

def update_battery_stats_for_period(sensingperiod):
    sp_list = sensingperiod["features"]
    s_period_id = sensingperiod["_id"]

    for subperiod in sp_list.keys():
        data = sp_list.get(subperiod)
        start = data["gait"]["start"]
        end = data["gait"]["end"]

        raw_dataframe = Data_Tools.get_subperiod_battery(db, s_period_id, start, end)
        battery_features = Data_Tools.get_battery_features(raw_dataframe)

        db_string = "features." + subperiod + ".battery"
        print(battery_features)

        db.sensingperiods.update_one({"_id": s_period_id}, {"$set": {db_string: battery_features}}, upsert=False)


    print("period processed")

def update_gyroscope_stats_for_period(sensingperiod):
    sp_list = sensingperiod["features"]
    s_period_id = sensingperiod["_id"]

    for subperiod in sp_list.keys():
        data = sp_list.get(subperiod)
        start = data["gait"]["start"]
        end = data["gait"]["end"]

        raw_dataframe = Data_Tools.get_subperiod_gyroscope(db, s_period_id, start, end)
        gyroscope_features = Data_Tools.get_gyroscope_features(raw_dataframe)

        db_string = "features." + subperiod + ".gyroscope"
        print(gyroscope_features)

        db.sensingperiods.update_one({"_id": s_period_id}, {"$set": {db_string: gyroscope_features}}, upsert=False)


    print("period processed")


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



def summarise_data():
    dbClient = MongoClient()
    global db
    db = dbClient.alcosensing
    periods = db.sensingperiods.find()
    #total_periods = periods.count()
    #total_with_motion = db.sensingperiods.find({"completeMotionData": True}).count()
    good = 0
    no_good = 0
    sections = 0
    drunk_sections = 0
    some_drink_sections = 0
    sober_sections = 0

    for period in periods:
        pprint(period)

    '''
    for period in periods:
        id = period["_id"]
        pprint(period)
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
    '''
    #print(total_periods)
    #print(total_with_motion)
    #print(good, no_good)
    #print(sections, drunk_sections, some_drink_sections, sober_sections)

if __name__ == "__main__":
    np.set_printoptions(linewidth=640)
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 1000)

    generate_features(1.3, 6)
    #summarise_data()
