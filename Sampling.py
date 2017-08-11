import random

import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split


# Method to take a random split of valid and pre-generated gait analysis data, splitting into training data
# and validation data
def get_from_db(db):
    #PERCENT_VALIDATION = 0.25
    #training_periods = []
    #validation_periods = []
    all_data = []
    periods = db.sensingperiods.find({"$and":[
        {"completeMotionData": True},
        {"completeLocationData":True},
        {"completeAudioData": True}
        #{"completeScreenData": True},
        #{"completeBatteryData": True},
        #{"completeGyroscopeData": True}
    ]})
    for period in periods:
        if "features" in period.keys():
            for subperiod in period["features"].keys():
                subperiod_data = period["features"][subperiod]
                all_data.append((period, subperiod_data))

    return all_data


def partition_and_label_data(data):
    data_list = []
    for d in data:
        period = d[0]
        subperiod_data = d[1]
        gait = subperiod_data["gait"]
        location = subperiod_data["location"]
        audio = subperiod_data["audio"]
        # screen = subperiod_data["screen"]
        # battery = subperiod_data["battery"]
        all_stats = {}
        all_stats.update(gait)
        all_stats.update(location)
        all_stats.update(audio)
        # all_stats.update(screen)
        # all_stats.update(battery)

        survey = period["survey"]
        if survey is not None:
            all_stats["didDrink"] = survey["didDrink"]
            all_stats["drinkUnits"] = survey["units"]
            all_stats["drinkFeeling"] = survey["feeling"]
            all_stats["drinkRating"] = survey["drinkRating"]

            data_list.append(all_stats)

    df = pd.DataFrame(data_list)

    df = df[df["cadence"] < 9999]
    df = df[df["duration"] > 30]
    df = df[df["step_count"] > 15]

    #SET CLASS RULE HERE
    df["drunk"] = np.where((df["didDrink"] == False), 0, np.where((df["drinkRating"] <= 8), 1, 2))
    # df["drunk"] = np.where((df["didDrink"] == False), 0, 1)

    training_df, validation_df = train_test_split(df, test_size=0.25)

    return training_df, validation_df


def get_training_inputs(df, selected_features):
    features = df.as_matrix(selected_features)
    targets = df.as_matrix(["drunk"]).ravel()
    return features, targets


def get_validation_inputs(df, selected_features):
    number_per_class = 18
    classes = df["drunk"].unique()

    filtered_samples = []
    for cl in np.nditer(classes):
        sub_df = df[df["drunk"] == cl]
        #print(cl, len(sub_df.index))
        sub_df = sub_df.sample(n=number_per_class)
        filtered_samples.append(sub_df)

    filtered_df = pd.concat(filtered_samples)

    features = filtered_df.as_matrix(selected_features)
    targets = filtered_df.as_matrix(["drunk"]).ravel()

    return features, targets





