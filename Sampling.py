import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from pprint import pprint


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
        {"completeAudioData": True},
        #{"completeScreenData": True},
        #{"completeBatteryData": True},
        {"completeGyroscopeData": True}
    ]})
    for period in periods:
        if "features" in period.keys():
            for subperiod in period["features"].keys():
                subperiod_data = period["features"][subperiod]
                all_data.append((period, subperiod_data))

    return all_data


def label_data(data):
    data_list = []
    for d in data:
        period = d[0]
        subperiod_data = d[1]
        #pprint(subperiod_data)
        gait = subperiod_data["gait"]
        location = subperiod_data["location"]
        audio = subperiod_data["audio"]
        #screen = subperiod_data["screen"]
        #battery = subperiod_data["battery"]
        gyroscope = subperiod_data["gyroscope"]
        all_stats = {}
        all_stats.update(gait)
        all_stats.update(location)
        all_stats.update(audio)
        #all_stats.update(screen)
        #all_stats.update(battery)
        all_stats.update(gyroscope)

        survey = period["survey"]
        if survey is not None:
            eBAC_data = subperiod_data["eBAC"]
            all_stats["didDrink"] = survey["didDrink"]
            all_stats["Units"] = survey["units"]
            all_stats["Feeling"] = survey["feeling"]
            all_stats["CIS"] = survey["CIS"]
            all_stats["eBAC"] = eBAC_data["eBAC_val"]
            all_stats["EBIS"] = eBAC_data["EBIS"]
            all_stats["Units (timeAdj)"] = eBAC_data["timeAdj_Units"]
            all_stats["Feeling (timeAdj)"] = eBAC_data["timeAdj_Feeling"]
            all_stats["CIS (timeAdj)"] = eBAC_data["timeAdj_CIS"]
            all_stats["eBAC (timeAdj)"] = eBAC_data["timeAdj_eBAC_val"]
            all_stats["EBIS (timeAdj)"] = eBAC_data["timeAdj_EBIS"]

            all_stats["weighted_eBAC"] = eBAC_data["eBAC_val"] * ((survey["feeling"] + 1)*0.2)

            data_list.append(all_stats)

    df = pd.DataFrame(data_list)

    df = df[df["cadence"] < 9999]
    df = df[df["duration"] > 30]
    df = df[df["step_count"] > 15]

    #SET CLASS RULE HERE
    #df["drunk"] = np.where((df["didDrink"] == False), 0, np.where((df["eBAC"] <= 0.452931), 1, 2))

    #df["drunk"] = np.where((df["Units"] ==0), 0, 1)
    #df["drunk"] = np.where((df["Feeling (timeAdj)"] == 0), 0, 1)
    #df["drunk"] = np.where((df["eBAC (timeAdj)"] <0.45), 0, 1)
    #df["drunk"] = np.where((df["EBIS (timeAdj)"] < 1.45), 0, 1)

    df["drunk"] = np.where((df["Feeling (timeAdj)"] <=0), 0, np.where((df["Feeling (timeAdj)"] < 1), 1, 2))
    #df["drunk"] = np.where((df["eBAC (timeAdj)"] < 0.45), 0, np.where((df["eBAC (timeAdj)"] < 0.8), 1, 2))
    #df["drunk"] = np.where((df["EBIS (timeAdj)"] < 1.45), 0, np.where((df["EBIS (timeAdj)"] < 2.7), 1, 2))

    #df["drunk"] = np.where((df["timeAdjUnits"] <= 2), 0, 1)
    #df["drunk"] = np.where((df["drinkRating"] <= 5), 0, np.where((df["drinkRating"] <= 18), 1, 2))

    return df


def partition_data(df):
    training_df, validation_df = train_test_split(df, test_size=0.25)
    return training_df, validation_df


def validate_partition(df, min_samples_per_class):
    classes = df["drunk"].unique()
    results = []
    for cl in np.nditer(classes):
        sub_df = df[df["drunk"] == cl]
        length = len(sub_df.index)
        results.append(length >= min_samples_per_class)
    return all(results)


def get_training_inputs(df, selected_features):
    features = df.as_matrix(selected_features)
    targets = df.as_matrix(["drunk"]).ravel()
    return features, targets


def get_validation_inputs(df, selected_features, samples_per_class):
    classes = df["drunk"].unique()

    filtered_samples = []
    for cl in np.nditer(classes):
        sub_df = df[df["drunk"] == cl]
        #print(cl, len(sub_df.index))
        sub_df = sub_df.sample(n=samples_per_class)
        filtered_samples.append(sub_df)

    filtered_df = pd.concat(filtered_samples)

    features = filtered_df.as_matrix(selected_features)
    targets = filtered_df.as_matrix(["drunk"]).ravel()

    return features, targets





