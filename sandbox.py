from pymongo import MongoClient
from pprint import pprint
import numpy as np
import pandas as pd
from multiprocessing import Pool
import random


import DB_Tools
import Data_Tools
import AWS_Tools
import RandomForest

np.set_printoptions(linewidth=640)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)




'''

user = db.users.find_one({"body.age":26})
pprint(user)
print("\n")
period = db.sensingperiods.find_one({"user": user["_id"]})
pprint(period)
print("\n")
data = db.data.find_one({"period": period["_id"]})
pprint(data)



allAccel = db.data.find({"sensorType": "Location"})

for file in allAccel:
    pprint(file)



print(db.data.find({"sensorType":"SurveyResult"}).count())



for period in db.sensingperiods.find():
    survey = period["survey"]
    if survey is not None:
        print(period["survey"]["spiritSingleCount"])



for period in db.sensingperiods.find({"survey.didDrink": True}):
    userID = period["user"]
    user = db.users.find_one({"_id": userID})
    if user is not None:
        if user["body"] is not None:
            print(user["body"]["email"] + "\n:\t" + str(period["survey"]["units"]) + "\n")




didDrink = 0
didntDrink = 0
for user in db.users.find():
    print(user['body']['email'])
    for period in db.sensingperiods.find({"user": user["_id"]}):
        print(period)
        if period is not None and (period['survey']) is not None:
            if (period['survey']['didDrink']) is True:
                didDrink += 1
            else:
                didntDrink += 1
    print("\n")
print(didDrink)
print(didntDrink)


'''


#period = db.sensingperiods.find_one({"$and": [{"completeMotionData": True}, {"user": "3fe685bc17774888"}]})
'''
period = db.sensingperiods.find_one({"completeMotionData": True})
id = period["_id"]
dfs_walking, dfs_non_walking = Data_Tools.get_data_split_by_walking(db, id)
walking_data = Data_Tools.get_walking_statistics(dfs_walking)
summary_df = pd.DataFrame(walking_data)
print(summary_df)

'''

dbClient = MongoClient()
db = dbClient.alcosensing

def main():

    #print(features, targets)

    #df_no_drink = df[df["drunk"] == 0]
    #df_moderate_drink = df[(df["didDrink"] == True) & (df["drinkFeeling"] < 2)]
    #df_high_drink = df[df["drunk"] == 1]

    #print("No drink count:" + str(df_no_drink.count()))
    #print("Drink count:" + str(df_high_drink.count()))

    #no_drink_stats = df_no_drink.mean()
    #moderate_drink_stats = df_moderate_drink.mean()
    #high_drink_stats = df_high_drink.mean()

    #stats = pd.concat([no_drink_stats, high_drink_stats], axis=1)
    #stats.columns = ["none", "high"]

    #print(stats)

    training_periods, validation_periods = split_data(db)
    training_features, training_targets = extract_data(training_periods)

    model = RandomForest.fit_forest(training_features, training_targets)

    accuracies = []

    for i in range(10):
        selected_periods = []
        for period in validation_periods:
            num = random.uniform(0, 1)
            if num > 0.5:
                selected_periods.append(period)
        validation_features, validation_targets = extract_data(selected_periods)
        accuracies.append(model.score(validation_features, validation_targets))

    print(accuracies)



def get_stats_wrapped(period):
    try:
        res = get_stats(period)
        return res
    except Exception as ex:
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        print(message)
        return None

def get_stats(period):
    id = period["_id"]
    userID = period["user"]
    userInfo = db.users.find_one({"_id": userID})["body"]
    dfs_walking, dfs_non_walking = Data_Tools.get_data_split_by_walking(db, id)
    walking_data = Data_Tools.get_walking_statistics(dfs_walking, period, userInfo)
    df = pd.DataFrame(walking_data)
    print("data processed")
    return df

def extract_data(periods):
    pool = Pool()
    results = pool.map(get_stats_wrapped, periods)
    pool.close()
    pool.join()

    df = pd.concat(results)
    df = df[df["cadence"] < 9999]
    df = df[df["duration"] > 30]
    df = df[df["step_count"] > 15]
    df["drunk"] = np.where((df["drinkFeeling"] < 2), 0, 1)


    features = df.as_matrix(["cadence", "step_time", "gait_stretch", "skewness", "kurtosis"])
    targets = df.as_matrix(["drunk"]).ravel()

    return features, targets

def split_data(db):
    training_periods = []
    validation_periods = []
    periods = db.sensingperiods.find({"completeMotionData": True})
    for period in periods:
        num = random.uniform(0, 1)
        if num > 0.3:
            training_periods.append(period)
        else:
            validation_periods.append(period)
    return training_periods, validation_periods


if __name__ == '__main__':
    main()

#df = Data_Tools.get_all_data_for_period(db, id)




#Data_Tools.plot_labelled_steps(df)



#df_walking = Data_Tools.get_step_labelled_walking_data(db, id)
#print(df_walking)






#df_motion = Data_Tools.get_motion_activity(db, period)
#df_accel = Data_Tools.get_accelerometer(db, period)

#Data_Tools.plot_file_data(df_motion, "walking", 1)
#Data_Tools.plot_general(df, "Accel_mag")
#Data_Tools.plot_labelled_steps(df_walking)


'''

DB_Tools.print_users(db)

'''







