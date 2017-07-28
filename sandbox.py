from pymongo import MongoClient
from pprint import pprint
import numpy as np
import pandas as pd


import DB_Tools
import Data_Tools
import AWS_Tools

np.set_printoptions(linewidth=640)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


dbClient = MongoClient()
db = dbClient.alcosensing

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
period = db.sensingperiods.find_one({"completeMotionData": True})
print(period)
id = period["_id"]

#df = Data_Tools.get_all_data_for_period(db, id)

dfs_walking, dfs_non_walking = Data_Tools.get_data_split_by_walking(db, id)

walking_data = []


for df in dfs_walking:
    df = Data_Tools.label_anti_steps(df)
    df_anti = df[df["anti_step"] == True]
    df_steps = df[df["step"] == True]
    start = df_steps.head(1).index.get_values()[0]
    end = df_steps.tail(1).index.get_values()[0]
    duration = (np.timedelta64(end - start, 's')).astype(int)

    step_count = df["step"].value_counts()[True]

    cadence = step_count / duration

    df_steps["step_time"] = df_steps.index.to_series().diff().astype('timedelta64[ms]')
    df_steps["step_time"] = np.where((df_steps["step_time"] < 2000), df_steps["step_time"], -1)
    df_steps["step_time"].fillna((-1), inplace=True)

    average_step_time = df_steps[df_steps["step_time"] > 0]["step_time"].mean() / 1000
    average_gait_stretch = df_anti["gait_stretch"].mean()

    skewness = df["Accel_mag"].skew()
    kurtosis = df["Accel_mag"].kurtosis()

    walking_data.append({"duration": duration,
                         "step_count": step_count,
                         "cadence": cadence,
                         "step_time": average_step_time,
                         "gait_stretch": average_gait_stretch,
                         "skewness": skewness,
                         "kurtosis":kurtosis})
    #Data_Tools.plot_labelled_steps(df)

pprint(walking_data)
print("\n")

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







