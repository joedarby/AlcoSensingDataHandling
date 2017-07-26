from pymongo import MongoClient
from pprint import pprint
import numpy as np
import pandas as pd
import datetime as dt

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


user = db.users.find_one({"body.age":26})
user_id = user["_id"]
print(user["body"]["email"])


data = db.data.find_one({"sensorType":"Accelerometer", "user":user_id})
pprint(data)
period = data["period"]
pprint(period)

df = Data_Tools.get_all_data_for_period(db, period)



df_walking = df[df["Motion_walking"] == True]
df_walking = df_walking.dropna(subset=["Accel_mag"])

start = df_walking.head(1).index.values[0]
milli = 1000000
end = df_walking.tail(1).index.values[0]
length = end - start

def filter_steps(row):
    time = row.name + dt.timedelta(milliseconds=0.01)
    time_plus_half_sec = time + dt.timedelta(milliseconds=1000)
    if row["step"]:
        sub_df = df_walking[time : time_plus_half_sec]
        if True in sub_df["step"]:
            row["step"] = False
        else:
            row["step"] = True


df_walking["rolling_std_dev"] = pd.rolling_std(df_walking["Accel_mag"], 50)
df_walking["step"] = df_walking["Accel_mag"] < (df_walking["Accel_mag_avg"] - (1.3 * df_walking["rolling_std_dev"]))
df_walking.apply(lambda row: filter_steps(row), axis=1)

print(df_walking)




#df_motion = Data_Tools.get_motion_activity(db, period)
#df_accel = Data_Tools.get_accelerometer(db, period)

#Data_Tools.plot_file_data(df_motion, "walking", 1)
Data_Tools.plot_file_data(df_walking, "Accel_mag", 0.1)











