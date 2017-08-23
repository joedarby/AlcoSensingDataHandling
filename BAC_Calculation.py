from pymongo import MongoClient
from pprint import pprint
from datetime import datetime as dt

db = None

def update_eBAC():
    dbClient = MongoClient()
    global db
    db = dbClient.alcosensing

    periods = db.sensingperiods.find({"$and": [
        {"completeMotionData": True},
        {"completeLocationData": True},
        {"completeAudioData": True}
        # {"completeScreenData": True},
        # {"completeBatteryData": True},
        # {"completeGyroscopeData": True}
    ]})
    for period in periods:
        #pprint(period)
        survey = period["survey"]
        if survey is not None:
            calculate_subperiods(period)


def calculate_subperiods(period):
    s_period_id = period["_id"]
    periodStart, units, feeling, combined_score, gender, weight = get_period_info(period)
    unadjustedEBAC = 0 if units == 0 else widmark_calculation(gender, weight, units)
    if "features" in period.keys():
        for key in period["features"].keys():
            subPeriod = period["features"][key]
            subPeriodStart = subPeriod['gait']['start']
            timeDelta = subPeriodStart - periodStart
            minutesSincePeriodStart = 0

            if timeDelta.days >= 0:
                days = timeDelta.days
                seconds = timeDelta.seconds
                minutesSincePeriodStart = (days * 24 * 60) + (seconds / 60.0)

            consumptionProportion = 0.5 + (minutesSincePeriodStart / 240)
            if consumptionProportion > 1:
                consumptionProportion = 1

            timeAdjustedUnits = units * consumptionProportion
            timeAdjustedFeeling = feeling * consumptionProportion
            timeAdjustedScore = combined_score * consumptionProportion

            eBAC = 0 if units == 0 else widmark_calculation(gender, weight, timeAdjustedUnits)
            EBIS = (eBAC + 1) * (feeling + 1)
            timeAdjustedEBIS = EBIS * consumptionProportion
            res = {"eBAC_val" : unadjustedEBAC,
                   "EBIS" : EBIS,
                   "timeAdj_eBAC_val": eBAC,
                   "timeAdj_Units": timeAdjustedUnits,
                   "timeAdj_Feeling": timeAdjustedFeeling,
                   "timeAdj_CIS": timeAdjustedScore,
                   "timeAdj_EBIS": timeAdjustedEBIS}

            db_string = "features." + key + ".eBAC"
            db.sensingperiods.update_one({"_id": s_period_id}, {"$set": {db_string: res}}, upsert=False)




def get_period_info(period):
    survey = period["survey"]
    periodStartString = period["startTime"]
    periodStart = dt.strptime(periodStartString, "%Y-%m-%d-%H%%3A%M")
    units = survey['units']
    feeling = survey['feeling']
    combined_score = survey['drinkRating']
    userID = period['user']

    user = db.users.find_one({"_id": userID})
    gender = user['body']['gender']
    weight = user['body']['weight']

    return periodStart, units, feeling, combined_score, gender, weight

def widmark_calculation(gender, weight, units):

    c = 0.58 if gender == "Male" else 0.49

    BAC = (((0.806 * units * 1.2) / (c * weight)) - 0.017*0.5) * 10

    return BAC

