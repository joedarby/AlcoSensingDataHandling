import matplotlib.pyplot as plt
import numpy as np


def print_users(db):
    print("\n")
    header = ["ID", "email", "responses", "drink", "noDrink", "age", "height", "weight", "gender"]
    print("{: >20} {: >30} {: >8} {: >8} {: >8} {: >6} {: >6} {: >6} {: >10}".format(*header))
    for user in db.users.find():

        userID = user["_id"]
        email = user["body"]["email"]
        age = str(user["body"]["age"])
        height = str(user["body"]["height"])
        weight = str(user["body"]["weight"])
        gender = get_gender(user)
        count, did_drink, no_drink = get_survey_responses(db, userID)
        info = [userID, email, count, did_drink, no_drink, age, height, weight, gender]

        print("{: >20} {: >30} {: >8} {: >8} {: >8} {: >6} {: >6} {: >6} {: >10}".format(*info))


def get_survey_responses(db, userID):
    count = 0
    did_drink = 0
    no_drink = 0
    for period in db.sensingperiods.find({"user": userID}):
        result = period["survey"]
        if result is not None:
            value = result["responseCount"]
            if result["didDrink"] is True:
                did_drink += 1
            else:
                no_drink += 1
            if value > count:
                count = value
    return count, did_drink, no_drink


def get_gender(user):
    try:
        gender = str(user["body"]["gender"])
        return gender
    except:
        return "NA!"


def check_surveys(db):
    periods = db.sensingperiods.find()
    header = ["User", "StartTime", "DidDrink", "Units", "Feeling", "DrinkRating", "Category"]
    print("{: >25} {: >20} {: >8} {: >8} {: >8} {: >8} {: >8}".format(*header))
    male_x = []
    male_y = []
    female_x = []
    female_y = []
    threshold1 = 5
    threshold2 = 15
    cat0 = 0
    cat1 = 0
    cat2 = 0
    for period in periods:
        user = period["user"]
        start = period["startTime"]
        if "survey" in period.keys() and period["survey"] is not None:
            userInfo = db.users.find_one({"_id":user})
            gender = userInfo["body"]["gender"]
            survey = period["survey"]
            didDrink = survey["didDrink"]
            units = survey["units"]
            feeling = survey["feeling"]
            rating = survey["drinkRating"]
            category = 0 if ((didDrink) == False or (rating <= threshold1)) else (1 if rating <= threshold2 else 2)
            if category == 0:
                cat0 += 1
            elif category == 1:
                cat1 += 1
            elif category == 2:
                cat2 += 1
            result = [user, start, didDrink, units, feeling, rating, category]
            print("{: >25} {: >20} {: >8} {: >8} {: >8} {: >8} {: >8}".format(*result))
            if didDrink and gender == "Male":
                male_x.append(units)
                male_y.append(feeling)
            elif didDrink and gender == "Female":
                female_x.append(units)
                female_y.append(feeling)
        else:
            result = [user, start, "no survey"]
            print("{: >25} {: >20} {: >10}".format(*result))

    print(cat0, cat1, cat2)
    fig, ax = plt.subplots()
    ax.plot(male_x, male_y, 'o')
    ax.plot(female_x, female_y, 'x')

    range1 = np.array(range(0,threshold2))
    formula1 = "threshold2/(range1+1) - 1"
    ax.plot(range1, eval(formula1))

    range2 = np.array(range(0, threshold1))
    formula2 = "threshold1/(range2+1) - 1"
    ax.plot(range2, eval(formula2))


    plt.xlabel("Units")
    plt.ylabel("Feeling")
    plt.show()


def summarise_data_completeness(db):
    total = db.sensingperiods.find().count()
    with_motion = db.sensingperiods.find({"completeMotionData": True}).count()
    with_location = db.sensingperiods.find({"completeLocationData": True}).count()
    with_audio = db.sensingperiods.find({"completeAudioData": True}).count()
    with_screen = db.sensingperiods.find({"completeScreenData": True}).count()
    with_battery = db.sensingperiods.find({"completeBatteryData": True}).count()
    with_gyroscope = db.sensingperiods.find({"completeGyroscopeData": True}).count()

    with_all = db.sensingperiods.find({"$and": [{"completeMotionData": True},
                                                {"completeLocationData": True},
                                                {"completeAudioData": True},
                                                # {"completeBatteryData": True},
                                                {"completeGyroscopeData": True}]}).count()

    print(total, with_motion, with_location, with_audio, with_screen, with_battery, with_gyroscope, with_all)


def summarise_walking_periods(db):
    periods = db.sensingperiods.find()

    num_periods = periods.count()
    with_walking = 0
    total_subperiods = 0

    for period in periods:
        if "features" in period.keys():
            num_subperiods = len(period["features"].keys())
            if num_subperiods > 0:
                with_walking += 1
                total_subperiods += num_subperiods
            for subperiod in period["features"].keys():
                subperiod_data = period["features"][subperiod]

    print(num_periods, with_walking, total_subperiods)


def summarise_sensing_trigger_types(db):
    periods = db.sensingperiods.find({"$and": [{"completeMotionData": True}, {"completeLocationData": True}]})
    # periods = db.sensingperiods.find()
    count = periods.count()
    with_survey = 0
    trigger_known = 0
    trigger0 = 0
    trigger1 = 0
    trigger2 = 0
    for period in periods:
        if "survey" in period.keys():
            survey = period["survey"]
            if survey is not None:
                with_survey += 1
                if "triggerType" in survey.keys():
                    trigger_known += 1
                    type = survey["triggerType"]
                    if type == 0:
                        trigger0 += 1
                    elif type == 1:
                        trigger1 += 1
                    elif type == 2:
                        trigger2 += 1

    print(count, with_survey, trigger_known)
    print(trigger0, trigger1, trigger2)

