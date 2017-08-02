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
    threshold = 8
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
            category = 0 if (didDrink) == False else (1 if rating <= threshold else 2)
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

    range1 = np.array(range(0,threshold))
    formula1 = "threshold/(range1+1) - 1"
    ax.plot(range1, eval(formula1))


    plt.xlabel("Units")
    plt.ylabel("Feeling")
    plt.show()

