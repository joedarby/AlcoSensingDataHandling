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
    header = ["User", "StartTime", "DidDrink", "Units", "Feeling"]
    print("{: >25} {: >20} {: >8} {: >8} {: >8}".format(*header))
    for period in periods:
        user = period["user"]
        start = period["startTime"]
        if "survey" in period.keys() and period["survey"] is not None:
            survey = period["survey"]
            didDrink = survey["didDrink"]
            units = survey["units"]
            feeling = survey["feeling"]
            result = [user, start, didDrink, units, feeling]
            print("{: >25} {: >20} {: >8} {: >8} {: >8}".format(*result))
        else:
            result = [user, start, "no survey"]
            print("{: >25} {: >20} {: >10}".format(*result))
