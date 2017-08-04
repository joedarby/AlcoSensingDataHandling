import boto3
from pymongo import MongoClient

import AWS_Tools
import DB_Tools

def main():
    s3 = boto3.resource('s3')
    dataBucket = s3.Bucket('jdarby-msc')

    dbClient = MongoClient()
    db = dbClient.alcosensing



    AWS_Tools.update_users(dataBucket, db)
    #AWS_Tools.check_files(dataBucket)
    AWS_Tools.update_files(dataBucket, db)
    AWS_Tools.update_survey_info(db)
    AWS_Tools.insert_drink_rating(db)
    AWS_Tools.check_data_complete(db)

    DB_Tools.print_users(db)


    DB_Tools.check_surveys(db)

    '''

    periods = db.sensingperiods.find({"$and":[{"completeMotionData" : True}, {"completeLocationData" : True}]})
    #periods = db.sensingperiods.find()
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

    periods = db.sensingperiods.find({"$and": [{"completeMotionData": True}, {"completeLocationData": True}]})
    sum_coords = 0
    for period in periods:
        id = period["_id"]
        locationEntry = db.data.find_one({"$and": [{"period":id}, {"sensorType": "Location"}]})
        locationFile = locationEntry["filePath"]
        print(locationFile)
        no_of_coords = AWS_Tools.get_file_length(locationFile)
        print(no_of_coords)
        sum_coords += no_of_coords
    print("Sum = " + str(sum_coords))
    '''




if __name__ == '__main__':
    main()

