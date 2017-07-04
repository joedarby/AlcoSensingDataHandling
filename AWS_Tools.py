import json
from pprint import pprint
import os


def update_users(dataBucket, db):
    path = "/home/joe/dev/MSc/data/"
    new = 0
    for file in dataBucket.objects.all():
        fileName = file.key
        if 'consent' in fileName:
            fileBody = file.get()['Body'].read().decode('utf-8')
            jsonFile = json.loads(fileBody)
            id = jsonFile["UID"]
            filePath = path + id + "/" + fileName
            if not os.path.exists(path + id):
                os.makedirs(path + id)
            if not os.path.exists(path + id + fileName):
                dataBucket.download_file(fileName, filePath)
            entry = db.users.find_one({"_id": id})
            if entry is None:
                print("New user:")
                new += 1
                db.users.insert_one({"body": jsonFile, "_id": id, "consentFilePath" : filePath})
                pprint(db.users.find_one({"_id": id}))

    print("\nThere are " + str(db.users.count()) + " users (" + str(new) + " are new sign ups).\n")


def check_files(dataBucket):
    sensors = {'Accelerometer': 0, 'Audio': 0, 'Battery': 0, 'Gyroscope': 0, 'Location': 0, 'Magnetometer': 0,
               'MotionActivity': 0, 'Notifications': 0, 'ScreenStatus': 0}
    surveys = 0
    for file in dataBucket.objects.all():
        fileName = file.key
        if 'consent' not in fileName and 'SurveyResult' not in fileName:
            sensor = fileName[36:-7]
            sensors[sensor] += 1
        elif 'SurveyResult' in fileName:
            surveys += 1

    pprint(sensors)
    pprint("\nThere have been %s surveys" % str(surveys))


def update_files(dataBucket, db):
    path = "/home/joe/dev/MSc/data/"
    for file in dataBucket.objects.all():
        fileName = file.key
        userID = fileName[:16]
        if 'consent' not in fileName:
            startTime = fileName[17:35]
            if not os.path.exists(path + userID+"/"+startTime):
                os.makedirs(path + userID+"/"+startTime)
                print("New sensing instance: " + userID + " " + startTime)
            if not os.path.exists(path + userID+"/"+startTime+"/"+fileName):
                dataBucket.download_file(fileName, path + userID+"/"+startTime+"/"+fileName)

            periodEntryID = userID + "-" + startTime
            periodEntry = db.sensingperiods.find_one({"_id": periodEntryID})
            if periodEntry is None:
                directory = path + userID+"/"+startTime+"/"
                db.sensingperiods.insert_one({"_id" : periodEntryID, "user" : userID, "startTime": startTime, "directory": directory})

            sensor = ""
            if 'SurveyResult' in fileName:
                sensor = 'SurveyResult'
            else:
                sensor = fileName[36:-7]
            dataEntryID = periodEntryID + "-" + sensor
            dataEntry = db.data.find_one({"_id": dataEntryID})
            if dataEntry is None:
                filePath = path + userID+"/"+startTime+"/"+fileName
                db.data.insert_one({"_id": dataEntryID, "period": periodEntryID, "user": userID, "sensorType": sensor, "filePath": filePath})


def update_survey_info(db):
    for period in db.sensingperiods.find():
        periodID = period["_id"]
        if "survey" not in period.keys():
            data = db.data.find_one({"$and": [{"period": periodID}, {"sensorType": "SurveyResult"}]})
            if data is not None:
                fileHandle = open(data["filePath"], "r")
                jsonContent = json.load(fileHandle)
                db.sensingperiods.update_one({"_id": periodID}, {"$set" : {"survey" : jsonContent}}, upsert=False)
            else:
                db.sensingperiods.update_one({"_id": periodID}, {"$set": {"survey": None}}, upsert=False)
        pprint(db.sensingperiods.find_one({"_id": period["_id"]}))
        print("\n")














