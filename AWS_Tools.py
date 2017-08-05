import json
import os
from pprint import pprint
import gzip
import csv

DIRECTORY = "/home/joe/dev/MSc/data/"


def update_users(dataBucket, db):
    path = DIRECTORY
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
    path = DIRECTORY
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
        #pprint(db.sensingperiods.find_one({"_id": period["_id"]}))
        #print("\n")

def insert_drink_rating(db):
    for period in db.sensingperiods.find():
        periodID = period["_id"]
        if "survey" in period.keys() and period["survey"] is not None:
            survey = period["survey"]
            if "drinkRating" not in survey.keys():
                units = survey["units"]
                feeling = survey["feeling"]
                drinkRating = (units + 1) * (feeling + 1)
                db.sensingperiods.update_one({"_id": periodID}, {"$set": {"survey.drinkRating": drinkRating}}, upsert=False)


def check_data_complete(db):
    path = DIRECTORY
    for period in db.sensingperiods.find():
        periodID = period["_id"]
        user = period["user"]
        time = period["startTime"]
        #if "completeData" not in period.keys():
        if True:
            directory = path + user + "/" + time
            list = os.listdir(directory)
            dataOK = filesOK(list, directory)
            motionDataOK, locationDataOK, audioOK = motionFilesOK(list, directory)
            if dataOK:
                db.sensingperiods.update_one({"_id": periodID}, {"$set": {"completeData": True}},
                                             upsert=False)
            else:
                db.sensingperiods.update_one({"_id": periodID}, {"$set": {"completeData": False}},
                                             upsert=False)
            if motionDataOK:
                db.sensingperiods.update_one({"_id": periodID}, {"$set": {"completeMotionData": True}},
                                             upsert=False)
            else:
                db.sensingperiods.update_one({"_id": periodID}, {"$set": {"completeMotionData": False}},
                                             upsert=False)
            if locationDataOK:
                db.sensingperiods.update_one({"_id": periodID}, {"$set": {"completeLocationData": True}},
                                             upsert=False)
            else:
                db.sensingperiods.update_one({"_id": periodID}, {"$set": {"completeLocationData": False}},
                                             upsert=False)
            if audioOK:
                db.sensingperiods.update_one({"_id": periodID}, {"$set": {"completeAudioData": True}},
                                             upsert=False)
            else:
                db.sensingperiods.update_one({"_id": periodID}, {"$set": {"completeAudioData": False}},
                                             upsert=False)
            period = db.sensingperiods.find_one({"_id":periodID})
            print(periodID, dataOK, period["completeData"], period["completeMotionData"], period["completeLocationData"], period["completeAudioData"])

def filesOK(list, directory):
    if len(list) < 9:
        return False
    if len(list) >= 9:
        for file in list:
            if file[-6:] == "csv.gz":
                filePath = directory + "/" + file
                if get_file_length(filePath) < 5:
                    print(file + "has " + str(get_file_length(filePath)) + " rows")
                    return False
    return True

def motionFilesOK(list, directory):
    accelOK = False
    motionOK = False
    locationOK = False
    audioOK = False
    for file in list:
        filePath = directory + "/" + file
        if "Accelerometer" in file:
            if get_file_length(filePath) > 100:
                accelOK = True
        if "MotionActivity" in file:
            if get_file_length(filePath) > 10:
                motionOK = True
        if "Location" in file:
            if get_file_length(filePath) > 3:
                locationOK = True
        if "Audio" in file:
            if get_file_length(filePath) > 20:
                audioOK = True

    return (motionOK and accelOK), locationOK, audioOK




def get_file_length(filePath):
    with gzip.open(filePath, 'r') as f:
        row_count = sum(1 for line in f)
    return row_count


def fix_consent(db):
    users = db.users.find()

    for user in users:
        id = user["_id"]
        path = user["consentFilePath"]
        print(path)
        with open(path, 'r') as file:

            jsonFile = json.loads(file.read())
            db.users.update_one({"_id": id}, {"$set": {"body": jsonFile}}, upsert=False)
