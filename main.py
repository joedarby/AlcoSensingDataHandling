import boto3
from pymongo import MongoClient

import AWS_Tools

s3 = boto3.resource('s3')
dataBucket = s3.Bucket('jdarby-msc')

dbClient = MongoClient()
db = dbClient.alcosensing

AWS_Tools.update_users(dataBucket, db)
#AWS_Tools.check_files(dataBucket)
AWS_Tools.update_files(dataBucket, db)
AWS_Tools.update_survey_info(db)