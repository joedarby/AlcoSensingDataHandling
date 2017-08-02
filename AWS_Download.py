import boto3
from pymongo import MongoClient

import AWS_Tools
import DB_Tools

def main():
    s3 = boto3.resource('s3')
    dataBucket = s3.Bucket('jdarby-msc')

    dbClient = MongoClient()
    db = dbClient.alcosensing
    DB_Tools.check_surveys(db)

    '''
    AWS_Tools.update_users(dataBucket, db)
    #AWS_Tools.check_files(dataBucket)
    AWS_Tools.update_files(dataBucket, db)
    AWS_Tools.update_survey_info(db)
    AWS_Tools.check_data_complete(db)

    DB_Tools.print_users(db)
    '''



if __name__ == '__main__':
    main()

