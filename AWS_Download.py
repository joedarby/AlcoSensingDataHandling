import boto3
from pymongo import MongoClient
from multiprocessing import Pool

import AWS_Tools
import DB_Tools



def main():
    s3 = boto3.resource('s3')
    dataBucket = s3.Bucket('jdarby-msc')

    dbClient = MongoClient()
    db = dbClient.alcosensing

    AWS_Tools.update_users(dataBucket, db)
    AWS_Tools.check_files(dataBucket)
    AWS_Tools.update_files(dataBucket, db)
    AWS_Tools.update_survey_info(db)
    AWS_Tools.calculate_combined_intoxication_score(db)

    pool = Pool()
    periods = db.sensingperiods.find()
    pool.map(AWS_Tools.check_data_complete, periods)
    pool.close()
    pool.join()

    DB_Tools.print_users(db)

    DB_Tools.check_surveys(db)


if __name__ == '__main__':
    main()

