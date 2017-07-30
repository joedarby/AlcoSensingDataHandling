import numpy as np
import pandas as pd
from pymongo import MongoClient

import RandomForest
import Gait_Analysis


def main():
    training_data, validation_data = Gait_Analysis.sample_data(db)
    training_features, training_targets = Gait_Analysis.generate_model_inputs(training_data)
    model = RandomForest.fit_forest(training_features, training_targets)
    RandomForest.validate_model(model, validation_data)


def initialise():

    np.set_printoptions(linewidth=640)
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 1000)

    dbClient = MongoClient()
    db = dbClient.alcosensing
    return db


if __name__ == '__main__':
    db = initialise()
    main()



#df = Data_Tools.get_all_data_for_period(db, id)

#Data_Tools.plot_labelled_steps(df)

#df_walking = Data_Tools.get_step_labelled_walking_data(db, id)
#print(df_walking)

#df_motion = Data_Tools.get_motion_activity(db, period)
#df_accel = Data_Tools.get_accelerometer(db, period)

#Data_Tools.plot_file_data(df_motion, "walking", 1)
#Data_Tools.plot_general(df, "Accel_mag")
#Data_Tools.plot_labelled_steps(df_walking)








