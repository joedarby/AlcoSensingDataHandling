import numpy as np
import pandas as pd
from pymongo import MongoClient
from multiprocessing import Pool

import RandomForest
import Gait_Analysis


def main():
    pool = Pool()
    mean_accuracies = pool.map(run_model, range(10))
    pool.close()
    pool.join()

    overall_accuracy = np.array(mean_accuracies).mean()
    print("overall mean accuracy = " + str(overall_accuracy))


def run_model(i):
    dbClient = MongoClient()
    db = dbClient.alcosensing

    #selected_features = ["cadence", "step_time", "gait_stretch", "skewness", "kurtosis", "total_power", "power_ratio",
                        # "SNR"]
    selected_features = ["cadence", "kurtosis", "power_ratio"]

    training_data, validation_data = Gait_Analysis.sample_data(db)
    training_features, training_targets = Gait_Analysis.generate_model_inputs(training_data, selected_features)
    model = RandomForest.fit_forest(training_features, training_targets, selected_features)
    mean_accuracy = RandomForest.validate_model(model, validation_data, selected_features)
    return mean_accuracy


def initialise():

    np.set_printoptions(linewidth=640)
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 1000)


if __name__ == '__main__':
    #db = initialise()
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








