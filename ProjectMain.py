import numpy as np
import pandas as pd
from pymongo import MongoClient
from multiprocessing import Pool
from multiprocessing import cpu_count

import RandomForest
import Gait_Analysis


def main():
    print(cpu_count())
    results = []
    for prt in range(6, 7):
        #Gait_Analysis.generate_features(1.3, prt)
        pool = Pool()
        mean_accuracies = pool.map(run_model, range(300))
        pool.close()
        pool.join()


        overall_accuracies = np.array(mean_accuracies)
        mean_overall_accuracy = overall_accuracies.mean()
        min_accuracy = overall_accuracies.min()
        max_accuracy = overall_accuracies.max()
        std_dev_of_accuracy = overall_accuracies.std()
        print("overall mean accuracy = " + str(mean_overall_accuracy))
        print("min accuracy = " + str(min_accuracy))
        print("max accuracy = " + str(max_accuracy))
        print("std dev = " + str(std_dev_of_accuracy))
        #results.append((prt, overall_accuracy))

    print(results)




def run_model(i):
    dbClient = MongoClient()
    db = dbClient.alcosensing

    selected_features = ["cadence",
                           "step_time",
                           "step_time_std_dev",
                           "step_time_skew",
                           "step_time_kurtosis",
                           "gait_stretch",
                           #-"signal_mean",
                           "signal_std_dev",
                           "signal_skewness",
                           "signal_kurtosis",
                           "gs_skew",
                           #-"gs_kurtosis",
                           "gs_std_dev",
                           "steps_mean",
                           "steps_std_dev",
                           #0"steps_skewness",
                           #0"steps_kurtosis",
                           "anti_mag_mean",
                           "anti_mag_std_dev",
                           #0"anti_mag_skewness",
                           #0"anti_mag_kurtosis",
                           "total_power",
                           "power_ratio",
                           "SNR",
                           "THD",
                           #"day_of_week",
                           #"time"
                         ]


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








