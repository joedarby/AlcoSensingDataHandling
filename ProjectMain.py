import numpy as np
import pandas as pd
from pymongo import MongoClient
from multiprocessing import Pool
from multiprocessing import cpu_count

import RandomForest
import Feature_Generation
import Sampling


def main():
    print(cpu_count())
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





def run_model(i):
    dbClient = MongoClient()
    db = dbClient.alcosensing


    selected_features = [ #"duration",
                            "cadence",
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
                           "day_of_week",
                           "time",
                            "bar_nearby",
                            #"night_club_nearby",
                         #"restaurant_nearby"
                         "audio_mean",
                        "audio_std_dev",
                        #"audio_skewness",
                        #-"audio_kurtosis",
                        "audio_max",
                        #"audio_min",
                        #"audio_range",
                        "audio_median",
                          #"audio_total_power",
                         "audio_power_ratio",
                         # "audio_SNR",
                         #"audio_THD",
                         # "screen_on_proportion",
                         # "screen_switches_on",
                         # "screen_switches_off",
                         # "screen_unlocks",
                         # "screen_mean_on_duration",
                         # "screen_mean_off_duration",
                         #"screen_unlocks_over_time",
                         # "screen_switches_on_over_time",
                         # "screen_switches_off_over_time",
                         #"screen_changes_over_time",
                        #"battery_start_pct"

                         ]

    all_data = Sampling.get_from_db(db)

    training_df, validation_df = Sampling.partition_and_label_data(all_data)

    training_features, training_targets = Sampling.get_training_inputs(training_df, selected_features)
    model = RandomForest.fit_forest(training_features, training_targets, selected_features)

    mean_accuracy = RandomForest.validate_model_new(model, validation_df, selected_features)
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








