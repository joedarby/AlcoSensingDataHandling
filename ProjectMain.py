import numpy as np
import pandas as pd
from pymongo import MongoClient
from multiprocessing import Pool
from multiprocessing import cpu_count
from pprint import pprint
from pandas.tools.plotting import scatter_matrix
import matplotlib.pyplot as plt

import RandomForest
import RegressionModels
import Feature_Generation
import Sampling
import BAC_Calculation

all_data_df = None
db = None

def main():
    print(cpu_count())
    for prt in range(6, 7):
        #Gait_Analysis.generate_features(1.3, prt)


        #Non-parallel
        '''
        mean_accuracies = []
        for i in range(50):
            res = run_model(i)
            mean_accuracies.append(res)
        '''

        #Paralell

        global db
        dbClient = MongoClient()
        db = dbClient.alcosensing

        global all_data_df
        all_data = Sampling.get_from_db(db)
        all_data_df = Sampling.label_data(all_data)

        drunk_df = all_data_df[all_data_df["Units"] > 0]
        #print(drunk_df)
        print(drunk_df["eBAC (timeAdj)"].describe(percentiles=[0.1,0.25,0.33,0.455,0.456,0.457,0.458,0.459, 0.46,0.5,0.66,0.75,0.9]))


        print(all_data_df["drunk"].value_counts())

        #accuracies_tuples = []
        #for i in range(300):
        #    res = run_random_forest_model(i)
        #    accuracies_tuples.append(res)

        pool = Pool()
        results = pool.map(run_random_forest_model, range(500))
        pool.close()
        pool.join()

        #accuracies_tuples = [x for (x,y) in results]
        accuracies_tuples = results
        #importances = [y for (x, y) in results]

        mean_accuracies = [x for (x,y,z) in accuracies_tuples]
        mean_drunk_accuracies = [y for (x, y, z) in accuracies_tuples]
        confusion_matrices = [z for (x,y,z) in accuracies_tuples]


        overall_accuracies = np.array(mean_accuracies)
        mean_overall_accuracy = overall_accuracies.mean()
        overall_drunk_accuracies = np.array(mean_drunk_accuracies)
        mean_overall_drunk_accuracy = overall_drunk_accuracies.mean()
        min_accuracy = overall_accuracies.min()
        max_accuracy = overall_accuracies.max()
        std_dev_of_accuracy = overall_accuracies.std()
        print("overall mean accuracy = " + str(mean_overall_accuracy))
        print("overall mean drunk accuracy = " + str(mean_overall_drunk_accuracy))
        print("min accuracy = " + str(min_accuracy))
        print("max accuracy = " + str(max_accuracy))
        print("std dev = " + str(std_dev_of_accuracy))

        average_conf_mat = [[0,0,0],[0,0,0],[0,0,0]]
        #average_conf_mat = [[0, 0], [0, 0]]
        for m in confusion_matrices:
            for row_index, row in enumerate(average_conf_mat):
                for col_index, value in enumerate(row):
                    average_conf_mat[row_index][col_index] += m[row_index][col_index]
        average_conf_mat = np.array(average_conf_mat)
        average_conf_mat = np.multiply((1/500), average_conf_mat)

        print(average_conf_mat)
        print("\n")

        #df_importances = pd.DataFrame(importances)
        #df_means = df_importances.mean()


        #print(df_means)
        #print(df_means.idxmin())




def find_avg_importances():
    global db
    dbClient = MongoClient()
    db = dbClient.alcosensing

    global all_data_df
    all_data = Sampling.get_from_db(db)
    all_data_df = Sampling.label_data(all_data)

    pool = Pool()
    importances = pool.map(run_random_forest_model, range(100))
    pool.close()
    pool.join()

    df = pd.DataFrame(importances)

    print(df)
    print(df.mean())


def find_correlations():
    dbClient = MongoClient()
    db = dbClient.alcosensing

    all_data = Sampling.get_from_db(db)
    df = Sampling.label_data(all_data)

    df = df[df["Units"] >0]

    #print(df[["drinkUnits", "timeAdjUnits", "eBAC", "weighted_eBAC", "drinkRating"]].describe())

    corr_df = df.corr(method='pearson')

    #corr_df = corr_df[["Units", "Feeling", "CIS", "eBAC", "EBIS", "Units (timeAdj)", "Feeling (timeAdj)", "CIS (timeAdj)", "eBAC (timeAdj)", "EBIS (timeAdj)"]]
    corr_df = corr_df[["Units", "Feeling", "CIS", "eBAC", "EBIS"]]
    corr_df = corr_df.drop(["Units", "Feeling", "CIS", "eBAC", "EBIS", "Units (timeAdj)", "Feeling (timeAdj)", "CIS (timeAdj)", "eBAC (timeAdj)", "EBIS (timeAdj)", "drunk", "weighted_eBAC", "didDrink"])

    corr_df.sort_index()

    #print(corr_df)

    #selected_df = df[["cadence", "step_time", "gait_stretch", "signal_mean", "steps_mean", "anti_mag_mean", "SNR", "THD", "total_power", "power_ratio"]]
    selected_df = df[["audio_max", "audio_min", "audio_mean", "audio_median", "audio_range", "audio_std_dev", "audio_total_power", "audio_SNR", "audio_THD", "audio_power_ratio", "audio_skewness", "audio_kurtosis"]]
    print(selected_df.corr(method='pearson'))
    Axes = scatter_matrix(selected_df)
    [plt.setp(item.yaxis.get_label(), 'size', 10) for item in Axes.ravel()]
    [plt.setp(item.xaxis.get_label(), 'size', 10) for item in Axes.ravel()]
    [plt.setp(item.xaxis.get_label(), 'rotation', 45) for item in Axes.ravel()]
    [plt.setp(item.yaxis.get_label(), 'rotation', 45) for item in Axes.ravel()]
    [plt.setp(item.yaxis.get_label(), 'ha', 'right') for item in Axes.ravel()]
    [plt.setp(item.xaxis.get_label(), 'ha', 'right') for item in Axes.ravel()]
    [item.xaxis.set_label(item.xaxis.get_label().set_text(item.xaxis.get_label().get_text()[6:])) for item in Axes.ravel()]
    [item.yaxis.set_label(item.yaxis.get_label().set_text(item.yaxis.get_label().get_text()[6:])) for item in
     Axes.ravel()]
    [plt.setp(item.xaxis.get_majorticklabels(), 'size', 5) for item in Axes.ravel()]
    [plt.setp(item.yaxis.get_majorticklabels(), 'size', 5) for item in Axes.ravel()]

    plt.show()
    '''
    selected_df = df[["cadence", "step_time", "gait_stretch", "signal_mean", "total_power", "power_ratio", "SNR", "THD"]]
    print(selected_df.corr(method='pearson'))
    scatter_matrix(selected_df)
    plt.show()

    selected_df = df[
        ["step_time", "step_time_std_dev", "step_time_skew","signal_kurtosis", "gait_stretch", "gs_std_dev", "gs_skew", "gs_kurtosis",]]
    print(selected_df.corr(method='pearson'))
    scatter_matrix(selected_df)
    plt.show()
    '''

    #selected_df = df[["anti_mag_kurtosis", "audio_mean", "audio_min", "audio_std_dev", "bar_nearby", "duration",
                    #  "night_club_nearby", "restaurant_nearby", "time" ]]
    #print(selected_df.corr(method='pearson'))
    #scatter_matrix(selected_df)
    #plt.show()

def plot_individual_features():

    all_data = Sampling.get_from_db(db)
    df = Sampling.label_data(all_data)

    features = ["duration",
                        "cadence",
                       "step_time",
                       "step_time_std_dev",
                       "step_time_skew",
                       "step_time_kurtosis",
                       "gait_stretch",
                       "signal_mean",
                       "signal_std_dev",
                       "signal_skewness",
                       "signal_kurtosis",
                       "gs_skew",
                       "gs_kurtosis",
                       "gs_std_dev",
                       "steps_mean",
                       "steps_std_dev",
                       "steps_skewness",
                       "steps_kurtosis",
                       "anti_mag_mean",
                       "anti_mag_std_dev",
                       "anti_mag_skewness",
                       "anti_mag_kurtosis",
                       "total_power",
                       "power_ratio",
                       "SNR",
                       "THD",
                       "day_of_week",
                       "time",
                        "bar_nearby",
                        "night_club_nearby",
                    "restaurant_nearby",
                     "audio_mean",
                    "audio_std_dev",
                    "audio_skewness",
                    "audio_kurtosis",
                    "audio_max",
                    "audio_min",
                    "audio_range",
                    "audio_median",
                      "audio_total_power",
                     "audio_power_ratio",
                      "audio_SNR",
                     "audio_THD"]

    for feature in features:
        fig, ax = plt.subplots()
        #ax.scatter(df[feature], df["drinkUnits"])
        #ax.scatter(df[feature], df["drinkFeeling"])
        ax.scatter(df[feature], df["drinkRating"])
        plt.xlabel(feature)
        plt.ylabel("Rating")
        plt.show()


def run_random_forest_model(i):

    gait_features = ["cadence",
                     "step_time",
                            "step_time_std_dev",
                           "step_time_skew",
                           "step_time_kurtosis",
                           "gait_stretch",
                           "signal_mean",
                           #"signal_std_dev",
                           "signal_skewness",
                           "signal_kurtosis",
                           "gs_skew",
                           "gs_kurtosis",
                           #"gs_std_dev",
                           "steps_mean",
                           "steps_std_dev",
                           "steps_skewness",
                          #"steps_kurtosis",
                           "anti_mag_mean",
                           #"anti_mag_std_dev",
                           "anti_mag_skewness",
                           #"anti_mag_kurtosis",
                           "total_power",
                           "power_ratio",
                           "SNR",
                           "THD"]

    audio_features = ["audio_mean",
                        "audio_std_dev",
                        #"audio_skewness",
                        "audio_kurtosis",
                        "audio_max",
                        "audio_min",
                        "audio_range",
                        "audio_median",
                        "audio_total_power",
                        "audio_power_ratio",
                        "audio_SNR",
                        "audio_THD"

    ]

    screen_features = ["screen_on_proportion",
                          #"screen_switches_on",
                          #"screen_switches_off",
                         #"screen_unlocks",
                         # "screen_mean_on_duration",
                         # "screen_mean_off_duration",
                         "screen_unlocks_over_time",
                          "screen_switches_on_over_time",
                          "screen_switches_off_over_time",
                         "screen_changes_over_time"]

    gyroscope_features = ["gyro_mean",
                        "gyro_std_dev",
                        "gyro_skewness",
                        "gyro_kurtosis",
                        "gyro_max",
                        #"gyro_min",
                        "gyro_range",
                        "gyro_median",
                        "gyro_total_power",
                        "gyro_power_ratio",
                        "gyro_SNR",
                        "gyro_THD"]

    selected_features = [  # "duration",
        # "day_of_week",
        #"time",
        #"bar_nearby",
        # "night_club_nearby",
        # "restaurant_nearby"
        #"battery_start_pct"
    ] + gyroscope_features + audio_features + gait_features




    while True:
        training_df, validation_df = Sampling.partition_data(all_data_df)
        if Sampling.validate_partition(validation_df, 15):
            break
        print("bad partition")

    training_features, training_targets = Sampling.get_training_inputs(training_df, selected_features)
    model = RandomForest.fit_forest(training_features, training_targets)
    #importances = RandomForest.get_importances(model, selected_features)


    accuracies_tuple = RandomForest.validate_model_new(model, validation_df, selected_features)
    #return (accuracies_tuple, importances)
    return (accuracies_tuple)



def initialise():

    np.set_printoptions(linewidth=640)
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 1000)


if __name__ == '__main__':
    initialise()
    main()
    #find_avg_importances()
    #find_correlations()
    #run_logistic_model()
    #plot_individual_features()
    #BAC_Calculation.update_eBAC()








