import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pprint import pprint
import datetime as dt
from scipy.signal import welch
from scipy.integrate import simps
from math import sqrt
from timeit import default_timer as timer

# Takes one sensor data file and converts to a pandas dataframe
def get_file_as_df(db, sensingPeriod, sensor):
    file_id = sensingPeriod + "-" + sensor
    record = db.data.find_one({"_id":file_id})
    file_path = record["filePath"]
    try:
        df = pd.read_csv(file_path, index_col=0, header=None)

        if sensor == 'Accelerometer':
            df.columns = ["Accel_x (ms-2)", "Accel_y (ms-2)", "Accel_z (ms-2)"]
            calc_accelerometer_magnitude(df)

        elif sensor == 'Audio':
            df.columns = ["Audio"]

        elif sensor == 'Battery':
            df.columns = ["Battery_charge (%/100)", "Battery_temperature (10^-1 deg C)", "Battery_voltage (mV)",
                          "Battery_plugged", "Battery_status", "Battery_health"]

        elif sensor == 'Gyroscope':
            df.columns = ["Gyro_x (rad/s)", "Gyro_y (rad/s)", "Gyro_z (rad/s"]

        elif sensor == 'Location':
            df.columns = ["Location_lat", "Location_long", "Location_altitude", "Location_accuracy"]

        elif sensor == 'Magnetometer':
            df.columns = ["Magnet_x", "Magnet_y", "Magnet_z"]

        elif sensor == 'MotionActivity':
            df.columns = ["Motion_activity", "Motion_string", "Motion_confidence"]

        elif sensor == 'ScreenStatus':
            df.columns = ["Screen_status", "Screen_string"]

        df.index = pd.to_datetime(df.index, unit='ms')

        # print(df.describe())
        return df

    except Exception as ex:
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        print(message)
        print(file_path)
        print (sensor + " empty")

        return None



# For a given sensing period, convert all files to dataframes and merge the dataframes
def get_all_data_for_period(db, sensingPeriod):
    main_df = get_file_as_df(db, sensingPeriod, "Accelerometer")

    for sensorRecord in db.data.find({"period": sensingPeriod}):
        sensor = sensorRecord["sensorType"]
        if sensor != "Accelerometer" and sensor != "SurveyResult":
            sub_df = get_file_as_df(db, sensingPeriod, sensor)
            if sub_df is not None:
                main_df = pd.merge(main_df, sub_df, how='outer', left_index=True, right_index=True)

    label_walking(main_df)

    return main_df

def get_accel_and_motion(db, sensingPeriod):
    main_df = get_file_as_df(db, sensingPeriod, "Accelerometer")
    motion_df = get_file_as_df(db, sensingPeriod, "MotionActivity")
    main_df = pd.merge(main_df, motion_df, how='outer', left_index=True, right_index=True)
    label_walking(main_df)

    return main_df


# For a given sensing period, get accelerometer data only
def get_accelerometer(db, sensingPeriod):
    df = get_file_as_df(db, sensingPeriod, "Accelerometer")
    calc_accelerometer_magnitude(df)
    print (df)
    return df

# Add a gravity normalised magnitude column to the accelerometer dataframe
def calc_accelerometer_magnitude(df):
    df["sq_rt_sum_sq"] = (df["Accel_x (ms-2)"] **2 + df["Accel_y (ms-2)"] ** 2 + df["Accel_z (ms-2)"] ** 2)**(1/2)
    window = 50
    df["rolling_avg"] = (pd.rolling_sum(df["sq_rt_sum_sq"], window)) / window
    df["Accel_mag"] = df["sq_rt_sum_sq"] - df["rolling_avg"]
    df["Accel_mag_avg"] = (pd.rolling_sum(df["Accel_mag"], window)) / window

# For a given sensing period, get motion activity data only
def get_motion_activity(db, sensingPeriod):
    df = get_file_as_df(db, sensingPeriod, "MotionActivity")
    label_walking(df)
    print(df)
    return df

# Add a column to the dataframe which indicates whether user was walking at that time
# (based on last motion activity record)
def label_walking(df):
    df['Motion_activity'].fillna((-1), inplace=True)
    df.loc[df['Motion_activity'] == 7, 'Motion_walking'] = True
    df.loc[(df['Motion_activity'] != 7) & (df['Motion_activity'] >= 0), 'Motion_walking'] = False
    df['Motion_walking'].fillna(method='ffill', inplace=True)


def get_data_split_by_walking(db, sensingPeriod):
    #df = get_all_data_for_period(db, sensingPeriod)
    df = get_accel_and_motion(db, sensingPeriod)
    #plot_general(df, "Accel_mag")
    dfs = [g for i,g in df.groupby(df['Motion_walking'].ne(df['Motion_walking'].shift()).cumsum())]
    dfs_walking = []
    dfs_non_walking = []
    for d in dfs:
        start = d.head(1).index.get_values()[0]
        end = d.tail(1).index.get_values()[0]
        duration = (np.timedelta64(end - start, 's')).astype(int)
        if (d.iloc[0]['Motion_walking'] == True) and duration > 30:
            dfs_walking.append(d)
        else:
            dfs_non_walking.append(d)

    return dfs_walking, dfs_non_walking


def get_walking_statistics(dfs, sd, prt):

    walking_data = []
    for df in dfs:
        freq_stats = get_walking_frequency_stats(df, prt)
        df = label_steps(df, sd)
        df = label_anti_steps(df)

        if 'anti_step' in df:
            df_anti = df[df["anti_step"] == True]
            average_gait_stretch = df_anti["gait_stretch"].mean()
            gs_std_dev = df_anti["gait_stretch"].std()
            gs_skew = df_anti["gait_stretch"].skew()
            gs_kurtosis = df_anti["gait_stretch"].kurtosis()
        else:
            average_gait_stretch = 0
            gs_std_dev = 0
            gs_skew = 0
            gs_kurtosis = 0

        df_steps = df[df["step"] == True]
        if len(df_steps.index) > 0:
            start = df_steps.head(1).index.get_values()[0]
            end = df_steps.tail(1).index.get_values()[0]
            duration = (np.timedelta64(end - start, 's')).astype(int)

            step_count = df_steps["step"].value_counts()[True]

            if (step_count > 15) and (duration > 30):
                #plot_labelled_steps(df)
                cadence = step_count / duration

                df_steps["step_time"] = df_steps.index.to_series().diff().astype('timedelta64[ms]')
                df_steps["step_time"] = np.where((df_steps["step_time"] < 2000), df_steps["step_time"], -1)
                df_steps["step_time"].fillna((-1), inplace=True)

                average_step_time = df_steps[df_steps["step_time"] > 0]["step_time"].mean() / 1000
                step_time_std_dev = df_steps[df_steps["step_time"] > 0]["step_time"].std() /1000
                step_time_skew = df_steps[df_steps["step_time"] > 0]["step_time"].skew()
                step_time_kurtosis = df_steps[df_steps["step_time"] > 0]["step_time"].kurtosis()

                std_dev = df["Accel_mag"].std()
                skewness = df["Accel_mag"].skew()
                kurtosis = df["Accel_mag"].kurtosis()

                results = {"start_time": start,
                                     "duration": duration,
                                     "step_count": step_count,
                                     "cadence": cadence,
                                     "step_time": average_step_time,
                                     "step_time_std_dev": step_time_std_dev,
                                     "step_time_skew": step_time_skew,
                                     "step_time_kurtosis": step_time_kurtosis,
                                     "gait_stretch": average_gait_stretch,
                                     "std_dev": std_dev,
                                     "skewness": skewness,
                                     "kurtosis": kurtosis,
                                     "gs_skew": gs_skew,
                                     "gs_kurtosis": gs_kurtosis,
                                     "gs_std_dev": gs_std_dev}
                #print(results)
                for key in freq_stats.keys():
                    results[key] = freq_stats[key]

                walking_data.append(results)

    return walking_data


# Metod to extract frequency domain features from walking data
def get_walking_frequency_stats(df, prt):
    df = df["Accel_mag"]
    #clean up and resample df to give evenly spaced samples (required for DFT)
    df = df[~df.index.duplicated(keep='first')]
    df = df.resample('ms').interpolate()
    df = df.resample('25ms').interpolate()
    df = df.dropna()
    array = df.as_matrix().ravel()

    #sample rate is 40 Hz (once per 25ms)
    fs = 1000/25
    #Welch's method for Power Spectral Density
    f, pxx = welch(array, fs=fs, return_onesided=True)

    #plot_PSD(f, pxx)

    #Simpson's integration from samples
    total_power = simps(pxx, f)

    df = pd.DataFrame(f, columns=["frequency"])
    df["power"] = pd.Series(pxx)
    #Consider 5 Hz to be low/high boundary
    low_high_boundary = prt
    low_df = df[df["frequency"] <= low_high_boundary]
    high_df = df[df["frequency"] > low_high_boundary]
    low_freq_power = simps(low_df["power"].as_matrix(), low_df["frequency"].as_matrix())
    high_freq_power = simps(high_df["power"].as_matrix(), high_df["frequency"].as_matrix())
    power_ratio = high_freq_power / low_freq_power

    fundamental_freq = df.loc[df["power"].argmax()]["frequency"]
    harmonics = [fundamental_freq * i for i in range(1,7)]
    df["S_or_N"] = df["frequency"].apply(lambda x: x in harmonics)
    signal_power = df[df["S_or_N"] == True]["power"].sum()
    noise_power = df[df["S_or_N"] == False]["power"].sum()
    SNR = signal_power / noise_power

    harmonic_powers = df[df["S_or_N"] == True]["power"].as_matrix()
    sum_sq_harmonic_power = 0
    for i in range(1,6):
        sum_sq_harmonic_power += (harmonic_powers[i]**2)
    THD = (sqrt(sum_sq_harmonic_power)) / harmonic_powers[0]


    my_total_power = df["power"].sum()

    stats = {"total_power": total_power,
             "power_ratio": power_ratio,
             "SNR": SNR,
             "THD": THD}

    return stats


# Filter out non-accelerometer data and label the steps
def label_steps(df, sd):
    #ts = timer()
    magnitude_threshold = 2
    std_dev_threshold = sd
    df = df.dropna(subset=["Accel_mag"])
    df = df[~df.index.duplicated(keep='first')]

    df_neg = df[df["Accel_mag"] < 0]
    df_neg["Accel_mag_neg_avg"] = (pd.rolling_sum(df_neg["Accel_mag"], 20)) / 20
    df_neg["rolling_std_dev"] = pd.rolling_std(df_neg["Accel_mag"], window=20, min_periods=20)
    df_neg["step_threshold"] = (df_neg["Accel_mag_neg_avg"] - (std_dev_threshold * df_neg["rolling_std_dev"]))
    df_neg["step"] = (df_neg["Accel_mag"] < df_neg["step_threshold"]) & (df_neg["Accel_mag"] < -magnitude_threshold)

    #ts = timer()
    df_neg = df_neg.apply(lambda row: filter_steps_forward(row, df_neg), axis=1)
    #te = timer()
    #print("filter steps forward took " + str(te - ts))

    #ts = timer()
    df_neg = df_neg.apply(lambda row: filter_steps_backward(row, df_neg), axis=1)
    #te = timer()
    #print("filter steps backward took " + str(te - ts))

    df.loc[df_neg.index, "step"] = df_neg.loc[df_neg.index, "step"]
    #df.loc[df_neg.index, "rolling_std_dev"] = df_neg.loc[df_neg.index, "rolling_std_dev"]
    #df.loc[df_neg.index, "step_threshold"] = df_neg.loc[df_neg.index, "step_threshold"]
    df["step"].fillna("False", inplace=True)
    #df["rolling_std_dev"].fillna(method='pad', inplace=True)
    #df["step_threshold"].fillna(method='pad', inplace=True)
    #te = timer()
    #print("label steps took " + str(te-ts))
    return df

# Used by function above to filter out accelerometer peaks which have been labelled as steps but are
# actually just artifacts (too close to a real step)
def filter_steps_forward(row, df):

    time = row.name + dt.timedelta(milliseconds=0.01)
    time_plus_250 = time + dt.timedelta(milliseconds=300)
    if row["step"] == True:
        sub_df = df[time: time_plus_250]
        for index, sub_row in sub_df.iterrows():
            if sub_row["step"] == True:
                if row["Accel_mag"] > sub_row["Accel_mag"]:
                    row["step"] = False
                    break

    return row

def filter_steps_backward(row, df):

    time = row.name + dt.timedelta(milliseconds=0.01)
    time_minus_250 = time - dt.timedelta(milliseconds=300)
    if row["step"] == True:
        sub_df = df[time_minus_250 : time]
        for index, sub_row in sub_df.iterrows():
            if sub_row["step"] == True:
                if row["Accel_mag"] > sub_row["Accel_mag"]:
                    row["step"] = False
                    break
    return row


def label_anti_steps(df):
    #ts = timer()
    for index, row in df.iterrows():
        if row["step"] == True:
            time = row.name + dt.timedelta(milliseconds=0.001)
            cut_off = time + dt.timedelta(milliseconds=750)
            forward_df = df[time:]
            for index2, row2 in forward_df.iterrows():
                if row2["step"] == True:
                    if index2 < cut_off:
                        sub_df = forward_df[time:index2]
                    else:
                        sub_df = forward_df[time:cut_off]
                    if len(sub_df.index) > 0:
                        max_index = sub_df["Accel_mag"].argmax()
                    else:
                        max_index = forward_df[time:index2]["Accel_mag"].argmax()
                    df.loc[max_index, "anti_step"] = True
                    df.loc[max_index, "gait_stretch"] = df.loc[max_index, "Accel_mag"] - row["Accel_mag"]
                    break

    if 'anti_step' in df:
        df["anti_step"].fillna(False, inplace=True)
    if 'gait_stretch' in df:
        df["gait_stretch"].fillna((-1), inplace=True)
    #te = timer()
    #print("label anti-steps took " + str(te - ts))
    return df


#Plot accelerometer data with steps labelled with an 's'
def plot_labelled_steps(df):
    times = df.index.values
    vals = df["Accel_mag"].values
    step_labels = df["step"].values

    std_dev = df["step_threshold"].values

    anti_step_labels = df["anti_step"].values

    fig, ax = plt.subplots()
    ax.plot(times,vals)
    ax.plot(times, std_dev)
    plt.title("Walking instance with steps/rebounds labelled")
    plt.xlabel("Time")
    plt.ylabel("Acceleration ms^-2")

    for i, label in enumerate(step_labels):
        if label == True:
            ax.annotate("s", (times[i], vals[i]))

    for i, label in enumerate(anti_step_labels):
        if label == True:
            ax.annotate("x", (times[i], vals[i]))

    plt.show()

#Plot any general numerical data against the time index
def plot_general(df, column):
    times = df.index.values
    vals = df[column].values

    plt.plot(times, vals)
    plt.title("Raw accelerometer data")
    plt.xlabel("Time")
    plt.ylabel("Acceleration ms^-2")
    plt.show()

def plot_PSD(f, pxx):
    plt.plot(f, pxx)
    plt.show()


