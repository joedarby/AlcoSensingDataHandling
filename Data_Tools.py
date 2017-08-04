import datetime as dt
from math import sqrt

import numpy as np
import pandas as pd
from scipy.integrate import simps
from scipy.signal import welch

import Charts
import time

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
    main_df = label_walking(main_df)
    #plot_3_axis(main_df)
    return main_df


def get_with_location(db, sensingPeriod):
    main_df = get_accel_and_motion(db, sensingPeriod)
    location_df = get_file_as_df(db, sensingPeriod, "Location")
    main_df = pd.merge(main_df, location_df, how='outer', left_index=True, right_index=True)
    main_df["Location_lat"].fillna(method='ffill', inplace=True)
    main_df["Location_long"].fillna(method='ffill', inplace=True)
    main_df["Location_lat"].fillna(method='bfill', inplace=True)
    main_df["Location_long"].fillna(method='bfill', inplace=True)
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
    return df


def split_out_walking_periods(raw_df):
    split_dfs = [g for i,g in raw_df.groupby(raw_df['Motion_walking'].ne(raw_df['Motion_walking'].shift()).cumsum())]

    walking_dfs = []
    #dfs_non_walking = []

    for df in split_dfs:
        if (df.iloc[0]['Motion_walking'] == False) and (get_df_duration(df) < 2):
            df['Motion_walking'] = True

    output_dfs = []
    for df in split_dfs:
        if len(output_dfs) == 0:
            output_dfs.append(df)
        else:
            last = output_dfs.pop(-1)
            if (last.iloc[0]['Motion_walking']) == (df.iloc[0]['Motion_walking']):
                merged = pd.concat([last, df])
                output_dfs.append(merged)
            else:
                output_dfs.append(last)
                output_dfs.append(df)

    for d in output_dfs:
        duration = get_df_duration(d)
        if (d.iloc[0]['Motion_walking'] == True) and duration > 30:
            walking_dfs.append(d)
        #else:
        #    dfs_non_walking.append(d)

    #Charts.plot_with_vertical_lines(main_df, dfs_walking)

    return walking_dfs


def filter_walking_periods(dfs, sd):
    valid_dfs = []
    for df in dfs:
        df = label_steps_old(df, sd)
        df_steps = df[df["step"] == True]
        step_count = len(df_steps.index)
        if step_count > 15:
            duration = get_df_duration(df_steps)
            df_steps["step_time"] = df_steps.index.to_series().diff().astype('timedelta64[ms]')
            df_steps["step_time"] = np.where((df_steps["step_time"] < 2000), df_steps["step_time"], -1)
            df_steps["step_time"].fillna((-1), inplace=True)
            valid_step_time_count = len(df_steps[df_steps["step_time"] > 0].index)
            if (duration > 30) and (valid_step_time_count > 8):
                valid_dfs.append(df)

    return valid_dfs


def get_walking_statistics(df, prt):
    freq_stats = get_walking_frequency_stats(df, prt)

    df = label_anti_steps(df)

    if 'anti_step' in df:
        df_anti = df[df["anti_step"] == True]
        anti_mag_mean = df_anti["Accel_mag"].mean()
        anti_mag_std_dev = df_anti["Accel_mag"].std()
        anti_mag_skewness = df_anti["Accel_mag"].skew()
        anti_mag_kurtosis = df_anti["Accel_mag"].kurtosis()

        average_gait_stretch = df_anti["gait_stretch"].mean()
        gs_std_dev = df_anti["gait_stretch"].std()
        gs_skew = df_anti["gait_stretch"].skew()
        gs_kurtosis = df_anti["gait_stretch"].kurtosis()
    else:
        average_gait_stretch = 0
        gs_std_dev = 0
        gs_skew = 0
        gs_kurtosis = 0
        anti_mag_mean = 0
        anti_mag_std_dev = 0
        anti_mag_skewness = 0
        anti_mag_kurtosis = 0

    df_steps = df[df["step"] == True]
    duration = get_df_duration(df_steps)
    step_count = len(df_steps.index)

    cadence = step_count / duration

    df_steps["step_time"] = df_steps.index.to_series().diff().astype('timedelta64[ms]')
    df_steps["step_time"] = np.where((df_steps["step_time"] < 2000), df_steps["step_time"], -1)
    df_steps["step_time"].fillna((-1), inplace=True)

    average_step_time = df_steps[df_steps["step_time"] > 0]["step_time"].mean() / 1000
    step_time_std_dev = df_steps[df_steps["step_time"] > 0]["step_time"].std() / 1000
    step_time_skew = df_steps[df_steps["step_time"] > 0]["step_time"].skew()
    step_time_kurtosis = df_steps[df_steps["step_time"] > 0]["step_time"].kurtosis()

    signal_mean = df["Accel_mag"].mean()
    signal_std_dev = df["Accel_mag"].std()
    signal_skewness = df["Accel_mag"].skew()
    signal_kurtosis = df["Accel_mag"].kurtosis()

    steps_mean = df_steps["Accel_mag"].mean()
    steps_std_dev = df_steps["Accel_mag"].std()
    steps_skewness = df_steps["Accel_mag"].skew()
    steps_kurtosis = df_steps["Accel_mag"].kurtosis()

    results = {"duration": duration,
               "step_count": step_count,
               "cadence": cadence,
               "step_time": average_step_time,
               "step_time_std_dev": step_time_std_dev,
               "step_time_skew": step_time_skew,
               "step_time_kurtosis": step_time_kurtosis,
               "gait_stretch": average_gait_stretch,
               "signal_mean": signal_mean,
               "signal_std_dev": signal_std_dev,
               "signal_skewness": signal_skewness,
               "signal_kurtosis": signal_kurtosis,
               "gs_skew": gs_skew,
               "gs_kurtosis": gs_kurtosis,
               "gs_std_dev": gs_std_dev,
               "steps_mean": steps_mean,
               "steps_std_dev": steps_std_dev,
               "steps_skewness": steps_skewness,
               "steps_kurtosis": steps_kurtosis,
               "anti_mag_mean": anti_mag_mean,
               "anti_mag_std_dev": anti_mag_std_dev,
               "anti_mag_skewness": anti_mag_skewness,
               "anti_mag_kurtosis": anti_mag_kurtosis}

    for key in freq_stats.keys():
        results[key] = freq_stats[key]

    general_stats = get_df_general_features(df)
    for key in general_stats:
        results[key] = general_stats[key]

    flag = True
    for key in results.keys():
        if pd.isnull(results.get(key)):
            print("null generated: " + key)
            flag = False

    if flag:
        print("walking data generated")
        return results
    else:
        return None

def get_location_statistics(df):
    #df = df.apply(lambda row: test(row))
    #print(df)
    return "blah"


def test(row):
    row["result"] = row["Location_lat"] * row["Location_long"]
    time.sleep(3)
    return row




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

    #Charts.plot_PSD(f, pxx)

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

    stats = {"total_power": total_power,
             "power_ratio": power_ratio,
             "SNR": SNR,
             "THD": THD}

    return stats


# Filter out non-accelerometer data and label the steps
def label_steps(df, sd):
    magnitude_threshold = 2
    std_dev_threshold = sd
    df = df.dropna(subset=["Accel_mag"])
    df = df[~df.index.duplicated(keep='first')]

    df_neg = df[df["Accel_mag"] < 0]
    df_neg["Accel_mag_neg_avg"] = (pd.rolling_sum(df_neg["Accel_mag"], 20)) / 20
    df_neg["rolling_std_dev"] = pd.rolling_std(df_neg["Accel_mag"], window=20, min_periods=20)
    df_neg["step_threshold"] = (df_neg["Accel_mag_neg_avg"] - (std_dev_threshold * df_neg["rolling_std_dev"]))
    df_neg["step"] = (df_neg["Accel_mag"] < df_neg["step_threshold"]) & (df_neg["Accel_mag"] < -magnitude_threshold)

    df_neg = df_neg.apply(lambda row: filter_steps_forward(row, df_neg), axis=1)
    df_neg = df_neg.apply(lambda row: filter_steps_backward(row, df_neg), axis=1)

    df.loc[df_neg.index, "step"] = df_neg.loc[df_neg.index, "step"]
    #df.loc[df_neg.index, "rolling_std_dev"] = df_neg.loc[df_neg.index, "rolling_std_dev"]
    #df.loc[df_neg.index, "step_threshold"] = df_neg.loc[df_neg.index, "step_threshold"]
    df["step"].fillna("False", inplace=True)
    #df["rolling_std_dev"].fillna(method='pad', inplace=True)
    #df["step_threshold"].fillna(method='pad', inplace=True)

    return df

def label_steps_old(df, sd):
    magnitude_threshold = 2
    std_dev_threshold = sd
    df = df.dropna(subset=["Accel_mag"])
    df = df[~df.index.duplicated(keep='first')]
    df["rolling_std_dev"] = pd.rolling_std(df["Accel_mag"], window=50, min_periods=50)
    df["step_threshold"] = (df["Accel_mag_avg"] - (std_dev_threshold * df["rolling_std_dev"]))
    df["step"] = (df["Accel_mag"] < df["step_threshold"]) & (df["Accel_mag"] < -magnitude_threshold)
    df = df.apply(lambda row: filter_steps_forward(row, df), axis=1)
    df = df.apply(lambda row: filter_steps_backward(row, df), axis=1)

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


def get_df_duration(df):
    start = df.head(1).index.get_values()[0]
    end = df.tail(1).index.get_values()[0]
    duration = (np.timedelta64(end - start, 'ms')).astype(float) /1000
    return duration

def get_df_general_features(df):
    start = df.head(1).index[0]
    end = df.tail(1).index[0]
    duration = end - start
    half_duration = duration / 2
    middle = start + half_duration
    hour = middle.hour
    minute = middle.minute

    day_of_week = middle.dayofweek if hour > 6 else (middle - pd.Timedelta('1 days')).dayofweek

    hour_since_6am = hour - 6 if hour >= 6 else hour + 24 - 6
    minutes_since_6am = (hour_since_6am * 60) + minute

    result = {
        "time" : minutes_since_6am,
        "day_of_week" : day_of_week
    }
    return result



