import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pprint import pprint
import datetime as dt
from scipy.signal import welch
from scipy.integrate import simps

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
    dfs = [g for i,g in df.groupby(df['Motion_walking'].ne(df['Motion_walking'].shift()).cumsum())]
    dfs_walking = []
    dfs_non_walking = []
    for d in dfs:
        start = d.head(1).index.get_values()[0]
        end = d.tail(1).index.get_values()[0]
        duration = (np.timedelta64(end - start, 's')).astype(int)
        if d.iloc[0]['Motion_walking'] and duration > 30:
            dfs_walking.append(d)
        else:
            dfs_non_walking.append(d)
    for i in range(len(dfs_walking)):
        dfs_walking[i] = label_steps(dfs_walking[i])

    return dfs_walking, dfs_non_walking


def get_walking_statistics(dfs):

    walking_data = []
    for df in dfs:
        freq_stats = get_walking_frequency_stats(df)
        df = label_anti_steps(df)

        if 'anti_step' in df:
            df_anti = df[df["anti_step"] == True]
            average_gait_stretch = df_anti["gait_stretch"].mean()
        else:
            average_gait_stretch = 0

        df_steps = df[df["step"] == True]

        if len(df_steps.index) > 0:
            start = df_steps.head(1).index.get_values()[0]
            end = df_steps.tail(1).index.get_values()[0]
            duration = (np.timedelta64(end - start, 's')).astype(int)

            step_count = df_steps["step"].value_counts()[True]

            if (step_count > 0) and (duration > 0):
                cadence = step_count / duration

                df_steps["step_time"] = df_steps.index.to_series().diff().astype('timedelta64[ms]')
                df_steps["step_time"] = np.where((df_steps["step_time"] < 2000), df_steps["step_time"], -1)
                df_steps["step_time"].fillna((-1), inplace=True)

                average_step_time = df_steps[df_steps["step_time"] > 0]["step_time"].mean() / 1000


                skewness = df["Accel_mag"].skew()
                kurtosis = df["Accel_mag"].kurtosis()

                results = {"start_time": start,
                                     "duration": duration,
                                     "step_count": step_count,
                                     "cadence": cadence,
                                     "step_time": average_step_time,
                                     "gait_stretch": average_gait_stretch,
                                     "skewness": skewness,
                                     "kurtosis": kurtosis}
                for key in freq_stats.keys():
                    results[key] = freq_stats[key]

                walking_data.append(results)

    return walking_data


def get_walking_frequency_stats(df):
    df = df["Accel_mag"]
    df = df[~df.index.duplicated(keep='first')]
    df = df.resample('ms').interpolate()
    df = df.resample('25ms').interpolate()
    df = df.dropna()
    array = df.as_matrix().ravel()
    fs = 1000/25
    f, pxx = welch(array, fs=fs, return_onesided=True)
    total_power = simps(pxx, f)
    df = pd.DataFrame(f, columns=["frequency"])
    df["power"] = pd.Series(pxx)
    low_df = df[df["frequency"] <= 3]
    high_df = df[df["frequency"] > 3]
    low_freq_power = simps(low_df["power"].as_matrix(), low_df["frequency"].as_matrix())
    high_freq_power = simps(high_df["power"].as_matrix(), high_df["frequency"].as_matrix())
    power_ratio = high_freq_power / low_freq_power
    stats = {"total_power": total_power,
             "power_ratio": power_ratio}
    return stats


# Filter out non-accelerometer data and label the steps
def label_steps(df):
    df = df.dropna(subset=["Accel_mag"])
    df["rolling_std_dev"] = pd.rolling_std(df["Accel_mag"], 50)
    df["step"] = (df["Accel_mag"] < (df["Accel_mag_avg"] - (1.25 * df["rolling_std_dev"]))) & (df["Accel_mag"] < -3)
    df = df.apply(lambda row: filter_steps(row, df), axis=1)

    return df

# Used by function above to filter out accelerometer peaks which have been labelled as steps but are
# actually just artifacts (too close to a real step)
def filter_steps(row, df):
    time = row.name + dt.timedelta(milliseconds=0.01)
    time_plus_half_sec = time + dt.timedelta(milliseconds=250)
    if row["step"]:
        sub_df = df[time : time_plus_half_sec]
        for index, sub_row in sub_df.iterrows():
            if sub_row["step"]:
                row["step"] = False
                break
    return row

def label_anti_steps(df):
    for index, row in df.iterrows():
        if row["step"]:
            time = index + dt.timedelta(milliseconds=0.001)
            forward_df = df[time:]
            for index2, row2 in forward_df.iterrows():
                if row2["step"]:
                    sub_df = forward_df[index:index2]
                    max_index = sub_df["Accel_mag"].argmax()
                    df.loc[max_index, "anti_step"] = True
                    df.loc[max_index, "gait_stretch"] = df.loc[max_index, "Accel_mag"] - row["Accel_mag"]
                    break
    if 'anti_step' in df:
        df["anti_step"].fillna(False, inplace=True)
    if 'gait_stretch' in df:
        df["gait_stretch"].fillna((-1), inplace=True)
    return df



#Plot accelerometer data with steps labelled with an 's'
def plot_labelled_steps(df):
    times = df.index.values
    vals = df["Accel_mag"].values
    step_labels = df["step"].values
    anti_step_labels = df["anti_step"].values

    fig, ax = plt.subplots()
    ax.plot(times,vals)

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
    plt.show()


