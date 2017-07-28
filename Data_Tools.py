import pandas as pd
import matplotlib.pyplot as plt
from pprint import pprint
import datetime as dt

# Takes one sensor data file and converts to a pandas dataframe
def get_file_as_df(db, sensingPeriod, sensor):
    file_id = sensingPeriod + "-" + sensor
    record = db.data.find_one({"_id":file_id})
    file_path = record["filePath"]
    try:
        df = pd.read_csv(file_path, index_col=0, header=None)

        if sensor == 'Accelerometer':
            df.columns = ["Accel_x (N)", "Accel_y (N)", "Accel_z (N)"]
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

    except:
        print (sensor + " empty")

        return None



# For a given sensing period, convert all files to dataframes and merge the dataframes
def get_all_data_for_period(db, sensingPeriod):
    main_df = get_file_as_df(db, sensingPeriod, "Accelerometer")

    for sensorRecord in db.data.find({"period": sensingPeriod}):
        sensor = sensorRecord["sensorType"]
        if sensor != "Accelerometer":
            sub_df = get_file_as_df(db, sensingPeriod, sensor)
            if sub_df is not None:
                main_df = pd.merge(main_df, sub_df, how='outer', left_index=True, right_index=True)

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
    df["sq_rt_sum_sq"] = (df["Accel_x (N)"] **2 + df["Accel_y (N)"] ** 2 + df["Accel_z (N)"] ** 2)**(1/2)
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


# For a given sensing period, generate the all-data dataframe, then filter out non-walking periods
# then add a boolean column that labels the steps in the accelerometer data
def get_step_labelled_walking_data(db, sensingPeriod):
    df = get_all_data_for_period(db, sensingPeriod)
    df_walking = df[df["Motion_walking"] == True]
    df_walking = df_walking.dropna(subset=["Accel_mag"])
    df_walking["rolling_std_dev"] = pd.rolling_std(df_walking["Accel_mag"], 50)
    df_walking["step"] = df_walking["Accel_mag"] < (df_walking["Accel_mag_avg"] - (1.25 * df_walking["rolling_std_dev"]))
    df_walking = df_walking.apply(lambda row: filter_steps(row, df_walking), axis=1)

    return df_walking

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


#Plot accelerometer data with steps labelled with an 's'
def plot_labelled_steps(df):
    times = df.index.values
    vals = df["Accel_mag"].values
    labels = df["step"].values

    fig, ax = plt.subplots()
    ax.plot(times,vals)

    for i, label in enumerate(labels):
        if label == True:
            ax.annotate("s", (times[i], vals[i]))

    plt.show()

#Plot any general numerical data against the time index
def plot_general(df, column):
    times = df.index.values
    vals = df[column].values

    plt.plot(times, vals)
    plt.show()


