import pandas as pd
import matplotlib.pyplot as plt
from pprint import pprint


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




def get_all_data_for_period(db, sensingPeriod):
    main_df = get_file_as_df(db, sensingPeriod, "Accelerometer")

    for sensorRecord in db.data.find({"period": sensingPeriod}):
        sensor = sensorRecord["sensorType"]
        if sensor != "Accelerometer":
            sub_df = get_file_as_df(db, sensingPeriod, sensor)
            if sub_df is not None:
                main_df = pd.merge(main_df, sub_df, how='outer', left_index=True, right_index=True)

    decide_if_walking(main_df)

    return main_df

def get_accelerometer(db, sensingPeriod):
    df = get_file_as_df(db, sensingPeriod, "Accelerometer")
    calc_accelerometer_magnitude(df)
    print (df)
    return df

def calc_accelerometer_magnitude(df):
    df["sq_rt_sum_sq"] = (df["Accel_x (N)"] **2 + df["Accel_y (N)"] ** 2 + df["Accel_z (N)"] ** 2)**(1/2)
    window = 50
    df["rolling_avg"] = (pd.rolling_sum(df["sq_rt_sum_sq"], window)) / window
    df["Accel_mag"] = df["sq_rt_sum_sq"] - df["rolling_avg"]
    df["Accel_mag_avg"] = (pd.rolling_sum(df["Accel_mag"], window)) / window

def get_motion_activity(db, sensingPeriod):
    df = get_file_as_df(db, sensingPeriod, "MotionActivity")
    decide_if_walking(df)
    print(df)
    return df

def decide_if_walking(df):
    df['Motion_activity'].fillna((-1), inplace=True)
    df.loc[df['Motion_activity'] == 7, 'Motion_walking'] = True
    df.loc[(df['Motion_activity'] != 7) & (df['Motion_activity'] >= 0), 'Motion_walking'] = False
    df['Motion_walking'].fillna(method='ffill', inplace=True)

def plot_file_data(dataframe, column, ms):
    times = dataframe.index.values
    vals = dataframe[column].values
    labels = dataframe["step"].values
    #print(times)
    #print(vals)

    fig, ax = plt.subplots()
    ax.plot(times,vals)

    for i, label in enumerate(labels):
        if label == True:
            ax.annotate("s", (times[i], vals[i]))

    plt.show()


