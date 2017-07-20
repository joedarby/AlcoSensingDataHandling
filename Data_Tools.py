import pandas
import matplotlib.pyplot as plt
from pprint import pprint


def get_file_as_df(db, sensingPeriod, sensor):
    file_id = sensingPeriod + "-" + sensor
    record = db.data.find_one({"_id":file_id})
    file_path = record["filePath"]
    df = pandas.read_csv(file_path, index_col=0, header=None)
    df.index = pandas.to_datetime(df.index, unit='ms')
    print(df.describe())
    return df

def plot_file_data(dataframe, column):
    times = dataframe.index.values
    vals = dataframe[column].values
    #print(times)
    #print(vals)

    plt.plot(times, vals, '+', ms=0.1)
    plt.show()
