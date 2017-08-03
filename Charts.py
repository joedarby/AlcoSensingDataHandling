from matplotlib import pyplot as plt


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
    plt.title("Power spectral density")
    plt.xlabel("Frequency, Hz")
    plt.ylabel("Power")
    plt.show()


def plot_3_axis(df):
    times = df.index.values
    vals_x = df["Accel_x (ms-2)"]
    vals_y = df["Accel_y (ms-2)"]
    vals_z = df["Accel_z (ms-2)"]
    fig, ax = plt.subplots()
    ax.plot(times, vals_x, label="x", linewidth=0.5)
    ax.plot(times, vals_y, label = "y", linewidth=0.5)
    ax.plot(times, vals_z, label = "z", linewidth=0.5)
    handles, labels = ax.get_legend_handles_labels()
    ax.legend(handles, labels)
    plt.title("Raw accelerometer data")
    plt.xlabel("Time")
    plt.ylabel("Acceleration ms^-2")
    plt.show()


def plot_with_vertical_lines(df, dfs_w):
    times = df.index.values
    vals = df["Accel_mag"].values
    walking = []
    not_walking = []

    for df in dfs_w:
        start = df.head(1).index.get_values()[0]
        end = df.tail(1).index.get_values()[0]
        walking.append(start)
        not_walking.append(end)

    fig, ax = plt.subplots()

    for i in walking:
        ax.axvline(x=i, color='green', label="walking_start", zorder=1)
    for j in not_walking:
        ax.axvline(x=j, color='red', label="walking_end", zorder=2)
    ax.plot(times, vals, linewidth=0.5, label="acceleration", zorder=3)

    handles, labels = ax.get_legend_handles_labels()
    new_handles = []
    new_labels = []
    for h, l in zip(handles, labels):
        if l not in new_labels:
            new_handles.append(h)
            new_labels.append(l)


    ax.legend(new_handles, new_labels)

    plt.title("Raw accelerometer data")
    plt.xlabel("Time")
    plt.ylabel("Acceleration ms^-2")
    plt.show()