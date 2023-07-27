import numpy as np
import matplotlib.pyplot as plt
import re
from six.moves import xrange, map as imap
from datetime import datetime

# For strings like "14.95s" we need to remove "s" before converting to float
time_format = re.compile(r"(?P<seconds>\d+(\.\d+)?)s?")

def parse_time_to_float_seconds(time):
    parts = time_format.match(str(time)).groupdict()
    return float(parts["seconds"])

def preprocess_time_column(operations, column):
    operations[column] = operations[column].apply(lambda time : parse_time_to_float_seconds(time))

def preprocess_operations(operations):
    time_columns = [
        "start_time",
        "finish_time",
        "real_duration",
        "jobs_total_duration",
        "job_max_duration",
        "preempted_jobs_total_duration",
    ]
    for time_column in time_columns:
        preprocess_time_column(operations, time_column)

# Filters out all operations that are not in given timeframe or have bad state
def time_filter(ops, start_time_threshold, end_time_threshold):
    timeframe_filter = \
        (ops["start_time"] >= start_time_threshold) \
        & (ops["start_time"] + ops["real_duration"] <= end_time_threshold) \
        & (ops["in_timeframe"])

    return ops[timeframe_filter & (ops["operation_state"] != "aborted") & (ops["operation_state"] != "failed")]

# Builds number of running operations for each moment of time
def build_load_history(operations):
    start_times = operations["start_time"] // 60
    end_times = operations["finish_time"] // 60
    min_time = int(min(start_times))
    max_time = int(max(end_times))
    print("Min time: {}m".format(min_time))
    print("Max time: {}m".format(max_time))
    history = np.zeros(max_time - min_time + 1, np.int16)

    for t in imap(int, start_times - min_time):
        history[t] += 1

    for t in imap(int, end_times - min_time):
        history[t] -= 1

    for t in xrange(1, max_time - min_time):
        history[t] += history[t - 1]

    return (history, min_time, max_time)

def plot_job_count_distribution(ranges, operations):
    plt.hist(operations["job_count"].values, bins=ranges)
    plt.gca().set_xscale("log")
    plt.show()

def diffNorm(a, b):
    return (a - b) / (a + b)

def plot_differences(operations, weighted=False):
    durations = operations["finish_time"] - operations["start_time"]
    differences = diffNorm(operations["real_duration"].values, durations.values)
    plt.figure(figsize=(12, 6))
    if weighted:
        plot = plt.hist(differences, weights=operations["job_count"].values, bins=30)
    else:
        plot = plt.hist(differences, bins=30)
    plt.xlim(-0.7, 0.7)
    plt.axvline(0, color="r", linestyle="dashed", linewidth=2)
    plt.show()

# Lists all operations with large deviation
def find_suspicious_operations(operations, threshold=1000):
    for row in operations.iterrows():
        row = row[1]
        start = row["start_time"]
        end = row["finish_time"]
        real_duration = row["real_duration"]
        sim_duration = end - start
        diff = real_duration - sim_duration
        diffNormalized = diffNorm(real_duration, sim_duration)

        if abs(diff) > threshold:
            print(row)
            print("Difference: {}".format(diff))
            print("Difference normalized: {}".format(diffNormalized))

def split_into_bins(objects, values, ranges):
    bins = []
    for i in xrange(len(ranges)):
        L = ranges[i]
        R = np.inf if i + 1 >= len(ranges) else ranges[i + 1]
        bins.append(objects[(values >= L) & (values < R)])
    return bins

def analyze_durations(operations):
    time_per_job = operations["real_duration"] / operations["job_count"]
    hist(time_per_job.values)
    print(mean(time_per_job))

def get_finish_time_percentiles(operations):
    percentiles = [100, 99.9]
    result = []
    finish_time = operations["finish_time"]
    for percentile in percentiles:
        result.append((percentile, np.percentile(finish_time, percentile)))
    return result

def parse_timestamp(timestamp_str):
    # "2012-10-19T11:22:58.190448Z"
    return datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%S.%fZ")
