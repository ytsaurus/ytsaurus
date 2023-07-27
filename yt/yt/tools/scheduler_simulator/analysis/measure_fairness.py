import matplotlib.pyplot as plt
import ujson as json
from six import iteritems
from six.moves import xrange, map as imap, filter as ifilter

from collections import defaultdict
from os import path

def extract_records(path):
    return list(imap(json.loads, open(path, "r")))

def load_data(directory, target):
    usage = extract_records(path.join(directory, "usage_" + target + ".json"))
    fair_share = extract_records(path.join(directory, "fair_share_" + target + ".json"))
    demand = extract_records(path.join(directory, "demand_" + target + ".json"))

    return usage, fair_share, demand

# Extract all occurrences of pool in fair_share history
def get_pool(records, pool):
    pool_records = []
    for record in records:
        pools = record["pools"]
        if pool in pools:
            pool_records.append(pools[pool])
        else:
            pool_records.append(0)
    return pool_records

def get_moving_window_sum(records, windowSize):
    T = len(records)

    windows_sums = []
    pools_window_sum = defaultdict(float)

    def update_window_sum(time, sign):
        pools = records[time]["pools"]
        for pool, value in iteritems(pools):
            pools_window_sum[pool] += sign * value

    for t in xrange(T):
        if t - windowSize >= 0:
            update_window_sum(t - windowSize, -1);
        update_window_sum(t, +1);

        # All sums are non-negative, so we can remove pools with zero sum
        empty_pools = [k for k, v in iteritems(pools_window_sum) if v < 1e-9]
        for pool in empty_pools:
            del pools_window_sum[pool]

        windows_sums.append(pools_window_sum.copy())

    return windows_sums

# Calculate windowed delay between usage and fair_share
def get_pools_aggregated_window_delay(usage, fair_share, windowSize):
    T = len(usage)

    aggregations = []
    fair_share_moving_window_sum = get_moving_window_sum(fair_share, windowSize)
    usage_moving_window_sum = get_moving_window_sum(usage, windowSize)

    for t in xrange(T):
        aggregation = 0.0
        for pool in fair_share_moving_window_sum[t]:
            pool_fair_share = fair_share_moving_window_sum[t][pool]
            pool_usage = usage_moving_window_sum[t][pool]
            if pool_fair_share > pool_usage:
                aggregation += pool_fair_share - pool_usage
        aggregations.append(aggregation)
    return aggregations

def get_pools_aggregated_delay(usage, fair_share):
    return get_pools_aggregated_window_delay(usage, fair_share, 1)

def get_pools_window_aggregation(records, windowSize):
    moving_window_sum = get_moving_window_sum(records, windowSize)
    aggregations = []
    for pools_window_sum in moving_window_sum:
        aggregations.append(sum(pools_window_sum.values()))
    return aggregations

def get_pools_aggregation(records):
    return get_pools_window_aggregation(records, 1);

def aggregate(records):
    aggregated = defaultdict(float)
    for record in records:
        for pool, value in iteritems(record["pools"]):
            aggregated[pool] += value
    return aggregated

def plot_satisfaction_histogram(satisfaction, usage):
    satisfactions = []
    weights = []
    for pool in satisfaction:
        satisfactions.append(satisfaction[pool])
        weights.append(usage.get(pool, 0))

    plt.hist(satisfactions, weights=weights, bins=50)
    plt.show()

def get_underserved_number(satisfaction, tolerance):
    return len(list(ifilter(lambda s: s < tolerance, satisfaction.values())))

def get_pools_underserved_periods(usage, fair_share, tolerance):
    T = len(usage)
    periods = defaultdict(list)
    current_underserved_pools = {}
    for t in xrange(T):
        pools_usage = usage[t]["pools"]
        pools_fair_share = fair_share[t]["pools"]
        new_underserved_pools = {}

        # Create list of pools who became underserved just now
        for pool in pools_fair_share:
            pool_usage = pools_usage.get(pool, 0)
            pool_fair_share = pools_fair_share[pool]
            if pool_fair_share * tolerance > pool_usage:
                new_underserved_pools[pool] = t

        # Iterate over pools who were underserved on previous step
        for pool, time in iteritems(current_underserved_pools):
            # If pool is still underserved, set correct underserved period start time
            if pool in new_underserved_pools:
                new_underserved_pools[pool] = time
            else:
                duration = t - time
                periods[pool].append(duration)

        # Update underserved pools list with new pools
        current_underserved_pools = new_underserved_pools

    # Process pools that were underserved till very end
    for pool, time in iteritems(current_underserved_pools):
        duration = t - time
        periods[pool].append(duration)
    return periods

def filter_periods(periods, threshold):
    filtered_periods = {}
    for pool in periods:
        filtered = list(ifilter(lambda x: x > threshold, periods[pool]))
        if len(filtered) != 0:
            filtered_periods[pool] = filtered
    return filtered_periods

def weighted_penalty_for_underserved(p, periods):
    penalty = 0.0
    for pool in periods:
        for duration in periods[pool]:
            penalty += duration ** p
    return penalty

