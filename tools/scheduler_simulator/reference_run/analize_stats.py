import csv

def read_stats(filename):
    stats = {}
    with open(filename) as fin:
        reader = csv.DictReader(fin)
        for row in reader:
            stats[row['id']] = row
    return stats

def str_value_to_value(value):
    if value[-1] == 's':
        return float(value[:-1])
    return float(value)

def find_stats_key_diff(stable, exp, key):
    mx = -1000000.0
    avg = 0.0
    for id_, stats in stable.iteritems():
        stats_exp = exp[id_]
        stable_value = str_value_to_value(stats[key])
        exp_value = str_value_to_value(stats_exp[key])
        mx = max(mx, abs(stable_value - exp_value))
        avg += abs(stable_value - exp_value)
    avg /= len(stable)
    return {
        'max' : mx,
        'avg' : avg,
    }

def find_stats_diff(stable, exp):
    diff = dict()
    for key in ['job_count', 'preempted_job_count', 'start_time', 'finish_time', 'job_max_duration', 'jobs_total_duration']:
        diff[key] = find_stats_key_diff(stable, exp, key)
    return diff

if __name__ == '__main__':
    stats_stable = read_stats('operations_stats_reference.csv')
    stats_exp = read_stats('operations_stats.csv')
    diff = find_stats_diff(stats_stable, stats_exp)
    for k, v in diff.iteritems():
        print k, v
