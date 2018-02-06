import yt.wrapper as yt

import copy
import json
import os
import subprocess
import sys
import time

with open(os.path.expanduser("~/.yt/token")) as f:
    YT_TOKEN = f.read().rstrip()
YT_PROXY="freud.yt.yandex.net"
TABLE_PATH="//home/levysotsky/benchmarks/antispam-cheki-9448_dor_sample-final_HAM_SPAM"

yt.config["proxy"]["url"] = YT_PROXY

def curl(num_rows):
    path = TABLE_PATH + "[:#{}]".format(num_rows)
    dev_null = open("/dev/null", "w")
    args = [
        "curl",
        "-S", "-v", "-L", "-X",
        "GET", "http://{}/api/v3/read_table".format(YT_PROXY),
        "-H", "Authorization: OAuth {}".format(YT_TOKEN),
        "-H", "X-YT-Header-Format: <format=text>yson",
        "-H", "X-YT-Output-Format: <format=binary>yson",
        "-H", "X-YT-Parameters: {{ path=\"{}\" }}".format(path)]
    subprocess.check_call(args, stdout=dev_null, stderr=dev_null)

def cpp(num_rows):
    subprocess.call(["./cpp", TABLE_PATH, str(num_rows)])

def python(num_rows, num_threads=1, unordered=False):
    if num_threads > 1:
        yt.config["read_parallel"]["enable"] = True
        yt.config["read_parallel"]["max_thread_count"] = num_threads
    else:
        yt.config["read_parallel"]["enable"] = False
    s = 0
    for row in yt.read_table(TABLE_PATH + "[:#{}]".format(num_rows), unordered=unordered):
        s += len(row)
    return s

funcs = {
    "curl": curl,
    "cpp": cpp,
    "python": python,
    "python x2 ordered": lambda num_rows: python(num_rows, 2, False),
    "python x4 ordered": lambda num_rows: python(num_rows, 4, False),
    "python x8 ordered": lambda num_rows: python(num_rows, 8, False),
    "python x2 unordered": lambda num_rows: python(num_rows, 2, True),
    "python x4 unordered": lambda num_rows: python(num_rows, 4, True),
    "python x8 unordered": lambda num_rows: python(num_rows, 8, True),
}

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: {} <num-rows> <num-repetitions>".format(sys.argv[0]))
        exit(1)
    num_rows = int(sys.argv[1])
    num_reps = int(sys.argv[2])

    times = {name: [] for name in funcs}
    for rep_idx in xrange(num_reps):
        print("Repetition {}/{}".format(rep_idx + 1, num_reps))
        for name, func in funcs.iteritems():
            start = time.time()
            func(num_rows)
            times[name].append(time.time() - start)
    with open("times-{}_rows-{}_reps.json".format(num_rows, num_reps), "w") as f:
        json.dump(times, f)
    for name, ts in times.iteritems():
        print("{}: {}".format(name, sum(ts) / num_reps))
