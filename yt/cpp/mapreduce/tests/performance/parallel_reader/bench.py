import yt.wrapper as yt

import copy
import itertools
import json
import os
import subprocess
import sys
import time

with open(os.path.expanduser("~/.yt/token")) as f:
    YT_TOKEN = f.read().rstrip()
YT_PROXY="freud.yt.yandex.net"
TABLE_PATH="//home/levysotsky/benchmarks/jupiter-dev-mercury-rotated-UrlsLog-20181019-060021-UrlsLog-concat"

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

def cpp(num_rows, num_threads, ordered, format):
    subprocess.call(map(str, ["./cpp", TABLE_PATH, num_rows, num_threads, int(ordered), format]))

def python(num_rows, num_threads, ordered):
    if num_threads > 1:
        yt.config["read_parallel"]["enable"] = True
        yt.config["read_parallel"]["max_thread_count"] = num_threads
    else:
        yt.config["read_parallel"]["enable"] = False
        ordered = True
    s = 0
    for row in yt.read_table(TABLE_PATH + "[:#{}]".format(num_rows), unordered=not ordered):
        s += len(row)
    return s

def make_runner(client, args):
    return lambda num_rows: client(num_rows, *args)

funcs = {
    "curl": curl,
}

for num_threads, ordered in itertools.product(
    [1,2,4,6,8],
    [True, False],
):
    if num_threads == 1 and not ordered:
        continue
    name = "python x{} {}".format(num_threads, "ordered" if ordered else "unordered")
    funcs[name] = make_runner(python, (num_threads, ordered))
    for format in ["proto", "node"]:
        name = "cpp x{} {} {}".format(num_threads, "ordered" if ordered else "unordered", format)
        funcs[name] = make_runner(cpp, (num_threads, ordered, format))

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
            print("  running {}".format(name))
            func(num_rows)
            times[name].append(time.time() - start)
    with open("times-{}_rows-{}_reps.json".format(num_rows, num_reps), "w") as f:
        json.dump(times, f)
    for name, ts in sorted(times.iteritems()):
        print("{}: {}".format(name, sum(ts) / num_reps))
