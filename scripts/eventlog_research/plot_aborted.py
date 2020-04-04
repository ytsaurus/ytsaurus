import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter, drange
import numpy as np
import json
import itertools
from collections import defaultdict
from datetime import datetime, timedelta

#day = 11

def take(iter):
    count = 0
    for x in iter:
        yield x
        count += 1
        if count == 1000:
            break
        

def group(points):
    for key, group in itertools.groupby(points, key=lambda point: point["timestamp"]):
        yield key, sum(point["duration"] for point in group)
    

data = map(json.loads, open("/Users/ignat/aggregated_data"))
filtered = filter(lambda row: row["timestamp"].startswith("2014-06-"), data)

print len(filtered)

jobs = defaultdict(lambda: defaultdict(lambda: 0))
for row in filtered:
    rounded_timestamp = row["timestamp"][:-4] + "0:00"
    if row["event_type"].startswith("job_"):
        jobs["all"][rounded_timestamp] += row["duration"]
        jobs[(row["event_type"], row.get("reason", ""))][rounded_timestamp] += row["duration"]
        #if row["event_type"] == "job_aborted":
        #    aborted_jobs[rounded_timestamp] += row["duration"]

keys = [(datetime(year=2014, month=6, day=1) + timedelta(seconds=10 * 60 * i)).strftime("%Y-%m-%dT%H:%M:%S") for i in xrange(17 * 24 * 6)]
xrange = drange(datetime(year=2014, month=6, day=1), datetime(year=2014, month=6, day=18), timedelta(seconds=600))

fig, ax = plt.subplots() #(212) #(211)
plt.ylim([-5, 105])
#ax.plot(xrange, [all_jobs[key] for key in keys])
#ax.plot(xs, [aborted_jobs[key] for key in keys])
for name, values in jobs.iteritems():
    if name == "all":
        continue
    ax.plot(xrange, [((jobs[name][key] * 100.0 / jobs["all"][key]) if key in jobs["all"] else 0.0)  for key in keys], label=", ".join(name))

handles, labels = ax.get_legend_handles_labels()
ax.legend(handles, labels, ncol=1, loc=3)#, bbox_to_anchor=(0.0, 1.02, 1.0, 1.102))

ax.xaxis.set_major_formatter( DateFormatter('%d June %H:%M') )
#fig.autofmt_xdate()


plt.show()
