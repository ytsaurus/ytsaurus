import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter, drange
import numpy as np
import json
import itertools
from collections import defaultdict
from datetime import datetime, timedelta

def take(iter):
    count = 0
    for x in iter:
        yield x
        count += 1
        if count == 1000:
            break

def avg(list):
    if not list:
        return 0.0
    return float(sum(list)) / len(list)
    

data = map(json.loads, open("/Users/ignat/aggregated_data"))
filtered = filter(lambda row: row["timestamp"].startswith("2014-06-"), data)

print len(filtered)

fair_share = defaultdict(lambda: [])
for row in filtered:
    rounded_timestamp = row["timestamp"][:-4] + "0:00"
    if row["event_type"].startswith("fair_share"):
        fair_share[rounded_timestamp].append(row["residue"])

keys = [(datetime(year=2014, month=6, day=10) + timedelta(seconds=10 * 60 * i)).strftime("%Y-%m-%dT%H:%M:%S") for i in xrange(8 * 24 * 6)]
xrange = drange(datetime(year=2014, month=6, day=10), datetime(year=2014, month=6, day=18), timedelta(seconds=600))
print len(keys), len(xrange)

fig, ax = plt.subplots() #(212) #(211)
ax.plot(xrange, [avg(fair_share[key]) for key in keys])

ax.xaxis.set_major_formatter( DateFormatter('%d June %H:%M') )

plt.show()
