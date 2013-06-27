#!/usr/bin/env python

import sys
import matplotlib
import matplotlib.pyplot as plt
from scipy.interpolate import spline
import numpy as np

from matplotlib.dates import HourLocator, MinuteLocator, DateFormatter

from collections import defaultdict
from datetime import datetime

res = defaultdict(lambda: [])

ignore = False
counter = 0
while True:
    line = sys.stdin.readline()
    if not line:
        break
    if  line.strip() == "RESET":
        ignore = True
        continue
    if line.strip() == "SEP":
        ignore = False
        continue

    if ignore:
        continue

    fields = line.split()
    res[int(fields[2])].append(int(fields[8]))

times = []
values = []

for k, v in sorted(res.items()):
    times.append(k)
    v = v[:24]
    values.append(sum(v) / 100.0)

def avg(l):
    return sum(l) / len(l)

step = 5
times_smoothed = []
values_smoothed = []
for i in xrange(0, len(times), step):
    times_smoothed.append(int(avg(times[i:i + step])))
    values_smoothed.append(avg(values[i:i + step]))

#times_smoothed = np.linspace(min(times), max(times), 20)
#values_smoothed = spline(times, values, times_smoothed)

fig = plt.figure()
ax = fig.add_subplot(111)

ax.plot_date(matplotlib.dates.date2num(map(datetime.fromtimestamp, times_smoothed)), values_smoothed, "-")
ax.xaxis.set_major_locator(HourLocator(interval=4))
ax.xaxis.set_major_formatter(DateFormatter('%m-%d %H'))
#ax.fmt_xdata = DateFormatter('%d-%h')
fig.autofmt_xdate()
#ax.xaxis.set_minor_locator(MinuteLocator())
#ax.autoscale_view()

plt.savefig(sys.argv[1])
