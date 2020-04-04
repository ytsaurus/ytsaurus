#!/usr/bin/env python

import sys
import matplotlib
import matplotlib.pyplot as plt
from datetime import datetime
from matplotlib.dates import HourLocator, DateFormatter

def avg(l):
    return sum(l) / len(l)

values = []

for line in sys.stdin:
    time, value = line.split()
    values.append((int(time), float(value)))

values.sort()

times = [x[0] for x in values]
values = [x[1] for x in values]


step = 31
times_smoothed = []
values_smoothed = []
for i in xrange(0, len(values), step):
    times_smoothed.append(int(avg(times[i:i + step])))
    values_smoothed.append(avg(values[i:i + step]))

fig = plt.figure()
ax = fig.add_subplot(111)

ax.plot_date(matplotlib.dates.date2num(map(datetime.fromtimestamp, times_smoothed)), map(lambda x: x * step, values_smoothed), "-")
ax.xaxis.set_major_locator(HourLocator(interval=4))
ax.xaxis.set_major_formatter(DateFormatter('%m-%d %H'))
fig.autofmt_xdate()

plt.savefig(sys.argv[1])
