#!/usr/bin/env python

# Description: gets stream of numbers and make histogram with 50 bins

import numpy as np
import matplotlib.mlab as mlab
import matplotlib.pyplot as plt

import sys

sizes = [int(line.strip()) for line in sys.stdin]

# the histogram of the data
# n, bins, patches = plt.hist(sizes, len(sizes), facecolor='blue', alpha=0.75) #normed=1
plt.bar(xrange(len(sizes)), sizes) 

plt.xlabel('Sort, %s, sum=%s' % (sys.argv[1], sys.argv[2]))
plt.ylabel('Time(sec)')
plt.yticks(range(0, 2000, 100))
plt.grid(True)

plt.savefig(sys.argv[3])
