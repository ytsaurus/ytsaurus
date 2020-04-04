#!/usr/bin/env python

# Description: gets stream of numbers and make histogram with 50 bins

import numpy as np
import matplotlib.mlab as mlab
import matplotlib.pyplot as plt

import sys

sizes = [float(line.strip()) for line in sys.stdin]

# the histogram of the data
n, bins, patches = plt.hist(sizes, 50, facecolor='green', alpha=0.75) #normed=1

plt.xlabel('Size')
plt.ylabel('Numnber of chunks')
plt.grid(True)

plt.show()

