#!/usr/bin/env python

import os
import sys
import random
import subprocess

if __name__ == "__main__":
    number = random.randint(0, 1000000000)
    output_table = "temp/%d" % number
    command = './mapreduce -server "n01-0449g.yt.yandex.net:8013" -write ' + output_table
    proc = subprocess.Popen(command, stdin=sys.stdin, shell=True)
    proc.communicate()

    print "table=%s" % output_table

