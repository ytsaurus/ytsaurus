#!/usr/bin/env python

import yt

import sys

operation = '"%s"' % sys.argv[1]

for job in yt.list("//sys/operations/%s/jobs" % operation):
    job_path = "//sys/operations/%s/jobs/%s" % (operation, job)
    if yt.get(job_path + "/@state") == "failed":
        print yt.get(job_path + "/@address")
