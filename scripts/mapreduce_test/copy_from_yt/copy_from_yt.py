#!/usr/bin/env python

import yt

#yt.run_map('./prepare.py | ./mapreduce -server "n01-0449g.yt.yandex.net:8013" -write access-log/2012-07-04', '//statbox/access-log/"2012-07-04"', "//home/ignat/temp", files=["../mapreduce", "prepare.py"], format=yt.DsvFormat())
yt.run_map('./mock.py | ./mapreduce -server "n01-0449g.yt.yandex.net:8013" -write access-log/2012-07-04', '//statbox/access-log/"2012-07-04"', "//home/ignat/temp", files=["../mapreduce", "mock.py"], format=yt.DsvFormat())
