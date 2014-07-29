#!/usr/bin/env python

import sys
from urllib import urlencode

from datetime import datetime

def to_timestamp(str):
    return int((datetime.strptime(str, "%Y-%m-%d %H:%M:%S") - datetime(year=1970, month=1, day=1)).total_seconds())

def format(obj):
    if isinstance(obj, str):
        return '"{}"'.format(obj)
    else:
        return str(obj)

hash_keys = ["project", "region_sname"]

print '[Connection: close]'
print '[Host: barney.yt.yandex.net]'
print '[Cookies: None]'
print '[X-YT-Output-Format: "yson"]'

for line in sys.stdin:
    words = line.split()


    values = {words[0]: words[1], words[2]: words[3] }
    values["hash"] = hash(tuple([values[key] for key in hash_keys])) % (2 ** 60)
    
    date_field = None
    if len(words) > 4:
        date_field = words[4]
        dates = (to_timestamp(words[5] + " " + words[6]), to_timestamp(words[7] + " " + words[8]))

    #tail = " ".join(words[9:])
    tail = ""

    query = "project, region_sname, fielddate, hosts, visitors_per_host, puids, old_cookie_hits, visitors, luids, hits, spuids, old_cookie_visitors, hits_per_visitor from [//tmp/totals] WHERE {} {} {}".format(
        " AND ".join('{}={}'.format(key, format(value)) for key, value in values.iteritems()),
        " AND {} > {} AND {} < {} ".format(date_field, dates[0], date_field, dates[1]) if date_field is not None else "",
        tail)

    print "/api/v2/select?" + urlencode({"query": query, "trace": "true"})
        
    
