#!/usr/bin/python

import collections
import sys
import time
import urllib2
import os
import urllib
import traceback

CONFIG = {'window' : 5 * 60,
          'time_format' : '%Y/%m/%d-%H:%M:%S',
          'hosts' : ['w394.hdp.yandex.net:8444', 'w395.hdp.yandex.net:8444', 'w396.hdp.yandex.net:8444'],
          'tag' : '{host=scheduler01-001g.yt.yandex.net}',
          'delta' : 15 * 60,
          'offset' : 5 * 60}

def get_timestamp_str(timestamp):
    return time.strftime(CONFIG['time_format'], time.localtime(timestamp))

def load_data(timestamp, metric = 'yt.scheduler.total_aborted_job_time.max'):
    params = {'start' : get_timestamp_str(timestamp - CONFIG['window']),
              'end' : get_timestamp_str(timestamp),
              'm' : 'sum:' + metric + CONFIG['tag']}
    for host in CONFIG['hosts']:
        try:
            otsdburl = "http://" + host + "/q?" +  urllib.urlencode(params).replace("%3A", ":").replace("%2F", "/") + "&ascii"
            return urllib2.urlopen(otsdburl, timeout = 30).read()
        except Exception, e:
            continue
            #print >>sys.stderr, 'Failed to collect data from ' + host + '\n' + traceback.format_exc()

def get_metric_value(timestamp, metric):
    data = load_data(timestamp, metric).split('\n')
    prev_time, prev_value = 0, 0;
    for line in data[0:-1]:
        currtime, value = line.split(' ')[1:3]
        currtime, value = float(currtime), float(value)
        if currtime > timestamp:
            return prev_value
        else:
            prev_time, prev_value = currtime, value
    return prev_value

def get_metric_diff(timestamp, metric = 'yt.scheduler.total_aborted_job_time.max'):
    prev_value = get_metric_value(timestamp - CONFIG['delta'], metric)
    curr_value = get_metric_value(timestamp, metric)
    return curr_value - prev_value


def print_ratio(curr_time):
    aborted_metric = 'yt.scheduler.total_aborted_job_time.max'
    failed_metric = 'yt.scheduler.total_failed_job_time.max'
    completed_metric = 'yt.scheduler.total_completed_job_time.max'
    completed_dtime = get_metric_diff(curr_time, completed_metric)
    aborted_dtime = get_metric_diff(curr_time, aborted_metric)
    failed_dtime = get_metric_diff(curr_time, failed_metric)
    full_dtime = completed_dtime + aborted_dtime + failed_dtime
    completed_ratio = completed_dtime / full_dtime
    aborted_ratio = aborted_dtime / full_dtime
    failed_ratio = failed_dtime / full_dtime
    print ("yt.scheduler.completed_ratio %d %f" % ( int(curr_time), completed_ratio))
    print ("yt.scheduler.aborted_ratio %d %f" % ( int(curr_time), aborted_ratio))
    print ("yt.scheduler.failed_ratio %d %f" % ( int(curr_time), failed_ratio))

def main():
    """ifstat main loop"""
    interval = CONFIG['delta']
    while True:
        curr_time = time.time() - CONFIG['offset']
        print_ratio(curr_time)
        sys.stdout.flush()
        time.sleep(interval)

if __name__ == "__main__":
    main()
