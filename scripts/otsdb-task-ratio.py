#!/usr/bin/python

import collections
import sys
import time
import urllib2
import os
import urllib

def get_timestampstr(timestamp):
    return time.strftime("%Y/%m/%d-%H:%M:%S", time.localtime(timestamp))

def load_data(timestamp, metric = 'yt.scheduler.total_aborted_job_time.max', timewindow = 5 * 60):
    params = {'start' : get_timestampstr(timestamp - timewindow), 'end' : get_timestampstr(timestamp), 'm' : 'sum:' + metric + '{host=scheduler01-001g.yt.yandex.net}'}
    otsdburl = "http://otsdb/q?" +  urllib.urlencode(params).replace("%3A", ":").replace("%2F", "/") + "&ascii"
    return urllib2.urlopen(otsdburl, timeout=5).read()

def get_metric_value(timestamp, metric = 'yt.scheduler.total_aborted_job_time.max', timewindow = 7200):
    data = load_data(timestamp, metric, timewindow).split('\n')
    (prev_time, prev_value) = (0, 0);
    for line in data[0:-1]:
        (currtime, value) = line.split(' ')[1:3]
        (currtime, value) = (float(currtime), float(value))
        if currtime > timestamp:
            return prev_value #(timestamp - prev_time) / (currtime - prev_value) * (value - prev_value) + prev_value
        else:
            (prev_time, prev_value) = (currtime, value)
    return prev_value

def get_metric_diff(timestamp, metric = 'yt.scheduler.total_aborted_job_time.max', delta = 60 * 15, timewindow = 7200):
    return get_metric_value(timestamp, metric, timewindow) - get_metric_value(timestamp - delta, metric, timewindow)


def print_ratio(timewindow = 7200):
    aborted_metric = 'yt.scheduler.total_aborted_job_time.max'
    failed_metric = 'yt.scheduler.total_failed_job_time.max'
    completed_metric = 'yt.scheduler.total_completed_job_time.max'
    curr_time = time.time() - 5 * 60
    completed_dtime = get_metric_diff(curr_time, completed_metric, timewindow)
    aborted_dtime = get_metric_diff(curr_time, aborted_metric, timewindow)
    failed_dtime = get_metric_diff(curr_time, failed_metric, timewindow)
    full_dtime = completed_dtime + aborted_dtime + failed_dtime
    completed_ratio = completed_dtime / full_dtime
    aborted_ratio = aborted_dtime / full_dtime
    failed_ratio = failed_dtime / full_dtime
    print ("yt.scheduler.completed_ratio %d %f" % ( int(curr_time), completed_ratio))
    print ("yt.scheduler.aborted_ratio %d %f" % ( int(curr_time), aborted_ratio))
    print ("yt.scheduler.failed_ratio %d %f" % ( int(curr_time), failed_ratio))


def main():
    """ifstat main loop"""
    interval = 15
    while True:
        print_ratio()
        sys.stdout.flush()
        time.sleep(interval)

if __name__ == "__main__":
    main()
