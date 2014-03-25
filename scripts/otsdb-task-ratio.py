#!/usr/bin/python

import collections
import sys
import time
import urllib2
import os
import urllib
import traceback

CONFIG = {'time_offset' : 5 * 60,         
          'fetch_window' : 5 * 60,
          'smoothing_window' : 15 * 60,
          'interval' : 1 * 60,
          'time_format' : '%Y/%m/%d-%H:%M:%S',
          'hosts' : [
              'w394.hdp.yandex.net:8444',
              'w395.hdp.yandex.net:8444',
              'w396.hdp.yandex.net:8444'
          ],
          'tags' : '{host=scheduler01-001g.yt.yandex.net}'}


def get_timestamp_str(timestamp):
    return time.strftime(CONFIG['time_format'], time.localtime(timestamp))


def load_data(timestamp, metric, fetch_window):
    params = {'start' : get_timestamp_str(timestamp - fetch_window),
              'end' : get_timestamp_str(timestamp),
              'm' : 'sum:' + metric + CONFIG['tags']}
    print >>sys.stderr, 'Loading data'
    hosts = CONFIG['hosts']
    for i in xrange(0, len(hosts)):
        host = hosts[i]
        try:
            print >>sys.stderr, 'Trying to collect data from ' + host + '\n'
            otsdb_url = 'http://' + host + '/q?'
            otsdb_url += urllib.urlencode(params).replace('%3A', ':').replace('%2F', '/')
            otsdb_url += '&ascii'
            hosts[0], hosts[i] = hosts[i], hosts[0]
            print >>sys.stderr, 'Data collected from ' + host + '\n'
            return urllib2.urlopen(otsdb_url, timeout = 10).read()
        except:
            print >>sys.stderr, 'Failed to collect data from ' + host + '\n' + \
                                traceback.format_exc()
    raise RuntimeError('Failed to collect data from all OTSDB hosts')


def get_metric_value(timestamp, metric, fetch_window = CONFIG['fetch_window']):
    data = load_data(timestamp, metric, fetch_window).split('\n')
    prev_time, prev_value = None, None
    curr_time, curr_value = 0.0, 0.0
    for line in data[0:-1]:
        curr_time, curr_value = line.split(' ')[1:3]
        curr_time, curr_value = float(curr_time), float(curr_value)
        if curr_time > timestamp:
            if prev_value != None:
                return (timestamp - prev_time) / (curr_time - prev_value) * (curr_value - prev_value) + prev_value
            else:
                print >>sys.stderr, 'Error ' + '\n'
                return None
        else:
            prev_time, prev_value = curr_time, curr_value
    return None


def get_metric_diff(timestamp, metric):
    prev_value = get_metric_value(timestamp - CONFIG['smoothing_window'], metric)
    curr_value = get_metric_value(timestamp, metric)
    if prev_value != None and curr_value != None:
        return curr_value - prev_value
    elif prev_value == None and curr_value == None:
        if get_metric_value(timestamp, metric, CONFIG['smoothing_window']) == None:
            print >>sys.stderr, 'There is no data for metric ' + metric + ', in period ' + \
                get_timestamp_str(timestamp - CONFIG['smoothing_window']) + ' - ' + \
                get_timestamp_str(timestamp) + '. So the difference is zero'
            return 0
    print >>sys.stderr, 'Failed to get data for metric: ' + metric + ', timestamp: ' + get_timestamp_str(timestamp)
    return None

def print_ratio(timestamp):
    completed_metric = 'yt.scheduler.total_completed_job_time.max'
    aborted_metric = 'yt.scheduler.total_aborted_job_time.max'
    failed_metric = 'yt.scheduler.total_failed_job_time.max'

    completed_dtime = get_metric_diff(timestamp, completed_metric)
    aborted_dtime = get_metric_diff(timestamp, aborted_metric)
    failed_dtime = get_metric_diff(timestamp, failed_metric)

    if failed_dtime == None or aborted_dtime == None or failed_dtime == None:
        return

    assert completed_dtime >= 0.0, 'completed_dtime is less than zero'
    assert aborted_dtime >= 0.0, 'aborted_dtime is less than zero'
    assert failed_dtime >= 0.0, 'failed_dtime is less than zero'
    
    full_dtime = completed_dtime + aborted_dtime + failed_dtime

    completed_ratio = completed_dtime / full_dtime
    aborted_ratio = aborted_dtime / full_dtime
    failed_ratio = failed_dtime / full_dtime

    print "yt.scheduler.completed_job_time_ratio %d %f" % (int(timestamp), completed_ratio)
    print "yt.scheduler.aborted_job_time_ratio %d %f" % (int(timestamp), aborted_ratio)
    print "yt.scheduler.failed_job_time_ratio %d %f" % (int(timestamp), failed_ratio)


def main():
    while True:
        try:
            curr_time = time.time()
            print_ratio(curr_time - CONFIG['time_offset'])
        except:
            print >>sys.stderr, 'Failed to compute time ratios\n' + \
                                 traceback.format_exc()
        finally:
            sys.stdout.flush()
            time.sleep(CONFIG['interval'] - (time.time() - curr_time))

if __name__ == "__main__":
    main()
