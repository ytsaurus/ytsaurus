#!/usr/bin/python

import collections
import json
import math
import re
import socket
import sys
import time
import traceback
import urllib2
import os

class YTCollector(object):
    def __init__(self, config_json):
        self.config = {
            u'interval': 30, # collector invocation period
            u'sources': [ ],
            u'window': 30, # aggregation window size in seconds
            u'metric_sync_period': 10 # reload metric names each N collect cycles
        }
        self.config.update(json.loads(config_json))

        endpoints = [self.get_endpoint(source) for source in self.config['sources']]

        self.sources = []
        for endpoint in endpoints:
            self.sources.append({
                'endpoint': endpoint,
                'status': False,
                'last_metric_sync': 0,
                'metrics': {}})

        self.window = int(self.config['window'])
        self.regex = re.compile("[^a-zA-Z0-9_/]")

    def load_metric_names(self, source):
        start_time = time.time()

        # get service name
        service_name_url = 'http://' + source['endpoint'] + '/orchid/@service_name'
        service = urllib2.urlopen(service_name_url, timeout=5).read().replace('"', '')

        # get metric paths
        new_metrics = {}
        profiling_url = 'http://' + source['endpoint'] + '/orchid/profiling'
        metric_paths = self.get_paths(self.get_json(profiling_url))

        # convert to graphite metrics
        host = source['endpoint'].split('.')[0]
        port = source['endpoint'].split(':')[1]

        for path in metric_paths:
            cleaned_path = self.regex.sub('_', path).replace('/', '.')
            g_metric = 'yt' + cleaned_path
            new_metrics[g_metric] = {'service': service, 'port': port, 'path': path, 'last_time': 0, 'tail': []}

        # update existing metrics
        if len(new_metrics) > 0:
            for (name, metric) in source['metrics'].items():
                if name in new_metrics:
                    new_metrics[name]['last_time'] = metric['last_time']
                    new_metrics[name]['tail'] = metric['tail']

        source['metrics'] = new_metrics
        source['last_metric_sync'] = 0

        self.publish_with_timestamp('yt.collector.load_time', time.time() - start_time, int(time.time()), 3)

    def collect_from_source(self, source):
        start_time = time.time()
        value_count = 0

        for (name, metric) in source['metrics'].items():
            vals = self.get_metric_values(source, metric)
            for v in vals:
                self.publish_with_timestamp(name + '.avg', int(v['avg']), v['time'], service=metric['service'], port=metric['port'])
                self.publish_with_timestamp(name + '.max', int(v['max']), v['time'], service=metric['service'], port=metric['port'])
                value_count += 2

        self.publish_with_timestamp('yt.collector.collect_count', value_count, int(time.time()), what="values")
        self.publish_with_timestamp('yt.collector.collect_count', len(source['metrics']), int(time.time()), what="metrics")

    def get_metric_values(self, source, metric):
        metric_url = 'http://' + source['endpoint'] + '/orchid/profiling' + metric['path']
        from_time = metric['last_time']
        if from_time > 0:
            metric_url += '?from_time=' + str(long(from_time*1E6))

        data = self.get_json(metric_url)
        if isinstance(data, list):
            values = []
            cur_bucket = from_time / self.window
            cur_vals = metric['tail']
            time = from_time
            for d in data:
                time = long(d['time']/1E6)
                val = d['value']
                bucket = time / self.window
                if bucket != cur_bucket:
                    if len(cur_vals) > 0:
                        values.append({ 'time': cur_bucket * self.window, 
                                        'avg': self.aggregate_avg(cur_vals), 
                                        'max': self.aggregate_max(cur_vals)})
                        cur_vals = []
                    cur_bucket = bucket
                cur_vals.append(val)
            metric['last_time'] = time
            metric['tail'] = cur_vals
            return values
        else:
            print >>sys.stderr, 'YTCollector: Unexpected reply from %s' % metric_url
            return []

    def publish_with_timestamp(self, name, value, timestamp, precision=0, **kwargs):
        fstring = "%%s %%d %%.%df" % precision
        sstring = fstring % (name, timestamp, value)
        tstring = " ".join("%s=%s" % (k,v) for (k, v) in kwargs.items())
        print sstring + " " + tstring

    def collect(self):
        iter_start = time.time()

        for source in self.sources:
            source['last_metric_sync'] += 1
            try: 
                if (source['status'] is False or 
                    source['last_metric_sync'] >= self.config['metric_sync_period']):
                    self.load_metric_names(source)
                    source['status'] = True # available
                if source['status'] is True:
                    self.collect_from_source(source)
            except Exception, e:
                print >>sys.stderr, 'YTCollector: Failed to collect data from ' + source['endpoint'] + '\n' + traceback.format_exc()
                source['status'] = False # failed

        self.publish_with_timestamp('yt.collector.collect_time', time.time() - iter_start, int(time.time()), 3)

    @staticmethod
    def aggregate_avg(vals):
        sum = 0
        for v in vals:
            sum += v
        return int(sum/len(vals))

    @staticmethod
    def aggregate_max(vals):
        max = 0
        for v in vals:
            if v > max:
                max = v
        return max

    @staticmethod
    def get_endpoint(endpoint_str):
        if ':' in endpoint_str:
            return endpoint_str
        else:
            return '%s:%s' % (socket.getfqdn(), endpoint_str)

    @staticmethod
    def get_json(url):
        resp = urllib2.urlopen(url, timeout=5).read()
        return json.JSONDecoder(object_pairs_hook=collections.OrderedDict).decode(resp)

    @staticmethod
    def get_paths(data, root =''):
        paths = []
        subpaths = []
        for (key, value) in data.items():
            if value is None:
                subpaths.append(key)
            else:
                subpaths = subpaths + YTCollector.get_paths(value, key)
        for subpath in subpaths:
            paths.append(root + '/' + subpath)
        return paths

if __name__ == "__main__":
    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'etc', 'yt_collector.json')
    config_path = os.path.normpath(config_path)
    config_json = open(config_path, "r").read()

    collector = YTCollector(config_json)

    while True:
        collector.collect()
        time.sleep(collector.config['interval'])
