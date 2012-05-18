import collections
import json
import math
import re
import urllib2
import socket
import sys
import time
import traceback
from diamond.collector import Collector
from diamond.metric import Metric

class YtCollector(Collector):

    def __init__(self, config, handlers):
        Collector.__init__(self, config, handlers)

        endpoints = []
        if isinstance(self.config['sources'], list):
            for endpoint_str in self.config['sources']:
                endpoints.append(self.get_endpoint(endpoint_str))
        else:
            endpoints.append(self.get_endpoint(self.config['sources']))

        self.sources = []        
        for endpoint in endpoints:
            self.sources.append({
                'endpoint': endpoint,
                'status': False,
                'metrics': {}})            

        self.window = int(self.config['window'])
        self.regex = re.compile("[^a-zA-Z0-9_/]")

    def get_default_config(self):
        return {
            'interval': 60, # collector invocation period
            'sources': [
                'meta01-001g.yt.yandex.net:10000', 
                'meta01-002g.yt.yandex.net:10000', 
                'meta01-003g.yt.yandex.net:10000'
            ],
            'window': 30 # aggregation window size in seconds
        }

    def load_metric_names(self, source):
        start_time = time.time()

        # get service name
        service_name_url = 'http://' + source['endpoint'] + '/orchid/@service_name'
        service = urllib2.urlopen(service_name_url, timeout=5).read().replace('"', '')
        
        # get metric paths
        new_metrics = {}
        profiling_url = 'http://' + source['endpoint'] + '/orchid/profiling'
        metric_paths = self.get_paths(self.get_json(profiling_url))
        #metric_paths = ['/action_queues/Control/time/total']

        # convert to graphite metrics
        host = source['endpoint'].split('.')[0]
        port = source['endpoint'].split(':')[1]
        for path in metric_paths:
            cleaned_path = self.regex.sub('_', path)
            g_metric = 'yt.%s.%s.%s%s' % (host, service, port, cleaned_path.replace('/', '.'))
            new_metrics[g_metric] = {'path': self.quote_path(path), 'last_time': 0}
        
        # update existing metrics
        if len(new_metrics) > 0:
            for (name, metric) in source['metrics'].items():
                if name in new_metrics:
                    new_metrics[name]['last_time'] = metric['last_time']

        source['metrics'] = new_metrics

        self.log.info('NewYtCollector: Loaded %d metric names from %s in %f sec', 
            len(source['metrics']), source['endpoint'], time.time() - start_time)

    def collect_from_source(self, source):
        start_time = time.time()
        value_count = 0

        for (name, metric) in source['metrics'].items():
            vals = self.get_metric_values(source, metric)
            last_time = metric['last_time']
            for v in vals:
                self.publish_with_timestamp(name + '.avg', int(v['avg']), v['time'])
                self.publish_with_timestamp(name + '.max', int(v['max']), v['time'])
                last_time = v['time']
                value_count += 2
            metric['last_time'] = last_time        
        
        self.log.info('NewYtCollector: Collected %d values for %d metrics from %s in %f sec', 
            value_count, len(source['metrics']), source['endpoint'], time.time() - start_time)

    def get_metric_values(self, source, metric):
        metric_url = 'http://' + source['endpoint'] + '/orchid/profiling' + metric['path']        
        last_time = metric['last_time']
        if last_time > 0:
            from_time = last_time + self.window
            metric_url += '?from_time=' + str(long(from_time*1E6))
        else:
            from_time = 0

        data = self.get_json(metric_url)
        if isinstance(data, list):
            values = []
            cur_bucket = from_time / self.window
            cur_vals = []
            for d in data:
                time = long(d['time']/1E6)                
                val = d['value']
                bucket = time / self.window
                #self.log.info('NewYtCollector: << %s %d %d' % (metric['path'], time, val))
                if bucket != cur_bucket:
                    if len(cur_vals) > 0:
                        values.append({ 'time': cur_bucket * self.window, 
                                        'avg': self.aggregate_avg(cur_vals), 
                                        'max': self.aggregate_max(cur_vals)})
                        cur_vals = []
                    cur_bucket = bucket
                cur_vals.append(val)
            return values
        else:
            # Error...
            self.log.error('NewYtCollector: Unexpected reply from %s', metric_url)
            return []

    def publish_with_timestamp(self, name, value, timestamp, precision=0):
        metric = Metric(name, value, timestamp, precision)
        #self.log.info('NewYtCollector: >> %s %d %d' % (name, timestamp, value))
        self.publish_metric(metric)

    def collect(self):
        iter_start = time.time()

        for source in self.sources:
            try: 
                if source['status'] is False: # unknown or failed
                    self.load_metric_names(source)
                    source['status'] = True # available
                if source['status'] is True:
                    self.collect_from_source(source)
            except Exception, e:
                self.log.error('NewYtCollector: Failed to collect data from ' 
                    + source['endpoint'] + '\n' + traceback.format_exc())
                source['status'] = False # failed

        self.log.info('NewYtCollector: Collected metrics in %f sec', time.time() - iter_start)

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
            if '$type' not in value or value['$type'] != 'entity':
                subpaths = subpaths + YtCollector.get_paths(value, key)
            else:
                subpaths.append(key)
        for subpath in subpaths:
            paths.append(root + '/' + subpath)
        return paths

    @staticmethod
    def quote_path(path):
        return (path.replace('/', '"/"') + '"')[1:]