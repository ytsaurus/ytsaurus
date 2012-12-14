#!/usr/bin/python
#coding: utf8

import os
import yt.wrapper as yt
from dateutil.parser import parse
from collections import namedtuple
from datetime import datetime, timedelta

Oper = namedtuple("Oper", ["time", "id", "state"]);

def clean_operations(count, failed_timeout):
    '''Clean all operations started no more than #days days ago,
leaving no more than #count most recent operations.'''

    operations = yt.get("//sys/operations", attributes=['state', 'start_time'])
    operations = [Oper(parse(v[u"$attributes"]["start_time"]).replace(tzinfo=None), k, \
        v[u"$attributes"]["state"]) for k, v in operations.iteritems()];
    operations.sort(reverse=True)
    
    running = 0
    saved = 0
    to_remove = []

    for x in operations:
        time_since = datetime.utcnow() - x.time 
        if x.state == 'failed' and time_since > failed_timeout:
            to_remove.append(x.id)
        elif x.state not in ['completed', 'aborted']:
            running += 1
        elif (saved + running >= count):
            to_remove.append(x.id)
        else:
            saved += 1

    for x in to_remove:
        os.system('''yt remove '//sys/operations/%s' ''' % x)
    

def main():
    import argparse

    parser = argparse.ArgumentParser(description='Clean operations from cypress.')
    parser.add_argument('--count', metavar='N', type=int, default=50,
                       help='leave history no more than N operations')
    parser.add_argument('--failed_timeout', metavar='N', type=int, default=3,
                       help='remove all failed operation older than N days')

    args = parser.parse_args()
    clean_operations(args.count, timedelta(days=args.failed_timeout))

if __name__ == "__main__":
	main()
