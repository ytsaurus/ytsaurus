#!/usr/bin/env python
# coding: utf-8

from Queue import Queue
from threading import Thread
import yt.wrapper
from collections import defaultdict

accounts = defaultdict(str)
count = 0

def visitor(path, child):
    global count, accounts
    t = child.attributes['type']
    if t in ['table', 'file']:
        account = child.attributes['account']
        time = child.attributes['modification_time']
        if (time > accounts[account]):
            accounts[account] = time

        count += 1
        if count % 1000 == 0:
            print 'Processed %d nodes' % count

    elif t == 'map_node':
        return '{0}/{1}'.format(path, child)


def traverse(roots, attributes, callback, verb='list', thread_pool_size=10):
    q = Queue()

    if isinstance(roots, basestring):
        roots = [roots]

    if not roots:
        return

    for root in roots:
        q.put(root)

    method = getattr(yt.wrapper, verb)

    def worker():
        while True:
            path = q.get()
            items = method(path, attributes=attributes)
            for item in items:
                p = callback(path, item)
                if p: q.put(p)
            q.task_done()

    for i in xrange(thread_pool_size):
        t = Thread(target = worker)
        t.daemon = True
        t.start()

    q.join()

if __name__ == '__main__':
    yt.wrapper.config.set_proxy('kant.yt.yandex.net')
    traverse( \
        ['//home', '//crypta', '//statbox'], \
        ['type', 'account', 'modification_time'], \
        visitor)
    items = accounts.items()
    items.sort(key=lambda x: x[1])
    for kv in items:
        print '%s, %s' % kv


