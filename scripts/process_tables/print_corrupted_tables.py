#!/usr/bin/python

import yt.wrapper as yt

if __name__ == "__main__":
    a = yt.list('//sys/lost_vital_chunks', max_size=10000)

    owning = set()
    for ch in a:
        for table in yt.get('//sys/chunks/%s/@owning_nodes' % ch):
            owning.add(str(table))

    owning = list(owning)
    owning.sort()

    for table in owning:
        print table

