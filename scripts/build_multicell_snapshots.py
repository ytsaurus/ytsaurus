#!/usr/bin/python

import yt.yson as y
import subprocess as sp;
import time

f = open('driver.conf', 'r');
c = y.load(f)['driver'];
cells = [c['primary_master']['cell_id']];
cells += [m['cell_id'] for m in c['secondary_masters']]; 

def snapshot(cell):
    print "Building snapshot for", c
    return sp.Popen(['yt-admin', 'build-snapshot', '--set-read-only', '--cell-id', c])

s = [[c, snapshot(c)] for c in cells];

while True:
    time.sleep(60)
    for ss in s:
        if ss[1].poll() is None:
            continue
        elif ss[1].poll() != 0:
            ss[1] = snapshot(ss[0])
	    continue
    ok = all([ss[1].poll() == 0 for ss in s])
    if ok: break


