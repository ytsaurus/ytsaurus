#!/usr/bin/python

import yt.wrapper as yt
import sys

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print 'Usage: set_opaque.py root_path'
        sys.exit(1)

    root = sys.argv[1]    
    for x in yt.list(root):
        if yt.get(root + '/' + x + '/@type') == 'map_node':
            print 'Setting opaque on', x
            yt.set(root + '/' + x + '/@opaque', 'true')

