#!/usr/bin/env python

import sys

if __name__ == "__main__":
    for line in sys.stdin:
        data = dict(field.split("=", 1) for field in line.strip().split("\t"))
        #print data
        source_uri = data["source_uri"]
        del data["source_uri"]
        iso_eventtime = data["iso_eventtime"]
        del data["iso_eventtime"]
        print "%s %s\t%s" % (source_uri, iso_eventtime, "\t".join("=".join([k, v]) for k, v in data.iteritems())) 

