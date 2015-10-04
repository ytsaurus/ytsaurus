#!/usr/bin/python
# encoding: utf-8

import sys
import os

if __name__ == "__main__":
	all_chunks = {}
	for cache in sys.argv[1:]:
		print "Scanning chunk cache %s" % cache
		subdirs = [os.path.join(cache, d) for d in os.listdir(cache) if os.path.isdir(os.path.join(cache, d))]
		for subdir in subdirs:
			for chunk in [c for c in os.listdir(subdir) if not c.endswith(u".meta")]:
				p = os.path.join(subdir, chunk)
				if all_chunks.has_key(chunk):
					all_chunks[chunk].append(p)
				else:
					all_chunks[chunk] = [p]

	for c in all_chunks.itervalues():
		if len(c) > 1:
			print "rm -rf %s" % (" ".join(["%s %s.meta" % (x, x) for x in c[1:]]))

