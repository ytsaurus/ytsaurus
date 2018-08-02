import yt.wrapper as yt

import sys

count = 0
for x in xrange(0, 256):
	path = "//sys/operations/" + ("0" + hex(x)[2:] if x < 16 else hex(x)[2:])
	print path
	for y in yt.list(path, attributes=["lock_count"]):
		if y.attributes["lock_count"] > 0:
			count += 1

print "Found", count, "locked operation nodes"