

import sys
import yt.wrapper as y
from collections import defaultdict

if len(sys.argv) < 2:
	print 'Not enough parameters'
	print 'USAGE: [this-program] path'
	exit(0)

home = sys.argv[1]

tables = []
attrs = ["uncompressed_data_size", "compressed_size"]
tables = y.search(home, node_type="table", attributes=attrs)

result = defaultdict(int)
for t in tables:
	#print t
	for k in attrs:
		result[k] += int(t.attributes[k])

for k, v in result.items():
	print "{0} = {1} Tb".format(k, round(float(v) / 2**40, 4))


