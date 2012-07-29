import sys
sys.path.append("./")

from yt_streaming import *

def mapper(row):
	line = row['line']
	words = line.strip().split()
	for word in words:
		yield {'word': word, 'count': 1}

run_map(mapper)

