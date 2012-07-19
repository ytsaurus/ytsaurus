import sys
sys.path.append("./")

from yt_streaming import *

def reducer(word, rows):
	count = sum(int(row['count']) for row in rows)
	yield {'word': word, 'count': count}

run_reduce(reducer, 'word')

