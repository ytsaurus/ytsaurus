#!/usr/bin/python

import re
import sys
import operator
import itertools

def do_decode(iterable):
    for line in iterable:
        row = line.rstrip("\r\n").split("\t")
        row = dict(map(lambda x: x.split("=", 1), row))
        yield row

def do_encode(iterable):
    for row in iterable:
        yield "\t".join("{0}={1}".format(*item) for item in row.items())

def do_map(iterable):
    iterable = map(operator.itemgetter("text"), iterable)
    iterable = map(lambda x: x.lower(), iterable)
    for line in iterable:
        for word in re.findall(r"\w+", line, re.U):
            yield { "word" : word, "count" : 1 }

def do_reduce(iterable):
    for key, group in itertools.groupby(iterable, operator.itemgetter("word")):
        group = map(operator.itemgetter("count"), group)
        group = map(lambda x: int(x), group)
        yield { "word" : key, "count" : sum(group) }

def do_print(iterable):
    for line in wf:
        sys.stdout.write(line)
        sys.stdout.write("\n")

if __name__ == "__main__":
    wf = sys.stdin
    wf = do_decode(wf)

    if len(sys.argv) >= 2 and sys.argv[1] == "map":
        wf = do_map(wf)
    elif len(sys.argv) >= 2 and sys.argv[1] == "reduce":
        wf = do_reduce(wf)
    else:
        print >>sys.stderr, "Please, specify either 'map' or 'reduce' as the first argument"
        sys.exit(1)

    wf = do_encode(wf)
    wf = do_print(wf)
