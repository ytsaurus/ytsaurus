#!/usr/bin/python

import sys
import datetime
import argparse

def get_time(line):
    tokens = line.split()
    try:
        return datetime.datetime.strptime(tokens[0] + " " + tokens[1], "%Y-%m-%d %H:%M:%S,%f")
    except:
        return None

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-n", "--count", help="Number of largest time intervals to show", default=10, type=int)

    args = parser.parse_args()
    n = args.count

    lines = sys.stdin.readlines()
    lines = list(map(str.strip, lines))
    lines = list(filter(lambda s: len(s) > 0, lines))

    # Some lines do not start from date; we join them to previous lines using '\n'.
    joined_lines = []
    for line in lines:
        if get_time(line) is None and len(joined_lines) > 0:
            joined_lines[-1] += "\\n" + line
        else:
            joined_lines.append(line)
    lines = joined_lines

    pairs = []
    for i in range(len(lines) - 1):
        diff = get_time(lines[i + 1]) - get_time(lines[i])
        pairs.append((diff.total_seconds(), i, lines[i], lines[i + 1]))

    pairs = sorted(pairs, reverse=True)
    for duration_ms, i, lhs_line, rhs_line in pairs[:min(len(pairs), n)]:
        print "{} sec, {}..{}".format(duration_ms, i, i + 1)
        print lhs_line
        print rhs_line
        print "---"

