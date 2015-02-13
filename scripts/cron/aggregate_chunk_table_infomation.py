#!/usr/bin/env python

import sys
from collections import defaultdict

info = defaultdict(lambda: 0)
children = defaultdict(lambda: set())

def walk(root, limit):
    if info[root] < limit:
        return
    print root, info[root]
    for child in sorted(children[root]):
        walk(child, limit)

def main():
    for line in sys.stdin:
        table, count = line.split()
        count = int(count)
        child = None
        while True:
            info[table] += count
            if child is not None:
                children[table].add(child)

            if table == "/":
                break

            child = table
            table = table[:table.rfind("/")]

    walk("/", 500000)


if __name__ == "__main__":
    main()
