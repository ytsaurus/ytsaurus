#!/usr/bin/env python

from __future__ import print_function

import sys

def output(word, value):
    if word is not None:
        sys.stdout.write("%s\t\t%d\n" % (word, value))

if __name__ == "__main__":
    current_word = None
    count = 0
    lines = []
    for line in sys.stdin:
        sys.stderr.write(line)
        lines.append(line)
    for line in lines:
        key, subkey, value = line.strip("\n\r").split("\t")
        if key != current_word:
            if subkey != "":
                print(line, file=sys.stderr)
                print("Incorrect input", file=sys.stderr)
                exit(1)
            output(current_word, count)
            current_word = key
            count = 0
        else:
            count += int(value)
    output(current_word, count)
