#!/usr/bin/env python

import sys
import random

from dictionary import Dictionary

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print >>sys.stderr, "Usage: ./%s <dictionary_filename> <record_count> <word_count>" % sys.argv[0]
        exit(1)

    seed = int(sys.stdin.readline())
    for line in sys.stdin:
        pass

    random.seed(seed)

    dict = Dictionary()
    dict.load(open(sys.argv[1]))
    for i in xrange(int(sys.argv[2])):
        words = [dict.get_random_word() for _ in xrange(int(sys.argv[3]))]
        sys.stdout.write("\t%s\n" % " ".join(words))

