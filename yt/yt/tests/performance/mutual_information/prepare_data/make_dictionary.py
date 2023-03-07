#!/usr/bin/env python

import sys
import time
from dictionary import Dictionary

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print >>sys.stderr, "Usage: ./%s <word_len> <word_count>" % sys.argv[0]
        exit(1)

    dict = Dictionary()
    dict.generate(int(sys.argv[1]), int(sys.argv[2]))
    dict.save(sys.stdout)

    #print >>sys.stderr, dict.words[:5]
    #for _ in xrange(1000):
    #    print dict.get_random_word()

