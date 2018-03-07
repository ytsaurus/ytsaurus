#!/usr/bin/env python

import sys
import random
import math
import random
import string
from itertools import imap

from dictionary import Dictionary

# TODO: because of problems in mapreduce
#class Dictionary(object):
#    def __init__(self):
#        self.words = []
#
#    def load(self, file):
#        self.words = [line.strip() for line in file]
#
#    def save(self, file):
#        def add_eoln(str): return str + "\n"
#        file.writelines(imap(add_eoln, self.words))
#
#    def generate(self, word_len, word_count):
#        self.words = ["".join([random.choice(string.ascii_letters)
#                               for _1 in xrange(word_len)])
#                      for _2 in xrange(word_count)]
#
#    def get_random_word(self):
#        def binary_search(func, value, min, max):
#            while max - min > 1:
#                mid = (max + min) / 2
#                if func(mid) > value:
#                    max = mid
#                else:
#                    min = mid
#            return min
#
#        sum = math.log(len(self.words))
#        return self.words[binary_search(math.log, random.uniform(0.0, sum), 1, len(self.words)) - 1]
 
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
        sys.stdout.write("\t\t%s\n" % " ".join(words))

