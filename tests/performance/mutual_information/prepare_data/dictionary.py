import math
import random
import string
from itertools import imap

class Dictionary(object):
    def __init__(self):
        self.words = []

    def load(self, file):
        self.words = [line.strip() for line in file]

    def save(self, file):
        def add_eoln(str): return str + "\n"
        file.writelines(imap(add_eoln, self.words))

    def generate(self, word_len, word_count):
        self.words = ["".join([random.choice(string.ascii_letters)
                               for _1 in xrange(word_len)])
                      for _2 in xrange(word_count)]

    def get_random_word(self):
        #def binary_search(func, value, min, max):
        #    while max - min > 1:
        #        mid = (max + min) / 2
        #        if func(mid) > value:
        #            max = mid
        #        else:
        #            min = mid
        #    return min

        #sum = math.log(len(self.words))
        #return self.words[binary_search(math.log, random.uniform(0.0, sum), 1, len(self.words)) - 1]

        sum = math.log(len(self.words))
        return self.words[int(math.exp(random.uniform(0.0, sum))) - 1]

