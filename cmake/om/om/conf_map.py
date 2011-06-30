# -*- coding: utf-8 -*-

from os.path import isfile

from conf_file import File

class Map(object):
    def __init__(self):
        self.map = { }
    def map(self):
        return self.map
    def get(self, key, default=''):
        return self.map.get(key, default)
    def read_file(self, path, base_dir=None):
        if not path or not isfile(path):
            return True
        return File(path, base_dir).read(self.map)
    def dump(self, name):
        print '%s variables:' % name, self.map
