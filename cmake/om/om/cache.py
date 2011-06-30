# -*- coding: utf-8 -*-

from os.path import join, isfile

from util import checked_remove
from conf_map import Map

class Cache(Map):
    def __init__(self, dir, reset=False):
        Map.__init__(self)

        self.path = join(dir, '.om.cache')
        if reset:
            checked_remove(self.path)

        self.exists = isfile(self.path)
        self.read_file(self.path)
        self.dirty = False
    def set(self, key, value):
        if not self.map.has_key(key) or self.map[key] != value:
            self.map[key] = value
            self.dirty = True
    def remove(self, key):
        if self.map.has_key(key):
            del self.map[key]
            self.dirty = True
    def update(self, map):
        for item in map.iteritems():
            self.set(item[0], item[1])
    def write(self):
        if not self.dirty:
            return
        if len(self.map) != 0:
            file = open(self.path, 'w')
            for key, value in self.map.iteritems():
                if value:
                    file.write('%s = %s\n' % (key, value))
            self.exists = True
        else:
            checked_remove(self.path)
            self.exists = False
        self.dirty = False
