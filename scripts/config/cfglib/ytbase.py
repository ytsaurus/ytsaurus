#!/usr/bin/env python
from metabase import *

import inspect
import os
import stat
import sys
from collections import namedtuple

from yt import yson

if os.name == 'nt':
    SCRIPT_EXT = 'bat'
else:
    SCRIPT_EXT = 'sh'

def make_executable(filename):
    if os.name != 'nt':
        os.chmod(filename, stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)

##################################################################

class FileDescr(object):
    def __init__(self, name, attrs=(), ext=SCRIPT_EXT, method=None):
        self.name = name
        self.attrs = attrs

        self.filename = name + '.' + ext
        if not method:
            self.method = name
        else:
            self.method = method

Config = FileDescr('config', ('remote', ), 'yson', 'makeConfig')
Run = FileDescr('run', ('aggregate', 'exec', ))
Stop = FileDescr('stop', ('aggregate', 'exec', ))
Clean = FileDescr('clean', ('aggregate', 'exec', ))

##################################################################

class AggrBase(ConfigBase):
    def _init_path(cls):
        if 'path' not in cls.__dict__:
            cls.path = os.path.join(cls.path, cls.__name__.lower())

        cls.local_dir = os.path.join(os.getcwd(), cls.path)
        #print cls, cls.local_dir
        cls.work_dir = cls.local_dir
        if not os.path.exists(cls.local_dir):
            os.makedirs(cls.local_dir)

        for descr in cls.files:
            setattr(cls, '_'.join((descr.name, 'path')),
                    cls.local_path(descr.filename))

    def local_path(cls, filename):
        return os.path.join(cls.local_dir, filename)

    @initmethod
    def init(cls):
        cls._init_path()


class Node(AggrBase):
    @propmethod
    def binary(cls):
        return os.path.split(cls.bin_path)[1]

    @propmethod
    def log_path(cls):
        return os.path.join(cls.work_dir, cls.__name__ + '.log')

    @propmethod
    def debug_log_path(cls):
        return os.path.join(cls.work_dir, cls.__name__ + '.debug.log')

    def makeConfig(cls, fd):
        yson.dump(cls.config, fd, indent='  ')

    def defaultFile(cls, fd, descr):
        raise "No file creation method for node (%s) and file %s" % (cls.path, desrc.name)

    def makeFiles(cls):
        for descr in cls.files:
            with open(cls.local_path(descr.filename), 'w') as fd:
                method = getattr(cls, descr.method, None)
                if method:
                    method(fd)
                else:
                    cls.defaultFile(fd, descr)

            if 'exec' in descr.attrs:
                make_executable(fd.name)


class ServerNode(Node):
    @propmethod
    def host(cls):
        return cls.address.split(':')[0]

    @propmethod
    def port(cls):
        return int(cls.address.split(':')[1])

##################################################################

def make_aggregate(node, runcmd, footer='', pack_size=20):
    if node.__leafs:
        node.__leafs.sort(key=lambda x:x.path)

        # make list of file descriptions
        names = set()
        for l in node.__leafs:
            for descr in l.files:
                if 'aggregate' in descr.attrs:
                    assert 'exec' in descr.attrs
                    names.add(descr.name)

        for name in names:
            with open(node.local_path(name + '.' + SCRIPT_EXT), 'w') as fd:
                leaf_count = len(node.__leafs)
                for i, l in enumerate(node.__leafs):
                    if i % pack_size == 0:
                        print >>fd, footer
                        print >>fd, 'echo "=== %.2f%% done."' % (i * 100. / leaf_count)
                    for descr in l.files:
                        if name == descr.name:
                            print >>fd, runcmd(l.local_path(descr.filename))
                if footer:
                    print >>fd, footer
                    print >>fd, 'echo "=== 100%% done."'
            make_executable(fd.name)

    for scls in node.__subclasses__():
        make_aggregate(scls, runcmd, footer, pack_size)

def make_files(root):
    # make file for each node
    def traverse_leafs(node):
        leafs = []
        subclasses = node.__subclasses__()
        if subclasses:
            for x in subclasses:
                leafs.extend(traverse_leafs(x))
            node.__leafs = leafs
        else:
            leafs = [node]
            node.makeFiles()
            node.__leafs = []

        return leafs

    traverse_leafs(root)

