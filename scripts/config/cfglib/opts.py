#!/usr/bin/python
#!-*-coding:utf-8-*-
import sys

def limit_iter(name, iterable):
    try:
        argv = sys.argv[1:]
        i = argv.index(name)
        count = int(argv[i + 1])
        return [x for i, x in enumerate(iterable) if i < count]
    except:
        return [x for x in iterable]

def get_string(name, default=''):
    try:
        argv = sys.argv[1:]
        i = argv.index(name)
        return argv[i + 1]
    except:
        return default
    
def choose(options):
    argv = sys.argv[1:]
    for name, value in options:
        if name in argv:
            return value
        
    return options[0][1]

