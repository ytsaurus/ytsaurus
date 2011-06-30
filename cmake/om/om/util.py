# -*- coding: utf-8 -*-

from os import makedirs, remove
from os.path import dirname, lexists, isfile, isdir, islink, join
from subprocess import Popen, PIPE
from platform import system
from re import compile

from exceptions import Error

def scat(seq):
    return ' '.join(seq).strip()

def map_scat(map, key, value):
    map[key] = scat((map.get(key, ''), value))

def find_or_end(string, sub, start=0, end=None):
    if end == None:
        end = len(string)
    pos = string.find(sub, start, end)
    if pos == -1:
        pos = end
    return pos

def indexed_split(string, sep=' '):
    last, parts, indexes = 0, [ ], [ ]
    while True:
        pos = string.find(sep, last)
        if pos == -1:
            break
        if pos != last:
            parts.append(string[last : pos])
            indexes.append((last, last + len(parts[-1])))
        last = pos + len(sep)
    if last < len(string):
        parts.append(string[last : ])
        indexes.append((last, last + len(parts[-1])))
    return parts, indexes

def list_intersection(*lists):
    if not lists:
        return [ ]
    s = set(lists[0])
    for l in lists[1 : ]:
        s.intersection_update(set(l))
    return list(s)

def flag_value(string):
    return string.lower() in [ 'yes', 'on', '1' ]

def check_relative(root, path):
    if not path.startswith(root):
        raise ValueError, '\'%s\' does not start with \'%s\'' % (path, root)

def relative_path(root, path):
    if not path.startswith(root):
        return path
    #Util.check_relative(root, path)
    return path.replace(root, '', 1).lstrip('/')

def traverse_up(root, path):
    check_relative(root, path)
    while True:
        yield path
        if path in [ '/', root ]:
            break
        path = dirname(path)

def find_up(dir, predicate):
    while True:
        if predicate(dir):
            return dir
        if dir == '/':
            return ''
        dir = dirname(dir)

def create_directory(path):
    if lexists(path):
        raise Error, ('cannot create directory \'%s\', this is an ' +
                      'existing file') % path
    makedirs(path)

def checked_remove(path):
    if lexists(path):
        remove(path)

def check_for_project(path, strict):
    # если strict=True, то проектом не может быть симлинк из одного
    # компонента пути
    return isdir(path) and (not strict or dirname(path) or not islink(path))

def is_build_directory(directory):
    return isfile(join(directory, 'Makefile'))

def require_file(path):
    if not isfile(path):
        raise Error, 'required file \'%s\' does not exist' % path

def raise_no_package(package, version=None):
    if version != None:
        sub = 'the %s version of package \'%s\'' % (version, package)
    else:
        sub = 'package \'%s\'' % package
    raise Error, 'function is not available because %s is not installed' % sub 

def locate_package_data(package, version, filename):
    try:
        from pkg_resources import Requirement, DistributionNotFound
        from pkg_resources import resource_filename
    except ImportError:
        raise_no_package('setuptools')
    try:
        path = resource_filename(Requirement.parse('%s==%s' % (package,
                                                               version)),
                                 '%s/data/%s' % (package, filename))
    except DistributionNotFound:
        raise_no_package(package, version)
    if not isfile(path):
        raise KeyError, 'package data file \'%s\' does not exist' % path
    return path

def gmake_present():
    return Popen('gmake -v', shell=True, stdout=PIPE, stderr=PIPE).wait() == 0

def process_output(command, **kw):
    if not kw.has_key('shell'):
        kw['shell'] = True
    kw['stdout'] = PIPE
    if not kw.has_key('stderr'):
        kw['stderr'] = PIPE
    return Popen(command, **kw).stdout

def cpu_core_count():
    if system() == 'Linux':
        pattern = compile(r'^processor\s*:\s\d+$')
        return len(filter(lambda x: pattern.match(x),
                          process_output('cat /proc/cpuinfo')))
    elif system() == 'FreeBSD':
        try:
            return int([ x for x in process_output('sysctl -n hw.ncpu') ][0])
        except IndexError:
            pass
    return None
