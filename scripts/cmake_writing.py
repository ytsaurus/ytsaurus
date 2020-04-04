#!/usr/bin/python
################################################################################

import hashlib
import os
import os.path
import re
import sys
import tempfile

if sys.hexversion <= 0x2060000:
    print >>sys.stderr, 'Incompatible Python version. Python >= 2.6 required.'
    sys.exit(255)

################################################################################
### Auxiliary constants and functions.

_LIST_HEADER   = '#:yt-updatable'

_RE_BASE       = re.compile(r'^set\( BASE .*\)$')
_RE_SRCS_BEGIN = re.compile(r'^set\( SRCS$')
_RE_SRCS_END   = re.compile(r'^\)$')
_RE_HDRS_BEGIN = re.compile(r'^set\( HDRS$')
_RE_HDRS_END   = re.compile(r'^\)$')

def compute_file_hash(path):
    md5 = hashlib.md5()
    with open(path, 'rb') as handle:
        for chunk in iter(lambda: handle.read(64 * 1024), ''): 
            md5.update(chunk)
    return md5.digest()


################################################################################
### Abstract rewriter.

class Rewriter(object):
    def __init__(self, iterable, sink):
        self.current = None
        self.iterator = iter(iterable)
        self.sink = sink

    def next(self):
        self.current = self.iterator.next()

    def passthrough(self):
        self.sink.write(self.current)
        self.next()
    
    def try_to_rewrite(self, regexp, callback):
        if regexp.match(self.current):
            callback(self.sink)
            self.next()

    def try_to_rewrite_between(self, re_begin, re_end, callback):
        if re_begin.match(self.current):
            self.passthrough()

            callback(self.sink)
            while not re_end.match(self.current):
                self.next()

            self.passthrough()


################################################################################
### A rewriter which updates CMakeLists.txt with up-to-date list of source
### and header files.

class CMakeListsUpdater(object):
    @staticmethod
    def is_updatable(list):
        with open(list, 'rt') as list_handle:
            header = list_handle.readline().strip()
            return header == _LIST_HEADER

    def __init__(self, working_directory, project_directory):
        self.working_directory = os.path.realpath(working_directory)
        self.project_directory = os.path.realpath(project_directory)
        
    def _make_relative(self, files):
        files = [ os.path.realpath(file) for file in files ]
        files = [ os.path.relpath(file, self.working_directory) for file in files ]
        return sorted(files)

    def set_headers(self, headers):
        self.headers = self._make_relative(headers)

    def set_sources(self, sources):
        self.sources = self._make_relative(sources)

    def apply(self):
        input_list = open(os.path.join(self.working_directory, 'CMakeLists.txt'), 'rt')
        output_list = tempfile.NamedTemporaryFile(dir = self.working_directory, prefix = '.', delete = False)

        try:
            r = Rewriter(input_list, output_list)
            r.next()

            while True:
                r.try_to_rewrite(_RE_BASE, self.write_new_base)
                r.try_to_rewrite_between(_RE_SRCS_BEGIN, _RE_SRCS_END, self.write_new_srcs)
                r.try_to_rewrite_between(_RE_HDRS_BEGIN, _RE_HDRS_END, self.write_new_hdrs)

                r.passthrough()
        except StopIteration:
            pass

        input_list.close()
        output_list.close()

        if compute_file_hash(input_list.name) != compute_file_hash(output_list.name):
            os.rename(input_list.name, input_list.name + '.orig')
            os.rename(output_list.name, input_list.name)
            return True
        else:
            os.remove(output_list.name)
            return False

    def write_new_base(self, sink):
        print >>sink, 'set( BASE {0} )'.format(
            self.working_directory.replace(self.project_directory, '${CMAKE_SOURCE_DIR}')
        )

    def write_new_srcs(self, sink):
        for source in self.sources:
            source = os.path.join('${BASE}', source)
            print >>sink, '  {0}'.format(source)

    def write_new_hdrs(self, sink):
        for header in self.headers:
            header = os.path.join('${BASE}', header)
            print >>sink, '  {0}'.format(header)


################################################################################
################################################################################

if __name__ == '__main__':
    pass
