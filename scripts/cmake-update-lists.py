#!/usr/bin/python
# Requires python 2.6+


import os
import os.path
import re
import sys
import tempfile
import hashlib
import glob


_LIST_HEADER   = '#:yt-updatable'


_RE_BASE       = re.compile(r'^set\( BASE .*\)$')
_RE_SRCS_BEGIN = re.compile(r'^set\( SRCS$')
_RE_SRCS_END   = re.compile(r'^\)$')
_RE_HDRS_BEGIN = re.compile(r'^set\( HDRS$')
_RE_HDRS_END   = re.compile(r'^\)$')


def compute_file_hash(path):
    md5 = hashlib.md5()
    with open(path, 'rb') as handle:
        for chunk in iter(lambda: handle.read(8192), ''): 
            md5.update(chunk)
    return md5.digest()


def get_project_root():
    root = os.path.realpath(__file__)
    root = os.path.join(os.path.dirname(root), '..')
    root = os.path.realpath(root)
    return root


def strip_project_root(path):
    return os.path.relpath(path, get_project_root())


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


class CMakeListsHandler(object):
    def __init__(self, directory):
        self.working_directory = os.path.realpath(directory)
        
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
            self.working_directory.replace(get_project_root(), '${CMAKE_SOURCE_DIR}')
        )

    def write_new_srcs(self, sink):
        for source in self.sources:
            source = os.path.join('${BASE}', source)
            print >>sink, '  {0}'.format(source)

    def write_new_hdrs(self, sink):
        for header in self.headers:
            header = os.path.join('${BASE}', header)
            print >>sink, '  {0}'.format(header)


def update_list_in_directory(directory):
    sources = glob.glob(os.path.join(directory, '*.cpp'))
    headers = glob.glob(os.path.join(directory, '*.h'))

    handler = CMakeListsHandler(directory)
    handler.set_sources(sources)
    handler.set_headers(headers)

    if handler.apply():
        print >>sys.stderr, '  * Updated'
    else:
        print >>sys.stderr, '  * Untouched'


def find_directories_with_feasible_lists(root_directory):
    for root, directores, files in os.walk(root_directory):
        if '.git' in root or '.svn' in root or 'CMakeLists.txt' not in files:
            continue
        
        with open(os.path.join(root, 'CMakeLists.txt'), 'rt') as candidate_list:
            candidate_header = candidate_list.readline().strip()
            if candidate_header != _LIST_HEADER:
                continue

        yield root


def main():
    for directory in find_directories_with_feasible_lists(get_project_root()):
        print >>sys.stderr, 'Processing {0}...'.format(strip_project_root(directory))
        update_list_in_directory(directory)


if __name__ == "__main__":
    main()
