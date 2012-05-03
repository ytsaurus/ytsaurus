#!/usr/bin/python
################################################################################

import glob
import os
import os.path
import sys

if sys.hexversion <= 0x2060000:
    print >>sys.stderr, 'Incompatible Python version. Python >= 2.6 required.'
    sys.exit(255)

from collections import defaultdict
from contextlib import contextmanager
import subprocess

import cmake_parsing
import cmake_writing
from ansi import Fore, Style

################################################################################
### Auxiliary functions.

# Decorates a function to cache its return value.
# Obviously, this decorator is suitable only for pure functions.
# Obviously, this decorator does not work with generating functions.
def cached(func):
    cache = {}
    def inner_func(*args, **kwargs):
        key = repr(func) + '#' + repr(args) + '#' + repr(kwargs)
        if key not in cache:
            cache[key] = func(*args, **kwargs)
        return cache[key]

    return inner_func

# Returns YT project root (where all sources reside).
@cached
def get_project_root():
    root = os.path.realpath(__file__)
    root = os.path.join(os.path.dirname(root), '..')
    root = os.path.realpath(root)
    return root

# Strips the project root from the given path.
@cached
def root_to_project(*path_tokens):
    path = os.path.join(*path_tokens)
    path = os.path.realpath(path)
    path = os.path.relpath(path, get_project_root())
    return path

# Prepends the project root to the given path.
@cached
def relative_to_absolute(*path_tokens):
    path = os.path.join(get_project_root(), *path_tokens)
    path = os.path.realpath(path)
    return path

# Checks whether the given path resides within the project tree.
@cached
def is_within_project(path):
    return '..' not in os.path.relpath(path, get_project_root())

# Checks whether the given path points to the existing file.
@cached
def is_good_file(path):
    return os.path.lexists(path) and os.path.isfile(path)

# Decorates a function to ensure that its first argument is a directory within
# the project tree.
def works_only_from_the_project_root(func):
    def inner_func(directory, *args, **kwargs):
        assert os.path.isdir(directory), '{0} is not a directory'.format(directory)
        assert is_within_project(directory), '{0} is not within the project tree'.format(directory)

        return func(directory, *args, **kwargs)

    return inner_func

################################################################################
### CMakeLists.txt discovery functions.

# Discovers all CMakeLists.txt on the path from `directory` to the project root.
@works_only_from_the_project_root
def get_lists_on_the_path_to_the_project_root(directory):
    directory = root_to_project(directory)
    while directory:
        list = relative_to_absolute(directory, 'CMakeLists.txt')
        if os.path.lexists(list) and os.path.isfile(list):
            yield list

        directory, _ = os.path.split(directory)

    list = relative_to_absolute('CMakeLists.txt')
    if is_good_file(list):
        yield list

# Discovers all CMakeLists.txt underneath `directory`.
@works_only_from_the_project_root
def get_lists_underneath(directory):
    # TODO(sandello): We can use os.listdir() and recursive calls here
    # to be more granular and hence more cache-friendly.
    for root, directores, files in os.walk(directory):
        if '.git' in root or '.svn' in root or 'CMakeLists.txt' not in files:
            continue

        list = os.path.join(root, 'CMakeLists.txt')
        list = os.path.realpath(list)

        if is_good_file(list):
            yield list


################################################################################
### Target discovery functions.

# Memoizing version of cmake_parsing.extract_targets
@cached
def _cached_extract_targets(*args, **kwargs):
    return list(cmake_parsing.extract_targets(*args, **kwargs))

# Discovers all targets defined exactly in the list within the given directory.
@works_only_from_the_project_root
def get_targets(directory):
    list = os.path.join(directory, 'CMakeLists.txt')
    list = os.path.realpath(list)

    if not is_good_file(list):
        return []

    return list(target for type, target in _cached_extract_targets(list))    

# Discovers all targets defined anywhere underneath the given directory.
@works_only_from_the_project_root
def get_targets_underneath(directory):
    targets = []
    for list in get_lists_underneath(directory):
        for type, target in _cached_extract_targets(list):
            targets.append(target)
    return targets

# Discovers all targets in the nearest CMakeLists.txt on the path from `directory`
# to the project root which defines at least one feasible target.
@works_only_from_the_project_root
def get_nearest_targets(directory):
    targets = []
    for list in get_lists_on_the_path_to_the_project_root(directory):
        for type, target in _cached_extract_targets(list):
            targets.append(target)
        if targets:
            break
    return targets


################################################################################
### (Functionality) Update CMakeLists.txt with special hash-tag.

# Filters CMakeLists.txt leaving only lists with special 'updatable' hash-tag
# (see `cmake_writing._LIST_HEADER`).
def filter_updatable_lists(iterable):
    for list in iterable:
        if cmake_writing.CMakeListsUpdater.is_updatable(list):
            yield list

# Updates CMakeLists.txt by writing up-to-date BASE, SRCS and HDRS variables
# (see `cmake_writing.CMakeLists.Updater`).
def update_list(list):
    # NB: No pre-condition checks here.
    # Everything have to be checked by the caller.
    directory = os.path.dirname(list)

    sources = glob.glob(os.path.join(directory, '*.cpp'))
    headers = glob.glob(os.path.join(directory, '*.h'))

    updater = cmake_writing.CMakeListsUpdater(directory, get_project_root())
    updater.set_sources(sources)
    updater.set_headers(headers)

    return updater.apply()


################################################################################
### (Functionality) Shell Executor

@contextmanager
def working_directory(directory = None):
    current_directory = os.getcwd()

    if directory:
        print >>sys.stderr, '## Changing directory to "{0}"...'.format(directory)
        os.chdir(directory)

    yield

    if directory:
        print >>sys.stderr, '## Reverting directory to "{0}"...'.format(current_directory)
        os.chdir(current_directory)

# TODO(sandello): This is hacky class, consider making it more robust.
# TODO(sandello): Add proper support for custom C(XX)?FLAGS.
# TODO(sandello): Add proper support for compiler selection.
class OmmExecutor(object):
    def __init__(self, path_to_config):
        self.config = defaultdict(str)

        # XXX: Set defaults.
        self.config['source_directory'] = get_project_root()

        # XXX: Load overrides.
        if is_good_file(path_to_config):
            with open(path_to_config, 'rt') as config_handle:
                for line_number, line in enumerate(config_handle):
                    try:
                        line = line.strip()

                        if line.startswith('#'):
                            continue

                        if '=' not in line:
                            raise RuntimeError, 'Cannot distinguish key and value'

                        key, value = line.split('=', 1)
                        self.config[key.strip()] = value.strip()
                    except RuntimeError, e:
                        print >>sys.stderr, 'Error on line #{0}: #{1}'.format(
                            1 + line_number,
                            str(e)
                        )

        # XXX: Assert existance.
        assert 'build_directory' in self.config
        assert 'build_type' in self.config

        # XXX: Normalize parameters.
        bd = self.config['build_directory']
        bd = relative_to_absolute(bd) if not os.path.isabs(bd) else os.path.realpath(bd)
        self.config['build_directory'] = bd

        print >>sys.stderr, '## Source directory: {0}'.format(self.config['source_directory'])
        print >>sys.stderr, '## Build directory: {0}'.format(self.config['build_directory'])

    def run(self, *args):
        print '-> {cmd}'.format(cmd = ' '.join(args))
        subprocess.check_call(*args, shell = True)

    def run_cmake(self):
        print >>sys.stderr, '## Running CMake...'
        with working_directory(self.config['build_directory']):
            self.run('cmake -DCMAKE_BUILD_TYPE={build_type} {source_directory}'.format(
                **self.config.items()
            ))

    def run_make(self, targets):
        print >>sys.stderr, '## Running make...'
        with working_directory(self.config['build_directory']):
            self.run('make {make_arguments} {targets}'.format(
                make_arguments = self.config['make_arguments'],
                targets = ' '.join(targets)
            ))

################################################################################
################################################################################

if __name__ == '__main__':
    # Expands all arguments in omm.py-style.
    # The main goal of this method is to deduce a list of CMakeLists.txt from
    # command line arguments. Behaviour is as follows:
    #   * if there are no arguments, then return all CMakeLists.txt
    #     in the project,
    #   * if the argument is a directory, then this argument expands to all
    #     lists underneath this directory,
    #   * if the argument is a file, then this argument is left untouched.
    def expand_args_to_lists(args):
        if not args:
            args = [ get_project_root() ]

        lists = []
        for arg in args:
            if os.path.isdir(arg):
                lists.extend(get_lists_underneath(arg))
                continue
            if os.path.isfile(arg):
                lists.append(os.path.realpath(arg))
                continue
            raise RuntimeError, 'Cannot interpret "{0}" as list specification: neither directory nor file'.format(arg)
        return lists

    # Expands all arguments in omm.py-style.
    # The main goal of this method is to deduce a list of targets from
    # command line arguments. Behaviour is as follows:
    #   * if there are no arguments, then return all targets in the project,
    #   * if the argument is a directory, then this argument expands to all
    #     nearest targets to this directory,
    #   * if the argument is a target, then this argument is left untouched.
    def expand_args_to_targets(args):
        feasible_targets = set(get_targets_underneath(get_project_root()))

        if not args:
            args = [ get_project_root() ]

        targets = []
        for arg in args:
            if os.path.isdir(arg):
                targets.extend(get_nearest_targets(arg))
                continue
            if arg in feasible_targets:
                targets.append(arg)
                continue
            raise RuntimeError, 'Cannot interpret "{0}" as target specification: neither directory nor feasible target'.format(arg)
        return targets if targets else [ 'all' ]

    # Validates that CMakeLists.txt are parsed by our grammar.
    def do_lint(args):
        for list in expand_args_to_lists(args):
            try:
                cmake_parsing.parse(list)
                print '  OKAY', list
            except Exception, e:
                print 'FAILED', list

    # Prints the parse tree for CMakeLists.txt by our grammar.
    def do_print(args):
        for list in expand_args_to_lists(args):
            print '* Processing {0}...'.format(list)
            for command, arguments in cmake_parsing.parse_to_iterable(list):
                print command
                for argument in arguments:
                    print ' ' * 2, argument

    # Updates updatable CMakeLists.txt.
    def do_update(args):
        for list in filter_updatable_lists(expand_args_to_lists(args)):
            print '* Processing {0}...'.format(list)
            if update_list(list):
                print Style.BRIGHT + Fore.RED + 'REWRITTEN' + Style.RESET_ALL, list
            else:
                print Style.BRIGHT + Fore.GREEN + 'UNTOUCHED' + Style.RESET_ALL, list

    def do_discover_lists(args):
        for list in expand_args_to_lists(args):
            print list

    def do_discover_targets(args):
        for target in expand_args_to_targets(args):
            print target

    def do_build(args):
        if not args:
            args = [ os.getcwd() ]
        
        targets = list(expand_args_to_targets(args))
        executor = OmmExecutor(relative_to_absolute('.omm.config'))

        #executor.run_cmake()
        executor.run_make(targets)
        #asdf

    from optparse import OptionParser, OptionGroup
    parser = OptionParser(usage = '%prog [mode] args...')

    dev_modes = OptionGroup(parser, 'Various operation modes for CMake wannabes')
    dev_modes.add_option('--lint',
        action = 'store_const', dest = 'callback', const = do_lint)
    dev_modes.add_option('--print',
        action = 'store_const', dest = 'callback', const = do_print)
    dev_modes.add_option('--update',
        action = 'store_const', dest = 'callback', const = do_update)
    dev_modes.add_option('--discover_lists',
        action = 'store_const', dest = 'callback', const = do_discover_lists)
    dev_modes.add_option('--discover_targets',
        action = 'store_const', dest = 'callback', const = do_discover_targets)

    op_modes = OptionGroup(parser, 'Various operation modes for mere mortals')
    op_modes.add_option('--build',
        action = 'store_const', dest = 'callback', const = do_build)

    parser.add_option_group(dev_modes)
    parser.add_option_group(op_modes)
    (options, args) = parser.parse_args()

    if options.callback is None:
        parser.error('Please, specify working mode.')

    (options.callback)(args)
