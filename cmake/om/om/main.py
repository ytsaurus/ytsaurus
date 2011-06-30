# -*- coding: utf-8 -*-

from sys import stdout, stderr, argv, exit
from os import getenv, remove, symlink, nice
from os.path import abspath, isfile, isdir, lexists, join

from exceptions import Error, Message
from util import find_or_end, indexed_split, find_up, create_directory
from util import require_file, raise_no_package, locate_package_data
from util import gmake_present, cpu_core_count
from version import version
from cache import Cache
from configuration import Configuration
from command_line import CommandLine
from parameters import Parameters
from execution import Execution

def source_root_check(dir):
    return isfile(join(dir, 'cmake/include/global.cmake'))

def almost_initialize(args, use_reset=True):
    source_root = find_up(abspath(''), source_root_check)
    source_cache = Cache(source_root)
    cl = CommandLine(source_cache, args)
    params = Parameters(cl.project, cl.target, source_root_check)
    conf = Configuration(params.source_root, params.project, params.target)
    if source_root != params.source_root or (cl.reset and use_reset):
        source_cache = Cache(params.source_root, cl.reset)
    params.import_(cl, conf, source_cache)
    
    nice(params.priority)

    return source_cache, cl, params, conf

def usual_job():
    source_cache, cl, params, conf = almost_initialize(argv)
    if not params.project and params.run_mode != CommandLine.run_shell:
        if params.run_in_project_root:
            stderr.write('WARNING!  You did not specify any project!  ' +
                         'Running for the entire project root (%s)...\n' %
                         params.source_root)
        else:
            raise Error, 'you did not specify any project'

    if params.verbose:
        params.dump()
        print
        conf.dump('Configuration')
    if params.require_local_dot_cmake:
        require_file(join(params.source_root, 'local.cmake'))

    if not isdir(params.build_pad):
        create_directory(params.build_pad)
    if params.create_pad_links:
        link = join(params.pad_links_dir, params.build_type)
        if lexists(link):
            remove(link)
        symlink(params.build_pad, link)

    build_cache = Cache(params.build_pad, cl.reset)
    exe = Execution(params, build_cache)

    cmake_succeeded = exe.CMake(params.run_mode == CommandLine.run_shell)

    exe.export_source_projects(source_cache)
    source_cache.write()
    exe.export_build_projects(build_cache)
    build_cache.write()
    
    if cmake_succeeded:
        if params.run_mode == CommandLine.run_build:
            exit(exe.make())
        elif params.run_mode == CommandLine.run_shell:
            exit(exe.shell())
        elif params.run_mode == CommandLine.run_test:
            exit(exe.test())
        elif params.run_mode == CommandLine.run_canonize:
            exit(exe.canonize())
        else:
            raise Error, 'unexpected run mode -- %d' % params.run_mode

def complete():
    try:
        line = getenv('COMP_LINE')
        point = int(getenv('COMP_POINT'))
        completing = argv[3]
        if completing:
            line = (line[ : line.rindex(completing, 0, point)] +
                    line[find_or_end(line, ' ', point) : ])
            point -= len(completing)
        words, indexes = indexed_split(line)

        source_cache, cl, params, conf = almost_initialize(words, False)

        out = cl.start_completion(point, indexes, source_cache, params)
        if params.project and cl.can_complete_targets(point, indexes):
            targets = Execution.project_targets(params.project_dir)
            out.extend(filter(lambda x: x != params.target and
                              x not in params.free_arguments.split(),
                              targets))
        if completing:
            out = filter(lambda x: x.startswith(completing) and
                         len(x) > len(completing), out)
        if out:
            print '\n'.join(out)
    except:
        pass

def print_package_data(name):
    stdout.write(open(locate_package_data('om', version(), name)).read())
def print_completion_code():
    print_package_data('om_bash_completion.sh')
def print_default_configuration():
    print_package_data('om.conf.sample')

def generate_configuration():
    if isdir('/usr/obj'):
        print 'bsd_build = yes'
        print 'create_pad_links = yes'
        print
    if (gmake_present()):
        print 'make_program = gmake'
    cores = cpu_core_count()
    if cores:
        print 'make_arguments = -j %d' % cores
        print
        print 'test_arguments = -j %d' % cores

def print_version():
    from version import version
    print 'om: %s' % version()
    try:
        from om_configuration import version
        print 'om_configuration: %s' % version()
    except ImportError:
        print 'om_configuration: not installed'

def main():
    quick_jobs = { '--complete': complete,
                   '--print-completion-code': print_completion_code,
                   '--print-default-configuration': print_default_configuration,
                   '--generate-configuration': generate_configuration,
                   '--version': print_version }

    try:
        if len(argv) > 1 and quick_jobs.has_key(argv[1]):
            quick_jobs[argv[1]]()
        else:
            usual_job()
    except Error, error:
        if `error`:
            stderr.write('Error: %s\n' % `error`)
        exit(1)
    except Message, message:
        if `message`:
            print `message` 
    except KeyboardInterrupt:
        stderr.write('Interrupted from keyboard\n')
        exit(1)
