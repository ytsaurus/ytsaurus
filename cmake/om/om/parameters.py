# -*- coding: utf-8 -*-

from os.path import abspath, normpath, join

from exceptions import Error
from util import find_up, relative_path, flag_value

class Parameters(object):
    def __init__(self, project, target, root_check):
        self.project_dir = abspath(project)
        self.source_root = find_up(self.project_dir, root_check)
        if not self.source_root:
            raise Error, ('could not find source root directory in \'%s\' ' +
                          'and it\'s parents') % self.project_dir
        self.project = relative_path(self.source_root, self.project_dir)
        self.target = target

    def import_(self, cl, conf, source_cache):
        self.build_root = abspath(conf.get('build_root',
                                           join(self.source_root, '../build')))
        self.build_type_list = conf.get('build_types',
                                        'debug release profile valgrind coverage').split()
        self.default_build_type = conf.get('default_build_type', 'debug')
        if self.default_build_type not in self.build_type_list:
            raise Error, ('default build type \'%s\' is not listed in known ' +
                          'build types') % self.default_build_type
        self.bsd_build = flag_value(conf.get('bsd_build'))
        self.bsd_objdir = conf.get('bsd_objdir', '/usr/obj')
        self.create_pad_links = flag_value(conf.get('create_pad_links'))
        self.pad_links_dir = abspath(conf.get('pad_links_dir',
                                              self.source_root))
        self.silent_CMake = flag_value(conf.get('silent_CMake', 'yes'))
        self.verbose_make = flag_value(conf.get('verbose_make'))
        self.run_in_project_root = flag_value(conf.get('run_in_project_root'))
        self.require_local_dot_cmake = flag_value(conf.get(
                                                     'require_local_dot_cmake'))
        self.priority = int(conf.get('priority', '15'))
        self.CMake_program = conf.get('CMake_program', 'cmake')
        self.CMake_environment = conf.get('CMake_environment')
        self.CMake_arguments = conf.get('CMake_arguments')
        self.make_program = conf.get('make_program', 'make')
        self.make_environment = conf.get('make_environment')
        self.make_arguments = conf.get('make_arguments')
        self.test_program = conf.get('test_program',
                                    join(self.source_root, 'check/run_test.py'))
        self.test_environment = conf.get('test_environment')
        self.test_arguments = conf.get('test_arguments')
        self.canonize_program = conf.get('canonize_program',
                                       join(self.source_root, 'check/canon.py'))
        self.canonize_environment = conf.get('canonize_environment')
        self.canonize_arguments = conf.get('canonize_arguments')

        cl.verify_build_type(self.build_type_list)
        self.run_mode = cl.run_mode
        self.run_mode_string = cl.run_mode_string()
        self.reset = cl.reset
        self.verbose = cl.verbose
        self.build_type = cl.build_type or self.default_build_type
        self.free_arguments = cl.free_arguments

        if self.bsd_build:
            self.build_root = join(self.bsd_objdir, self.source_root[1:])
        self.build_root = abspath(self.build_root)
        self.build_pad = join(self.build_root, self.build_type)
        self.build_dir = normpath(join(self.build_pad, self.project))

    def dump(self):
        print 'Run mode: %s' % self.run_mode_string
        print 'Project: %s' % self.project
        print 'Target: %s' % self.target
        print 'Build type: %s' % self.build_type
        print 'Reset: %s' % self.reset
        print 'Verbose: %s' % self.verbose
        print 'Free arguments: %s' % self.free_arguments
        print
        print 'Current directory: %s' % abspath('.')
        print 'Source root: %s' % self.source_root
        print 'Build root: %s' % self.build_root
        print 'Build pad: %s' % self.build_pad
        print 'Project source directory: %s' % self.project_dir
        print 'Project build directory: %s' % self.build_dir
        print
        print 'Build types: %s' % self.build_type_list
        print 'Default build type: %s' % self.default_build_type
        print 'BSD-like build: %s' % self.bsd_build
        print 'BSD object files directory: %s' % self.bsd_objdir
        print 'Create pad symlinks: %s' % self.create_pad_links
        print 'Pad symlinks directory: %s' % self.pad_links_dir
        print 'Hide CMake output: %s' % self.silent_CMake
        print 'Verbose make output: %s' % self.verbose_make
        print 'Run in project root: %s' % self.run_in_project_root
        print 'Require local.cmake: %s' % self.require_local_dot_cmake
        print 'priority: %s' % self.priority
        print 'CMake program: %s' % self.CMake_program
        print 'CMake environment: %s' % self.CMake_environment
        print 'CMake arguments: %s' % self.CMake_arguments
        print 'Make program: %s' % self.make_program
        print 'Make environment: %s' % self.make_environment
        print 'Make arguments: %s' % self.make_arguments
        print 'Test program: %s' % self.test_program
        print 'Test environment: %s' % self.test_environment
        print 'Test arguments: %s' % self.test_arguments
        print 'Canonize program: %s' % self.canonize_program
        print 'Canonize environment: %s' % self.canonize_environment
        print 'Canonize arguments: %s' % self.canonize_arguments
