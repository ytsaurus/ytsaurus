# -*- coding: utf-8 -*-

from sys import stdout, argv
from os import getenv, environ
from os.path import isfile, isdir, join
from subprocess import Popen, PIPE
from errno import ENOENT

from exceptions import Error
from util import checked_remove, scat, list_intersection, relative_path
from util import is_build_directory, process_output
from conf_map import Map

class Context(object):
    def __init__(self, command=[], cwd=None, env=None):
        self.command = command
        self.cwd = cwd
        self.env = env

class Execution(object):
    @staticmethod
    def project_targets(project_dir):
        targets = [ ]
        for line in filter(lambda x: x.startswith('... '),
                           process_output('%s help' % argv[0],
                                          cwd=project_dir)):
            targets.append(line.split()[1])
        return targets

    def __init__(self, params, build_cache):
        self.p = params
        self.build_cache_exists = build_cache.exists
        self.source_projects = { }
        self.projects = set(build_cache.get('projects').split())
        self.build_projects = set(build_cache.get('build_projects').split())
        self.bad_projects = set(build_cache.get('bad_projects').split())

    """
    1. Если om для корня и есть projects, то очистить projects и позвать cmake.
    2. Если om для проекта:
        2.1 если проект есть в build_projects или в projects, то уже всё готово
            (случай, когда нет в build_projects, но есть в projects означает
            директорию, которая на самом деле не проект -- ну и пусть, не будет
            лишних вызовов cmake для bash completion);
        2.2 иначе cmake only с добавленной в projects проектом.
    3. Make всегда в директории проекта (или в корне).
    """
    """
    1. Удалить плохие проекты из projects и build_projects.
    2. Если в кэше cmake есть плохие проекты, перезапустить cmake.
    3. Если om для проекта:
        3.1 если cmake отработал нормально удалить проект из плохих;
        3.2 если с ошибкой, то добавить проект в плохие. 
    """
    def CMake(self, run_shell):
        if not self.p.project and run_shell:
            return True
 
        cache_path = join(self.p.build_pad, 'CMakeCache.txt')
        if self.p.reset:
            checked_remove(cache_path)
        cache = Map()
        cache.read_file(cache_path)
        cmake_projects = set((cache.get('MAKE_ONLY:STRING') or
                              cache.get('MAKE_ONLY:INTERNAL')).split(';'))

        run = False
        if not isfile(cache_path) or not self.build_cache_exists:
            run = True
        if cmake_projects.intersection(self.bad_projects):
            run = True
        self.projects.difference_update(self.bad_projects)
        self.build_projects.difference_update(self.bad_projects)
        if not self.p.project:
            if self.projects:
                self.projects.clear()
                run = True
        elif (self.p.project not in self.build_projects and
              self.p.project not in self.projects):
            self.projects.add(self.p.project)
            run = True
        if run_shell:
            if not isdir(self.p.build_dir):
                run = True
        elif not is_build_directory(self.p.build_dir):
            run = True

        if not run:
            return True
        silent = self.p.silent_CMake and not self.p.verbose
        if silent:
            stdout.write('Updating configuration... ')
            stdout.flush()
        succeeded = self._execute(self.CMake_context(), silent) == 0
        if silent and succeeded:
            print 'Done.'
        self.collect_projects()
        if self.p.project:
            if succeeded:
                self.bad_projects.discard(self.p.project)
            else:
                self.bad_projects.add(self.p.project)
        return succeeded
    def make(self):
        if not is_build_directory(self.p.build_dir):
            raise Error, ('nothing to build in source directory \'%s\'' %
                          self.p.project)
        return self._execute(self.make_context())
    def shell(self):
        return self._execute(Context(cwd=self.p.build_dir,
                             command=[self.p.free_arguments or getenv('SHELL')]))
    def test(self):
        context = self._command_template('test')
        context.cwd = self.p.build_dir
        self._execute(context)
    def canonize(self):
        context = self._command_template('canonize')
        context.cwd = self.p.build_dir
        self._execute(context)

    def CMake_context(self):
        context = self._command_template('CMake', False, [
            '-DMAKE_CHECK=no',
            '-DMAKE_ONLY=%s' % ';'.join(self.projects),
            '-DCMAKE_BUILD_TYPE=%s' % self.p.build_type,
        ])
        context.command.append(self.p.source_root)
        context.cwd = self.p.build_pad
        return context
    def make_context(self):
        context = self._command_template('make')
        if self.p.target:
            args = scat((self.p.make_arguments, self.p.free_arguments))
            targets = Execution.project_targets(self.p.project_dir)
            if not list_intersection(args.split(), targets):
                context.command.append(self.p.target)
        if self.p.verbose_make or self.p.verbose:
            context.command.append('VERBOSE=1')
        context.cwd = self.p.build_dir
        return context

    class command_settings(object):
        def __init__(self, map, name):
            self.map = map
            self.name = name
        def __call__(self, key):
            return self.map['%s_%s' % (self.name, key)]
    def _command_template(self, name, put_free=True, pre_arguments=[ ]):
        settings = Execution.command_settings(self.p.__dict__, name)
        context = Context()
        env = [ kv.split("=") for kv in settings("environment").split(" ") if kv != "" ]
        if env:
            context.env = dict(environ)
            context.env.update(env)
        context.command = []
        context.command.append(settings('program'))
        context.command.extend(pre_arguments)
        context.command.extend([ x for x in settings('arguments').split(" ") if x != "" ])
        if put_free:
            context.command.extend([ x for x in self.p.free_arguments.split(" ") if x != "" ])
        return context

    def _execute(self, context, silent=False):
        if not isdir(context.cwd):
            raise Error, ('build directory \'%s\' does not exist or is not a ' +
                          'directory') % context.cwd
        if self.p.verbose:
            print '\nStarting: %s\nIn: %s\n' % (" ".join(context.command), context.cwd)
        try:
            child = Popen(context.command, cwd=context.cwd, env=context.env, stdout=silent and PIPE or None)
        except OSError, e:
            if e.errno == ENOENT:
                raise Exception("command not found: %s" % " ".join(context.command))
            else:
            	raise e
        return child.wait()

    def collect_projects(self):
        projects = { }
        for line in open(join(self.p.build_pad, 'target.list')):
            record = line.split()
            projects[record[0]] = relative_path(self.p.source_root, record[1])
        self.source_projects.update(projects)
        self.build_projects = projects.values()

    def export_source_projects(self, cache):
        cache.update(self.source_projects)
    def export_build_projects(self, cache):
        cache.set('projects', scat(self.projects))
        cache.set('build_projects', scat(self.build_projects))
        cache.set('bad_projects', scat(self.bad_projects))
