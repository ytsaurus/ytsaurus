# -*- coding: utf-8 -*-

from os.path import basename

from exceptions import Error, Message
from util import scat, check_for_project

class CommandLine(object):
    (run_build, run_shell, run_test, run_canonize) = range(4)

    @staticmethod
    def usage_string(command):
        return ('usage: %s options [<make arguments>]\n' +
                '       %s options shell [<command>]\n' +
                '       %s options test [<test arguments>]\n' +
                '       %s options canonize [<canonize arguments>]\n' +
                '\n' +
                '       %s --print-completion-code\n' +
                '       %s --print-default-configuration\n' +
                '       %s --generate-configuration\n' +
                '       %s --version\n' +
                '\n' +
                'options: [reset] [verbose] [<project>|<target>] ' +
                    '[<build type>]\n' +
                '\n' +
                'documentation: https://wiki.yandex-team.ru/Poiskovaja' +
                    'Platforma/Build/Unix') % tuple([ command ] * 8)
    def run_mode_string(self):
        return [ 'build', 'shell', 'test', 'canonize' ][self.run_mode]

    def __init__(self, source_cache, argv):
        if len(argv) > 1 and argv[1] in [ '-h', '--help' ]:
            raise Message, CommandLine.usage_string(basename(argv[0]))

        self.argv = argv
        self.run_mode = CommandLine.run_build
        self.name, self.project, self.target, self.build_type = [ '' ] * 4
        self.reset, self.verbose = [ False ] * 2

        for i in range(min(len(self.argv), len(self.__dict__.keys()))):
            arg = self.argv[i]
            if i == 0:
                self.name = arg
            elif self.run_mode != CommandLine.run_shell and arg == 'shell':
                self.run_mode = CommandLine.run_shell
            elif self.run_mode != CommandLine.run_test and arg == 'test':
                self.run_mode = CommandLine.run_test
            elif (self.run_mode != CommandLine.run_canonize and
                  arg == 'canonize'):
                self.run_mode = CommandLine.run_canonize
            elif not self.reset and arg == 'reset':
                self.reset = True
            elif not self.verbose and arg == 'verbose':
                self.verbose = True
            elif not self.project and check_for_project(arg, True):
                self.project = arg
            elif check_for_project(source_cache.get(arg), False):
                self.project = source_cache.get(arg)
                self.target = arg
            elif not self.build_type:
                self.build_type = arg
                self.build_type_pos = i
            else:
                i -= 1
                break
        self.free_args_pos = i + 1
    def verify_build_type(self, build_type_list):
        if self.build_type and self.build_type not in build_type_list:
            if self.build_type_pos == self.free_args_pos - 1:
                self.build_type = ''
                self.free_args_pos -= 1
            else:
                raise Error, ('build type \'%s\' is not listed in known ' +
                              'build types') % self.build_type
        self.free_arguments = scat(self.argv[self.free_args_pos : ])

    def can_complete_parameters(self, point, indexes):
        return not self.free_arguments or point < indexes[self.free_args_pos][0]
    def can_complete_targets(self, point, indexes):
        return (self.run_mode == CommandLine.run_build and
                point > indexes[self.free_args_pos - 1][1]) 
    def start_completion(self, point, indexes, source_cache, params):
        out = [ ]
        if self.can_complete_parameters(point, indexes):
            if self.run_mode == CommandLine.run_build:
                out.extend([ 'shell', 'test', 'canonize' ])
            if not self.reset:
                out.append('reset')
            if not self.verbose:
                out.append('verbose')
            if not params.project:
                out.extend(source_cache.map.keys())
            if not self.build_type:
                out.extend(params.build_type_list)
        return out
