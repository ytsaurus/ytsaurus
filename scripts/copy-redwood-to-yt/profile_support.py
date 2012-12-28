# -*- encoding: utf-8 -*-

import sys
from contextlib import contextmanager

@contextmanager
def profiler(args, label):
    if 'profile' not in args:
        yield
    else:
        # profiler output goes to:
        #  1. stderr if -o is absent completely
        #  2. {funcname}.prof if -o is specified without an argument
        #  3. a file specified by argument of -o
        outputfile = None
        if 'output_file' in args:
            outputfile = args.output_file
            if outputfile == '{stab}':
                outputfile = '%s.prof' % (label)

        if args.profile == 'cprofile':
            import cProfile
            import pstats
            p = cProfile.Profile()
            try:
                try:
                    p.enable()
                    yield
                except SystemExit:
                    pass
            finally:
                p.disable()
                # first clause prints text result
                # second clause dumps stats in binary form for later examination
                # this discrepancy is intentional
                if outputfile is None:
                    print >>sys.stderr, '%s stats:\n' % (label)
                    pstats.Stats(p, stream=sys.stderr).strip_dirs().sort_stats('time').print_stats(10)
                else:
                    p.dump_stats(outputfile)

        elif args.profile == 'statprof':
            import statprof
            try:
                statprof.start()
                yield
            finally:
                statprof.stop()
                if outputfile is None:
                    print >>sys.stderr, '%s stats:\n' % (label)
                    statprof.display(sys.stderr)
                else:
                    with open(outputfile, 'wb') as f:
                        statprof.display(f)

def _create_profile_args_parser():
    import argparse
    parser = argparse.ArgumentParser(add_help=False)
    group = parser.add_argument_group('Profile options')
    group.add_argument('-p', '--profile', help='turn on profiler',
            choices=['cprofile', 'statprof'],
            nargs='?', default=argparse.SUPPRESS, const='cprofile',
            )
    parser.add_argument('-o', '--output-file', help='profiler output file',
            nargs='?', default=argparse.SUPPRESS, const='{stab}',
            )
    return parser

argparser = _create_profile_args_parser()
