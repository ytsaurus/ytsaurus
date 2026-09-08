"""
..
  PYTEST_DONT_REWRITE
"""

import argparse
from functools import partial

from _pytest import pathlib
from _pytest._io import TerminalWriter
from _pytest.config import Config
from _pytest.config.findpaths import locate_config

from pytest_benchmark.csv import CSVResults

from . import plugin
from .logger import Logger
from .plugin import add_csv_options
from .plugin import add_display_options
from .plugin import add_global_options
from .plugin import add_histogram_options
from .table import CompareBetweenResults
from .table import TableResults
from .utils import DEFAULT_COLUMNS
from .utils import NAME_FORMATTERS
from .utils import first_or_value
from .utils import load_storage
from .utils import parse_columns
from .utils import report_noprogress

COMPARE_HELP = """examples:

    pytest-benchmark {0} 'Linux-CPython-3.5-64bit/*'

        Loads all benchmarks ran with that interpreter. Note the special quoting that disables your shell's glob
        expansion.

    pytest-benchmark {0} 0001

        Loads first run from all the interpreters.

    pytest-benchmark {0} /foo/bar/0001_abc.json /lorem/ipsum/0001_sir_dolor.json

        Loads runs from exactly those files."""


class HelpAction(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        if values:
            make_parser().parse_args([values, '--help'])
        else:
            parser.print_help()
        parser.exit()


class CommandArgumentParser(argparse.ArgumentParser):
    commands = None
    commands_dispatch = None

    def __init__(self, *args, **kwargs):
        kwargs['add_help'] = False

        super().__init__(*args, formatter_class=argparse.RawDescriptionHelpFormatter, **kwargs)
        self.add_argument('-h', '--help', metavar='COMMAND', nargs='?', action=HelpAction, help='Display help and exit.')
        help_command = self.add_command('help', description='Display help and exit.')
        help_command.add_argument('command', nargs='?', action=HelpAction)

    def add_command(self, name, **opts):
        if self.commands is None:
            self.commands = self.add_subparsers(
                title='commands',
                dest='command',
                parser_class=argparse.ArgumentParser,
            )
            self.commands_dispatch = {}
        if 'description' in opts and 'help' not in opts:
            opts['help'] = opts['description']

        command = self.commands.add_parser(name, formatter_class=argparse.RawDescriptionHelpFormatter, **opts)
        self.commands_dispatch[name] = command
        return command


def add_glob_or_file(addoption):
    addoption('glob_or_file', nargs='*', help='Glob or exact path for JSON files. If not specified all runs are loaded.')


def make_parser():
    parser = CommandArgumentParser('py.test-benchmark', description="pytest_benchmark's management commands.")
    add_global_options(parser.add_argument, prefix='')

    parser.add_command('list', description='List saved runs.')

    compare_command = parser.add_command(
        'compare',
        description='Compare saved runs.',
        epilog="""examples:

    pytest-benchmark compare 'Linux-CPython-3.5-64bit/*'

        Loads all benchmarks ran with that interpreter. Note the special quoting that disables your shell's glob
        expansion.

    pytest-benchmark compare 0001

        Loads first run from all the interpreters.

    pytest-benchmark compare /foo/bar/0001_abc.json /lorem/ipsum/0001_sir_dolor.json

        Loads runs from exactly those files.""",
    )
    add_display_options(compare_command.add_argument, prefix='')
    add_histogram_options(compare_command.add_argument, prefix='')
    compare_command.add_argument(
        '--between',
        metavar='COLUMNS',
        type=parse_columns,
        default=None,
        help='Compare same-named benchmarks across different source files. '
        'Takes a comma-separated list of metric columns to compare (e.g. min,mean,ops).',
    )
    add_glob_or_file(compare_command.add_argument)
    add_csv_options(compare_command.add_argument, prefix='')
    compare_command.add_argument(
        '-k',
        metavar='EXPR',
        dest='filter_expr',
        default=None,
        help="Only show benchmarks matching the given expression. Uses the same syntax as pytest's -k option (e.g. 'foo and not bar').",
    )

    return parser


class HookDispatch:
    def __init__(self, *, root, **kwargs):
        _, _, config, *_ = locate_config(invocation_dir=root, args=())
        conftest_file = pathlib.Path('conftest.py')
        if conftest_file.exists():
            self.conftest = pathlib.import_path(
                conftest_file,
                **kwargs,
                root=root,
                consider_namespace_packages=bool(config.get('consider_namespace_packages')),
            )
        else:
            self.conftest = None

    def __getattr__(self, item):
        default = getattr(plugin, item)
        return getattr(self.conftest, item, default)


def main():
    parser = make_parser()
    args = parser.parse_args()
    level = Logger.QUIET if args.quiet else Logger.NORMAL
    if args.verbose:
        level = Logger.VERBOSE
    logger = Logger(level)
    storage = load_storage(args.storage, logger=logger, netrc=args.netrc)

    hook = HookDispatch(mode=args.importmode, root=pathlib.Path('.'))

    if args.command == 'list':
        for file in storage.query():
            print(file)
    elif args.command == 'compare':
        histogram = first_or_value(args.histogram, False)
        if args.between:
            if args.columns:
                parser.error('--between is not compatible with --columns (--between already specifies the columns)')
            if histogram:
                parser.error('--between is not compatible with --histogram')
            results_table_cls = CompareBetweenResults
            args.columns = args.between
        else:
            results_table_cls = TableResults
            if not args.columns:
                args.columns = DEFAULT_COLUMNS

        results_table = results_table_cls(
            columns=args.columns,
            sort=args.sort,
            histogram=histogram,
            name_format=NAME_FORMATTERS[args.name],
            logger=logger,
            scale_unit=partial(
                hook.pytest_benchmark_scale_unit,
                config=Config.fromdictargs({'benchmark_time_unit': args.time_unit}, []),
            ),
        )
        benchmarks = storage.load_benchmarks(*args.glob_or_file)
        if args.filter_expr:
            from _pytest.mark.expression import Expression  # noqa: PLC0415

            expr = Expression.compile(args.filter_expr)

            def _evaluate_expr(benchmark):
                name = benchmark.get('fullname') or benchmark.get('name', '')
                return expr.evaluate(lambda key: key in name)

            benchmarks = filter(_evaluate_expr, benchmarks)
        groups = hook.pytest_benchmark_group_stats(
            benchmarks=benchmarks,
            group_by=args.group_by,
            config=None,
        )
        results_table.display(TerminalReporter(), groups, progress_reporter=report_noprogress)
        if args.csv:
            results_csv = CSVResults(args.columns, args.sort, logger)
            (output_file,) = args.csv

            results_csv.render(output_file, groups)
    elif args.command is None:
        parser.error('missing command (available commands: {})'.format(', '.join(map(repr, parser.commands.choices))))
    else:
        parser.error(f'unexpected command {args.command!r}')


class TerminalReporter:
    def __init__(self):
        self._tw = TerminalWriter()

    def ensure_newline(self):
        pass

    def write(self, content, **markup):
        self._tw.write(content, **markup)

    def write_line(self, line, **markup):
        if not isinstance(line, str):
            line = line.decode(errors='replace')
        self._tw.line(line, **markup)

    def rewrite(self, line, **markup):
        line = str(line)
        self._tw.write('\r' + line, **markup)

    def write_sep(self, sep, title=None, **markup):
        self._tw.sep(sep, title, **markup)

    def section(self, title, sep='=', **kw):
        self._tw.sep(sep, title, **kw)

    def line(self, msg, **kw):
        self._tw.line(msg, **kw)
