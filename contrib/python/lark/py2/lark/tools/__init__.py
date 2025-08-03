import sys
from argparse import ArgumentParser, FileType
try:
    from textwrap import indent
except ImportError:
    def indent(text, prefix):
        return ''.join(prefix + line for line in text.splitlines(True))
from logging import DEBUG, INFO, WARN, ERROR
import warnings

from lark import Lark, logger

lalr_argparser = ArgumentParser(add_help=False, epilog='Look at the Lark documentation for more info on the options')

flags = [
    ('d', 'debug'),
    'keep_all_tokens',
    'regex',
    'propagate_positions',
    'maybe_placeholders',
    'use_bytes'
]

options = ['start', 'lexer']

lalr_argparser.add_argument('-v', '--verbose', action='count', default=0, help="Increase Logger output level, up to three times")
lalr_argparser.add_argument('-s', '--start', action='append', default=[])
lalr_argparser.add_argument('-l', '--lexer', default='contextual', choices=('standard', 'contextual'))
k = {'encoding': 'utf-8'} if sys.version_info > (3, 4) else {}
lalr_argparser.add_argument('-o', '--out', type=FileType('w', **k), default=sys.stdout, help='the output file (default=stdout)')
lalr_argparser.add_argument('grammar_file', type=FileType('r', **k), help='A valid .lark file')

for f in flags:
    if isinstance(f, tuple):
        options.append(f[1])
        lalr_argparser.add_argument('-' + f[0], '--' + f[1], action='store_true')
    else:
        options.append(f)
        lalr_argparser.add_argument('--' + f, action='store_true')


def build_lalr(namespace):
    logger.setLevel((ERROR, WARN, INFO, DEBUG)[min(namespace.verbose, 3)])
    if len(namespace.start) == 0:
        namespace.start.append('start')
    kwargs = {n: getattr(namespace, n) for n in options}
    return Lark(namespace.grammar_file, parser='lalr', **kwargs), namespace.out


def showwarning_as_comment(message, category, filename, lineno, file=None, line=None):
    # Based on warnings._showwarnmsg_impl
    text = warnings.formatwarning(message, category, filename, lineno, line)
    text = indent(text, '# ')
    if file is None:
        file = sys.stderr
        if file is None:
            return 
    try:
        file.write(text)
    except OSError:
        pass


def make_warnings_comments():
    warnings.showwarning = showwarning_as_comment
