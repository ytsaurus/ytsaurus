#!/usr/bin/python
################################################################################

import sys

if sys.hexversion <= 0x2060000:
    print >>sys.stderr, 'Incompatible Python version. Python >= 2.6 required.'
    sys.exit(255)

################################################################################
### PEG grammar for CMakeLists.txt

from pyparsing import *

# A small PEG parser for CMake-like grammar. Currently it covers most of
# CMake syntax. After parsing it produces a list of commands (see tests below).
Name         = Forward()
Name        << ( Word(alphas + '_') | Combine('$' + '{' + Name + '}') )
ArgQuoted    = QuotedString('"', escChar = '\\', multiline = True)
ArgUnquoted  = Word(alphanums + '!$%&*+,-./:;<=>?@[\\]^_`{|}~')
Argument     = ArgQuoted | ArgUnquoted
Command      = ( Word(alphas + '_') + Suppress('(') + ZeroOrMore(Argument) + Suppress(')') )
List         = OneOrMore(Group(Command))

List.ignore(pythonStyleComment)
List.enablePackrat()


################################################################################
### Auxiliary functions.

# Parses a CMakeLists.txt into pyparsing internal representation.
def parse(list):
    return List.parseFile(list, parseAll = True)

# Parses a CMakeLists.txt into a list of commands.
def parse_to_iterable(list):
    try:
        for command in parse(list):
            yield command[0].lower(), command[1:]
    except ParseException, e:
        print >>sys.stderr, 'Error while parsing {list} (line {line}, column {column}): {message}'.format(
            list = list, line = e.lineno, column = e.col, message = e.msg)
        print >>sys.stderr, ' > ', e.line
        print >>sys.stderr, ' > ', ' ' * (e.column - 1) + '^'

# Extracts all targets from a CMakeLists.txt.
def extract_targets(list):
    for command, arguments in parse_to_iterable(list):
        if command == 'add_library':
            yield 'library', arguments[0]
        if command == 'add_executable':
            yield 'executable', arguments[0]


################################################################################
################################################################################

if __name__ == '__main__':
    from StringIO import StringIO
    import unittest

    class TestCMakeParser(unittest.TestCase):
        CASES = [
            ("""
                if (YT_BUILD_WITH_STLPORT)
                    add_subdirectory(extern/STLport/build/cmake)
                endif()
            """,
            [
                ('if', ['YT_BUILD_WITH_STLPORT']),
                ('add_subdirectory', ['extern/STLport/build/cmake']),
                ('endif', [])
            ]),

            ("""
                add_subdirectory(contrib/libs)
                add_subdirectory(contrib/libs/snappy)
            """,
            [
                ('add_subdirectory', ['contrib/libs']),
                ('add_subdirectory', ['contrib/libs/snappy'])
            ]),

            ("""
                if ( NOT ${ARCADIA_ROOT} EQUAL ${ARCADIA_BUILD_ROOT} )
                if ( NOT EXISTS ${ARCADIA_BUILD_ROOT}/yt/experiments/meta_state/ut/configs )
                EXECUTE_PROCESS(COMMAND ln -s ${ARCADIA_ROOT}/yt/experiments/master_state/ut/configs ${ARCADIA_BUILD_ROOT}/yt/experiments/meta_state/ut/configs )
                endif( NOT EXISTS ${ARCADIA_BUILD_ROOT}/yt/experiments/meta_state/ut/configs )
                endif( NOT ${ARCADIA_ROOT} EQUAL ${ARCADIA_BUILD_ROOT} )
            """,
            [
                ('if', ['NOT', '${ARCADIA_ROOT}', 'EQUAL', '${ARCADIA_BUILD_ROOT}']),
                ('if', ['NOT', 'EXISTS', '${ARCADIA_BUILD_ROOT}/yt/experiments/meta_state/ut/configs']),
                ('execute_process', ['COMMAND', 'ln', '-s', '${ARCADIA_ROOT}/yt/experiments/master_state/ut/configs', '${ARCADIA_BUILD_ROOT}/yt/experiments/meta_state/ut/configs']),
                ('endif', ['NOT', 'EXISTS', '${ARCADIA_BUILD_ROOT}/yt/experiments/meta_state/ut/configs']),
                ('endif', ['NOT', '${ARCADIA_ROOT}', 'EQUAL', '${ARCADIA_BUILD_ROOT}'])
            ])
        ]

        def testEverything(self):
            for source, result in self.CASES:
                actual = list(parse_to_iterable(StringIO(source)))
                expected = result

                self.assertEqual(actual, expected)

    unittest.main()
