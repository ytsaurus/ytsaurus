#!/usr/bin/python
# vim:set fileencoding=utf-8:

"""Automatic generation of macroses for preprocessor metaprogramming."""

import sys
import time
import functools

from optparse import OptionParser

################################################################################
class Generator:
    """Handy DSL for generation."""
    def __init__(self, handle, limit):
        self.handle = handle
        self.limit = limit
        pass

    def __xor__(self, function):
        print >>self.handle, 80 * "/"
        function(self)
        print >>self.handle, ""

    def __or__(self, string):
        needs_format = False
        needs_format = needs_format or ("{i}" in string) or ("{j}" in string)
        needs_format = needs_format or ("{prev}" in string) or ("{next}" in string)

        if needs_format:
            for index in range(self.limit):
                print >>self.handle, string.format(
                    i = index, j = index + 1,
                    prev = index, next = index + 1
                )
        else:
            print >>self.handle, string

################################################################################
def make_all(handle, limit):
    """Default entry point for generation."""

    g = Generator(handle, limit)

    g ^ make_header

    g ^ make_function_count
    g ^ make_function_kill
    g ^ make_function_element
    g ^ make_function_head
    g ^ make_function_tail
    g ^ make_for_each

    g ^ make_footer

################################################################################
def make_header(g):
    g | """
#pragma once

/*!
  \internal
*/

// WARNING: This file was auto-generated.
// Please, consider incorporating any changes into the generator.

// Generated on {timestamp}.

#ifndef PREPROCESSOR_GEN_H_
#error "Direct inclusion of this file is not allowed, include preprocessor.h"
#endif
#undef PREPROCESSOR_GEN_H_
""".strip("\r\n").format(timestamp = time.ctime())

################################################################################
def make_footer(g):
    g | """
/*!
  \endinternal
*/
""".strip("\r\n")

################################################################################
def make_function_count(g):
    g | "#define PP_COUNT_IMPL(...) PP_CONCAT(PP_COUNT_CONST_, PP_COUNT_IMPL_0 __VA_ARGS__)"
    g | "#define PP_COUNT_CONST_PP_COUNT_IMPL_{prev} {prev}"
    g | "#define PP_COUNT_IMPL_{prev}(_) PP_COUNT_IMPL_{next}"

################################################################################
def make_function_kill(g):
    g | "#define PP_KILL_IMPL(seq, index) PP_CONCAT(PP_KILL_IMPL_, index) seq"
    g | "#define PP_KILL_IMPL_0"
    g | "#define PP_KILL_IMPL_{next}(_) PP_KILL_IMPL_{prev}"

################################################################################
def make_function_element(g):
    g | "#define PP_ELEMENT_IMPL(seq, index) " \
        "PP_ELEMENT_IMPL_A((PP_CONCAT(PP_ELEMENT_IMPL_, index) seq))"
    g | "#define PP_ELEMENT_IMPL_A(x) PP_ELEMENT_IMPL_C(PP_ELEMENT_IMPL_B x)"
    g | "#define PP_ELEMENT_IMPL_B(x, _) x"
    g | "#define PP_ELEMENT_IMPL_C(x) x"

    g | "#define PP_ELEMENT_IMPL_0(x) x, PP_NIL"
    g | "#define PP_ELEMENT_IMPL_{next}(_) PP_ELEMENT_IMPL_{prev}"

################################################################################
def make_function_head(g):
    g | "#define PP_HEAD_IMPL(seq) PP_ELEMENT_IMPL(seq, 0)"

################################################################################
def make_function_tail(g):
    g | "#define PP_TAIL_IMPL(seq) PP_KILL_IMPL(seq, 1)"

################################################################################
def make_for_each(g):
    g | "#define PP_FOR_EACH_IMPL(what, seq) PP_CONCAT(PP_FOR_EACH_IMPL_, PP_COUNT(seq))(what, seq)"
    g | "#define PP_FOR_EACH_IMPL_0(what, seq)"
    g | "#define PP_FOR_EACH_IMPL_{next}(what, seq) " \
        "what(PP_HEAD(seq)) PP_FOR_EACH_IMPL_{prev}(what, PP_TAIL(seq))"

################################################################################
if __name__ == "__main__":
    parser = OptionParser(description = __doc__)
    parser.add_option("-n", "--limit",
        metavar = "N", type = "int", default = 99,
        help = "limit on number of elements in sequence"
    )

    (options, args) = parser.parse_args()

    if not options.limit:
        parser.error("-n/--limit option is required")
        parser.print_usage()

        sys.exit(1)

    make_all(sys.stdout, options.limit)

