#!/usr/bin/env python

import argparse
import os
import subprocess
import sys
import traceback
import re
import pystache
import logging
from collections import defaultdict

# Locate clang bindings and clang library.

__root__ = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, __root__)


def guess_clang_library_path():
    try:
        return subprocess.check_output(["llvm-config-3.5", "--libdir"]).strip()
    except OSError as e:
        if e.errno == 2:
            pass
        else:
            raise
    return None

try:
    import clang.cindex
    clang.cindex.conf.set_library_path(guess_clang_library_path())
    clang.cindex.conf.get_cindex_library()
    from clang.cindex import \
        CompilationDatabase, CursorKind, Diagnostic, Index, TranslationUnit
except ImportError:
    print >>sys.stderr, "Failed to import 'clang.cindex'"
    traceback.print_exc(file=sys.stderr)
    sys.exit(1)


sh = logging.StreamHandler(sys.stderr)
sh.setFormatter(logging.Formatter("%(asctime)s - %(levelname)8s - %(message)s"))
sh.setLevel(logging.DEBUG)
logger = logging.getLogger()
logger.addHandler(sh)
logger.setLevel(logging.DEBUG)


def get_translation_units(directory, filenames, shallow):
    """Compiles specified files into AST. Yields translation units."""
    db = CompilationDatabase.fromDirectory(directory)
    idx = Index.create()

    if shallow:
        options = TranslationUnit.PARSE_SKIP_FUNCTION_BODIES
    else:
        options = TranslationUnit.PARSE_NONE

    for filename in filenames:
        compile_commands = db.getCompileCommands(filename)
        if compile_commands is None:
            logger.warn("No such file '%s' in compilation database under '%s'", filename, directory)
        else:
            for compile_command in compile_commands:
                wd, path, args = get_compiler_args(compile_command)
                os.chdir(wd)
                tu = idx.parse(path, args, options=options)
                ok = True
                if len(tu.diagnostics) > 0:
                    logger.warn(
                        "There were %s diagnostic messages while parsing '%s'",
                        len(tu.diagnostics),
                        filename)
                    for item in tu.diagnostics:
                        logger.warn("  * %s", item)
                        if item.severity >= Diagnostic.Error:
                            ok = False
                if ok:
                    yield tu
                else:
                    logger.error("Skipping '%s' due to parsing errors", filename)


def get_compiler_args(compile_command):
    """Extracts proper compiler invocation arguments."""
    def _system_args(cc1):
        # XXX(sandello): Force clang as the compiler to avoid clashing with gcc internals.
        cc1 = "clang++-3.5"
        with open("/dev/null") as handle:
            output = subprocess.check_output([cc1, "-E", "-x", "c++", "-", "-v"],
                                             stdin=handle,
                                             stderr=subprocess.STDOUT)
        # Enforce C++ as a language.
        yield "-x"
        yield "c++"
        # Add system include paths.
        skip = True
        for line in output.split("\n"):
            if line.startswith("#include <...> search starts here"):
                skip = False
                continue
            if line.startswith("End of search list"):
                skip = True
                continue
            if skip:
                continue
            yield "-I" + line.strip()

    def _user_args(args):
        yield "-Wno-undefined-inline"
        skip = False
        for arg in args:
            # Omit target flags.
            if arg.startswith("-f") or arg.startswith("-m"):
                continue
            # Omit code generation flags.
            if arg.startswith("-O") or arg.startswith("-c") or arg.startswith("-g"):
                continue
            if arg == "-o":
                skip = True
                continue
            if skip:
                skip = False
                continue
            yield arg

    args = list(compile_command.arguments)
    path = args[-1]
    args = list(_system_args(args[0])) + list(_user_args(args[1:-1]))

    assert path.endswith(".cpp")
    assert os.path.isfile(path)

    return compile_command.directory, path, args


def extract_perl_api_mapping(tus):
    matcher = RecordDeclMatcher(AllOfMatcher(
        IsDefinitionMatcher(),
        TypeMatcher(NameMatcher(r"NYT::NApi::T.*Options"))
    ))

    options = defaultdict(lambda: {"rank": 0})

    def _to_snake_case(name):
        return re.sub(
            r"((?<=[a-z0-9])[A-Z]|(?!^)[A-Z](?=[a-z]))",
            r"_\1",
            name).lower()

    def _extract_bases(c):
        for c in c.get_children():
            if c.kind == CursorKind.CXX_BASE_SPECIFIER:
                base = options[c.referenced.get_usr()]
                base["rank"] += 1
                yield base

    def _extract_fields(c):
        for c in c.get_children():
            if c.kind == CursorKind.FIELD_DECL:
                yield {
                    "name": c.spelling,
                    "type": c.type.spelling,
                    "key": _to_snake_case(c.spelling),
                    "key_length": len(_to_snake_case(c.spelling)),
                }

    for tu in tus:
        for cursor in matcher(tu):
            usr = cursor.get_usr()
            bases = list(_extract_bases(cursor))
            fields = list(_extract_fields(cursor))
            options[usr].update({
                "usr": usr,
                "type": cursor.type.spelling,
                "bases": bases,
                "fields": fields,
                "number_of_bases": len(bases),
                "number_of_fields": len(fields),
            })

    seeds = sorted(options.iteritems(), key=lambda p: (-p[1]["rank"], p[1]["type"]), reverse=True)
    seeds = map(lambda p: p[1], seeds)
    marks = dict()
    result = []

    def dfs(option):
        marks[option["usr"]] = False
        for base in option["bases"]:
            mark = marks.get(base["usr"], None)
            if mark is True:
                continue
            if mark is False:
                raise ValueError("Cyclic dependency.")
            seeds.remove(base)
            dfs(base)
        marks[option["usr"]] = True
        result.append(option)

    while seeds:
        dfs(seeds.pop())

    template = """
// Mapping for '{{{type}}}'
template <>
struct TPerlMappingTraits<{{{type}}}>
{
    static inline void FromPerlImpl({{{type}}}* result, SV* sv, TPerlMappingContext* ctx)
    {
        {{#bases}}
        // Base '{{{type}}}'
        TPerlMappingTraits<{{{type}}}>::FromPerlImpl(result, sv, ctx);
        {{/bases}}
        {{#number_of_fields}}
        // Fields
        HV* hash = nullptr;
        if (SvROK(sv) && SvTYPE(SvRV(sv)) == SVt_PVHV) {
            hash = reinterpret_cast<HV*>(SvRV(sv));
        } else {
            THROW_ERROR_EXCEPTION("Expected a hash reference while parsing {{{type}}}");
        }
        {{#fields}}
        // Field '{{{name}}}'
        {
            SV** value = hv_fetch(hash, "{{{key}}}", {{{key_length}}}, 0);
            if (value) {
                result->{{{name}}} = FromPerl<{{{type}}}>(*value, ctx);
            }
        }
        {{/fields}}
        {{/number_of_fields}}
    }
};"""

    print """// This file was auto-generated. Do not edit.
#ifndef PERL_MAPPING_TRAITS_GENERATED_H_
#error "Direct inclusion of this file is not allowed"
#endif
#undef PERL_MAPPING_TRAITS_GENERATED_H_
"""

    for option in result:
        print pystache.render(template, option)


def dump_tree(node, iter_fn, label_fn):
    from cStringIO import StringIO

    def _dump_tree_impl(node, prefix, iter_fn, label_fn):
        s = StringIO()

        if prefix:
            s.write(prefix[:-3])
            s.write("  +--")
        s.write(label_fn(node))
        s.write("\n")

        children = list(iter_fn(node))

        for index, child in enumerate(children):
            if 1 + index == len(children):
                child_prefix = prefix + "   "
            else:
                child_prefix = prefix + "  |"
            s.write(_dump_tree_impl(child, child_prefix, iter_fn, label_fn))

        return s.getvalue()

    return _dump_tree_impl(node, "", iter_fn, label_fn)


def dump_cursor(cursor):
    def _iter_fn(cursor):
        return cursor.get_children()

    def _label_fn(cursor):
        text = cursor.spelling or cursor.displayname
        kind = str(cursor.kind)[1 + str(cursor.kind).index("."):]
        return kind + " " + text

    return dump_tree(cursor, _iter_fn, _label_fn)


class AstMatcher(object):
    def __init__(self):
        pass

    def __call__(self, tu):
        if not isinstance(tu, TranslationUnit):
            raise TypeError("Expected TranslationUnit")
        for cursor in tu.cursor.walk_preorder():
            if self.matches(cursor):
                yield cursor

    def matches(self, cursor):
        raise NotImplemented


class TrueMatcher(AstMatcher):
    def matches(self, cursor):
        return True


class FalseMatcher(AstMatcher):
    def matches(self, cursor):
        return False


class AllOfMatcher(AstMatcher):
    def __init__(self, *args):
        self.args = args

    def matches(self, cursor):
        return all(arg.matches(cursor) for arg in self.args)


class AnyOfMatcher(AstMatcher):
    def __init__(self, args):
        self.args = args

    def matches(self, cursor):
        return any(arg.matches(cursor) for arg in self.args)


class WrappingAstMatcher(AstMatcher):
    def __init__(self, inner):
        self.inner = inner

    def matches(self, cursor):
        return self.inner.matches(cursor)


class RecordDeclMatcher(WrappingAstMatcher):
    def matches(self, cursor):
        if cursor.kind == CursorKind.STRUCT_DECL \
                or cursor.kind == CursorKind.UNION_DECL \
                or cursor.kind == CursorKind.CLASS_DECL:
                    return self.inner.matches(cursor)


class TypeMatcher(WrappingAstMatcher):
    def matches(self, cursor):
        return self.inner.matches(cursor.type)


class NameMatcher(AstMatcher):
    def __init__(self, pattern):
        self.matcher = re.compile(pattern)

    def matches(self, cursor):
        return self.matcher.match(cursor.spelling or cursor.displayname)


class IsDefinitionMatcher(AstMatcher):
    def matches(self, cursor):
        return cursor.is_definition()


def main():
    parser = argparse.ArgumentParser(description="Clang Tools")
    parser.add_argument("-a", "--action", required=True,
                        help="What to do?")
    parser.add_argument("-b", "--build-directory", required=True,
                        help="Build directory with a compilation database")
    parser.add_argument("filenames", nargs="+",
                        help="Source files to process")
    args = parser.parse_args()

    if args.action == "perl_api_mapping":
        tus = get_translation_units(args.build_directory, args.filenames, True)
        extract_perl_api_mapping(tus)
    else:
        raise RuntimeError("Bad action :(")


if __name__ == "__main__":
    main()
