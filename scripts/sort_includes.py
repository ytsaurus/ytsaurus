#!/usr/bin/env python
# Mostly inspired by http://llvm.org/viewvc/llvm-project/llvm/trunk/utils/sort_includes.py?view=markup

import argparse
import os
import sys
import itertools

STL_HEADERS = """
<algorithm>
<array>
<atomic>
<bitset>
<cassert>
<cctype>
<cerrno>
<cfenv>
<cfloat>
<chrono>
<cinttypes>
<climits>
<clocale>
<cmath>
<codecvt>
<complex>
<condition_variable>
<csetjmp>
<csignal>
<cstdarg>
<cstddef>
<cstdint>
<cstdio>
<cstdlib>
<cstring>
<ctime>
<cuchar>
<cwchar>
<cwctype>
<deque>
<exception>
<forward_list>
<fstream>
<functional>
<future>
<initializer_list>
<iomanip>
<ios>
<iosfwd>
<iostream>
<istream>
<iterator>
<limits>
<list>
<locale>
<map>
<memory>
<mutex>
<new>
<numeric>
<ostream>
<queue>
<random>
<ratio>
<regex>
<scoped_allocator>
<set>
<shared_mutex>
<sstream>
<stack>
<stdexcept>
<streambuf>
<string>
<strstream>
<system_error>
<thread>
<tuple>
<typeindex>
<typeinfo>
<type_traits>
<unordered_map>
<unordered_set>
<utility>
<valarray>
<vector>
""".split("\n")
STL_HEADERS = map(lambda h: h[1:-1], STL_HEADERS)
STL_HEADERS = set(filter(lambda h: h != "", STL_HEADERS))

SOURCE_EXTENSIONS = {".cpp", ".c", ".xs"}
HEADER_EXTENSIONS = {".h"}


class IncludeDirective(object):
    def __init__(self, line, filename):
        assert line.startswith("#include")
        self.line = line
        self.filename = filename

        self.header = line[len("#include"):].strip()
        self.is_local = self.header.startswith('"')
        self.header = self.header[1:-1]

    @property
    def sort_priority(self):
        h = self.header
        f = self.filename
        if self.is_local:
            if os.path.splitext(h)[0] == os.path.splitext(f)[0]:
                return 0
            elif h == "public.h":
                return 1
            elif h == "private.h":
                return 2
            else:
                return 3
        if h.startswith("yt/server/"):
            return 100
        if h.startswith("yt/ytlib/"):
            return 110
        if h.startswith("yt/core/"):
            return 120
        if h.startswith("library/"):
            return 200
        if h.startswith("util/"):
            return 210
        if h.startswith("contrib/") or h.startswith("yt/contrib/"):
            return 300
        if h in STL_HEADERS:
            return 1000
        return 10000

    @property
    def sort_group(self):
        x = self.sort_priority
        if x >= 10000:
            return "system"
        if x >= 1000:
            return "stl"
        if x >= 200:
            return "/".join(self.header.split("/")[:2])
        if x >= 100:
            return "/".join(self.header.split("/")[:3])
        return ""


def sort_includes(f):
    """Sort the #include lines of a specific file."""

    if "yt/contrib/" in f.name:
        return

    sfn, ext = os.path.splitext(f.name)
    sfn = os.path.basename(sfn)
    bfn = os.path.basename(f.name)
    dfn = os.path.dirname(f.name)

    if ext not in SOURCE_EXTENSIONS and ext not in HEADER_EXTENSIONS:
        return

    lines = f.readlines()
    header_lines = []

    look_for_paired_file = ext in SOURCE_EXTENSIONS
    found_headers = False
    headers_begin = 0
    headers_end = 0

    for (i, l) in enumerate(lines):
        if l.strip() == "":
            continue

        if l.startswith("#include"):
            if not found_headers:
                headers_begin = i
                found_headers = True
            headers_end = i

            directive = IncludeDirective(l, bfn)
            header_lines.append((directive.sort_priority, directive.sort_group, l))
            continue

        # Only allow comments and #defines prior to any includes. If either are
        # mixed with includes, the order might be sensitive.
        if found_headers:
            break

        if l.startswith("//") or l.startswith("#define") or l.startswith("#pragma"):
            continue

        break

    if not found_headers:
        return
    elif look_for_paired_file:
        if os.path.exists(os.path.join(dfn, sfn + ".h")):
            fake_line = "#include \"%s.h\"\n" % sfn
            fake_directive = IncludeDirective(fake_line, bfn)
            header_lines.append((fake_directive.sort_priority, fake_directive.sort_group, fake_line))

    header_lines_set = sorted(set(header_lines))
    header_lines = []

    for _, header_group in itertools.groupby(header_lines_set, key=lambda t: t[1]):
        for _, _, header_line in header_group:
            header_lines.append(header_line)
        header_lines.append("\n")

    if header_lines[-1] == "\n":
        header_lines.pop()

    lines = lines[:headers_begin] + header_lines + lines[headers_end + 1:]

    f.seek(0)
    f.truncate()
    f.writelines(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("files", nargs="+", type=argparse.FileType("r+"),
                        help="the source files to sort includes within")

    args = parser.parse_args()
    for f in args.files:
        sort_includes(f)

if __name__ == "__main__":
    main()
