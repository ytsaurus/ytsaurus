#!/usr/bin/python

import fileinput
import sys

begin_ns = []
NAMESPACE_BEGIN = "namespace "

def flush_begin_ns():
    global begin_ns
    if len(begin_ns) > 0:
        sys.stdout.write("namespace ")
        for i in xrange(len(begin_ns)):
            print begin_ns[i],
            if i < len(begin_ns) - 1:
                sys.stdout.write("::")
        sys.stdout.write(" {\n")
    begin_ns = []

end_ns = []
NAMESPACE_END = "} // namespace "

def flush_end_ns():
    global end_ns
    if len(end_ns) > 0:
        sys.stdout.write("} // namespace ")
        for i in xrange(len(end_ns)):
            print end_ns[i],
            if i < len(end_ns) - 1:
                sys.stdout.write("::")
        sys.stdout.write("\n")
    end_ns = []

def flush_ns():
    flush_begin_ns()
    flush_end_ns()

if __name__ == "__main__":
    for line in fileinput.input():
        line = line[:-1]
        if line.startswith(NAMESPACE_BEGIN):
            ns = line[len(NAMESPACE_BEGIN):-1].strip()
            begin_ns.append(ns)
        elif line.startswith(NAMESPACE_END):
            ns = line[len(NAMESPACE_END):].strip()
            end_ns.append(ns)
        else:
            flush_ns()
            sys.stdout.write(line + "\n")

    flush_begin_ns()

