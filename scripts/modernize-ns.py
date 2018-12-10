#!/usr/bin/python

import fileinput
import sys

begin_nss = []
NAMESPACE_BEGIN = "namespace "

def get_begin_ns(line):
    if not line.startswith(NAMESPACE_BEGIN):
        return None
    ns = line[len(NAMESPACE_BEGIN):-1].strip()
    if len(ns) == 0:
        return None
    if "::" in ns:
        return None
    return ns

def flush_begin_nss():
    global begin_nss
    if len(begin_nss) > 0:
        sys.stdout.write("namespace ")
        for i in xrange(len(begin_nss)):
            print begin_nss[i],
            if i < len(begin_nss) - 1:
                sys.stdout.write("::")
        sys.stdout.write(" {\n")
    begin_nss = []

end_nss = []
NAMESPACE_END = "} // namespace "

def get_end_ns(line):
    if not line.startswith(NAMESPACE_END):
        return None
    ns = line[len(NAMESPACE_END):].strip()
    if len(ns) == 0:
        return None
    if "::" in ns:
        return None
    return ns    

def flush_end_nss():
    global end_nss
    if len(end_nss) > 0:
        sys.stdout.write("} // namespace ")
        for i in reversed(xrange(len(end_nss))):
            print end_nss[i],
            if i > 0:
                sys.stdout.write("::")
        sys.stdout.write("\n")
    end_nss = []

def flush_nss():
    flush_begin_nss()
    flush_end_nss()

if __name__ == "__main__":
    for line in fileinput.input():
        line = line[:-1]
        begin_ns = get_begin_ns(line)
        end_ns = get_end_ns(line)
        if begin_ns is not None:
            begin_nss.append(begin_ns)
        elif end_ns is not None:
            end_nss.append(end_ns)
        else:
            flush_nss()
            sys.stdout.write( line + "\n")

    flush_nss()
