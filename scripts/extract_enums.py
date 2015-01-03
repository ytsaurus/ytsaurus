#!/usr/bin/env python

import os
import sys
import string

from argparse import ArgumentParser

def show_handler(filename, first_line, last_line):
    print "%s:" % filename
    print ">>>>>>"
    index = 0
    for line in open(filename):
        if index >= first_line and index <= last_line:
            print line[:-1]
        index += 1
    print "<<<<<<"
    print
    print

def parse(filename, first_line, last_line):
    def strip_begin(text, prefix):
        assert text.startswith(prefix), "Text %s should starts with %s" % (text, prefix)
        return text[len(prefix):]
    
    def strip_end(text, suffix):
        assert text.endswith(suffix), "Text %s should ends with %s" % (text, suffix)
        return text[:len(text) - len(suffix)]

    enum_lines = []
    index = 0
    for line in open(filename):
        line = line.split("//")[0]
        if index >= first_line and index <= last_line:
            enum_lines.append(line)
        index += 1
    
    text = "".join(enum_lines)
    for sym in [" ", "\t", "\n"]:
        text = text.replace(sym, "")
    
    text = strip_begin(text, "DEFINE_ENUM(")
    text = strip_end(text, ");")

    # Instead of fair parsing we use some hacky way
    # that may parse correctly something weird
    tokens = text.split("((")

    clean = lambda x: x.strip(",()")

    enum_name = clean(tokens[0].replace(",", ""))
    items = map(lambda token: map(clean, token.split(")(")), tokens[1:])

    return enum_name, items


def java_handler(namespace, filename, first_line, last_line):
    name, items = parse(filename, first_line, last_line)
    print \
"""public enum %s {
    %s;

    private final int index;   

    Foo(int index) {
        this.index = index;
    }

    public int index() { 
        return index; 
    }

}""" % (namespace[1:] + "ErrorCode", ",\n    ".join("%s (%s)" % (item[0], item[1]) for item in items))


def python_handler(namespace, filename, first_line, last_line):
    name, items = parse(filename, first_line, last_line)
    print \
"""class %s:
    %s
""" % (namespace[1:] + "ErrorCode", "\n    ".join("%s=%s" % (item[0], item[1]) for item in items))

numbers = {}
def check_handler(namespace, filename, first_line, last_line):
    name, items = parse(filename, first_line, last_line)
    for item in items:
        if item[1] in numbers:
            print >>sys.stderr, "Duplicated error codes:"
            print >>sys.stderr, filename, first_line, last_line
            for x in numbers[item[1]]:
                print >>sys.stderr, x,
            print >>sys.stderr
        numbers[item[1]] = (filename, first_line, last_line)


def calculate_balance(line):
    return line.count("(") - line.count(")")

def find_number(line):
    def is_number(word):
        return all(char in string.digits for char in word)
    return any(is_number(word) for word in line.replace("(", " ").replace(")", " ").split())

def main():
    parser = ArgumentParser()
    parser.add_argument("path")
    parser.add_argument("--check", action="store_true", default=False)
    parser.add_argument("--output-format")
    parser.add_argument("--exclude", action="append", default=[])
    args = parser.parse_args()

    if args.path is None:
        print >>sys.stderr, "You should pass a path to search for enums"

    if args.check:
        handler = check_handler
    elif args.output_format is None:
        handler = show_handler
    elif args.output_format == "java":
        handler = java_handler
    elif args.output_format == "python":
        handler = python_handler
    elif args.output_format == "check":
        handler = check_handler
    else:
        print >>sys.stderr, "Incorrect output format:", args.output_format
        sys.exit(1)

    for root, dirs, files in os.walk(args.path):
        if any(root.startswith(prefix) for prefix in args.exclude):
            continue
        for file in [os.path.join(root, file) for file in files]:
            if file.endswith(".h") or file.endswith(".cpp"):
                index = 0
                inside = False
                start = None
                balance = None
                has_number = False
                namespace = None
                for line in open(file):
                    line = line.split("//")[0]
                    if line.strip() and line.split()[0] == "namespace" and line.replace("{", "").strip() != "namespace":
                        namespace = line.split("namespace")[1].replace("{", " ").split()[0]
                    if any(word.startswith("DEFINE_ENUM") for word in line.split()) and not line.startswith("#define") and line.find("Error") != -1:
                        assert not inside, "found DEFINE_ENUM while reading previous"
                        inside = True
                        start = index
                        
                        tokens = line.split("DEFINE_ENUM")
                        assert len(tokens) == 2, "more than one DEFINE_ENUM at one line"
                        suffix = line.split("DEFINE_ENUM")[1]
                        balance = calculate_balance(suffix)
                        has_number = has_number or find_number(suffix)
                    elif inside:
                        balance += calculate_balance(line)
                        has_number = has_number or find_number(line)
                        if balance == 0:
                            if has_number:
                                handler(namespace, file, start, index)
                            inside = False
                            has_number = 0
                    index += 1

if __name__ == "__main__":
    main()
