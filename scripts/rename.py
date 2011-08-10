#!/usr/bin/python

"""
Small and nifty utility to perform supervised semi-automated renames over
source tree.
"""

# TODO: May be, more correct (atomic) work with temporary files in process_file()
# TODO: Make colorama support optional
import os
import sys
import re

import colorama
colorama.init()

VALID_EXTENSIONS = set(".cpp .h .proto".split())

def find_suitable_files(target_directory):
    """Recursively walks the target directory and yields full names of C++
    source and header files (*.cpp, *.h)"""
    for root, dirs, files in os.walk(target_directory):
        for file in files:
            if file.split(".")[-1] in VALID_EXTENSIONS:
                yield os.path.abspath(os.path.join(root, file))

def tokenize(string):
    """Tokenizes #string into a serie of tokens."""
    encode_char = lambda x: (1 * x.isalnum() + 2 * x.isspace())

    current_token = ""
    current_code = -1

    result = []

    for char in string:
        if current_code != -1 and current_code != encode_char(char):
            result.append(current_token)
            current_token = ""

        current_code = encode_char(char)
        current_token += char

    result.append(current_token)
    return result

def yield_with_context(iterable, n = 1):
    """Iterate over elements of #iterable with their context."""
    from collections import deque
    it = iter(iterable)
    context = deque([ None ] * n, maxlen = 2 * n + 1)

    for i in range(n):
        try:
            context.append(it.next())
        except StopIteration:
            context.append(None)

    while True:
        try:
            context.append(it.next())
        except StopIteration:
            context.append(None)

        if context[n] != None:
            yield context[n], (list(context)[:n], list(context)[-n:])
        else:
            break

def process_file(file, pattern, replacement):
    """Processes one file."""
    os.rename(file, file + ".__")
    old_handle = open(file + ".__", "r")
    new_handle = open(file, "w")

    iterable = old_handle.readlines()
    iterable = enumerate(yield_with_context(iterable))

    try:
        for n, line_with_context in iterable:
            old_line, context = line_with_context
            new_line, number = re.subn(pattern, replacement, old_line)

            need_to_overwrite = False

            if number > 0:
                pprint_header(file, 1 + n)
                pprint_line_difference_with_context(old_line, new_line, context)
                need_to_overwrite = query_yes_no("Accept this change?")

            if need_to_overwrite:
                new_handle.write(new_line)
            else:
                new_handle.write(old_line)

        os.remove(file + ".__")
    except KeyboardInterrupt:
        os.remove(file)
        os.rename(file + ".__", file)

def query_yes_no(question, default = "yes"):
    """Ask a yes/no question via raw_input() and return their answer."""
    valid = { "yes" : True, "y" : True, "ye" : True, "no" : False, "n":False }
    if default == None:
        prompt = " [y/n] "
    elif default == "yes":
        prompt = " [Y/n] "
    elif default == "no":
        prompt = " [y/N] "
    else:
        raise ValueError("Invalid default answer: '%s'" % default)

    while True:
        sys.stdout.write(question + prompt)
        choice = raw_input().lower()
        if default is not None and choice == '':
            return valid[default]
        elif choice in valid:
            return valid[choice]
        else:
            sys.stdout.write("Please respond with 'yes' or 'no' (or 'y' or 'n').\n")

def pprint_header(file, line):
    sys.stdout.write(colorama.Style.BRIGHT)
    sys.stdout.write("==== {file}:{line}\n".format(file = file, line = line))
    sys.stdout.write(colorama.Style.RESET_ALL)

def pprint_line_difference_with_context(old_line, new_line, context):
    sys.stdout.write("\n".join(" " + x for x in context[0]))
    pprint_line_difference(old_line, new_line)
    sys.stdout.write("\n".join(" " + x for x in context[1]))
    sys.stdout.write("\n")

def pprint_line_difference(old_line, new_line):
    """Pretty-prints difference between two lines."""
    from difflib import SequenceMatcher
    old_line = tokenize(old_line)
    new_line = tokenize(new_line)

    matcher = SequenceMatcher(None, old_line, new_line)

    diff_with_old = colorama.Fore.RESET
    diff_with_new = colorama.Fore.RESET

    wrappers = dict([
        [ "replace", lambda x: colorama.Fore.YELLOW + "".join(x) ],
        [ "delete",  lambda x: colorama.Fore.RED    + "".join(x) ],
        [ "insert",  lambda x: colorama.Fore.GREEN  + "".join(x) ],
        [ "equal",   lambda x: colorama.Fore.RESET  + "".join(x) ]
    ])

    for tag, i1, i2, j1, j2 in matcher.get_opcodes():
        diff_with_old += wrappers[tag](old_line[i1:i2])
        diff_with_new += wrappers[tag](new_line[j1:j2])

    sys.stdout.write(colorama.Fore.RED)
    sys.stdout.write('-')
    sys.stdout.write(diff_with_old)
    sys.stdout.write(colorama.Fore.GREEN)
    sys.stdout.write('+')
    sys.stdout.write(diff_with_new)
    sys.stdout.write(colorama.Style.RESET_ALL)

def main():
    from optparse import OptionParser
    from pprint import pprint

    parser = OptionParser()
    parser.add_option("", "--target", help = "Target directory")
    parser.add_option("-p", "--pattern", help = "Pattern")
    parser.add_option("-r", "--replacement", help = "Replacement")
    (options, args) = parser.parse_args()

    if not options.target:
        parser.error("Target directory is missing.")
    if not options.pattern:
        parser.error("Pattern is missing.")
    if not options.replacement:
        parser.error("Replacement is missing.")

    files = find_suitable_files(options.target)
    for file in files:
        process_file(file, options.pattern, options.replacement)

if __name__ == "__main__":
    main()
