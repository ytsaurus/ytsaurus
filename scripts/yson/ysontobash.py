import sys
import yson_parser
from sys import stdout

max_level = 3
sentinel = ''

list_begin = ''
list_separator = '\n'
list_end = ''

map_begin = ''
map_separator = '\n'
map_key_value_separator = '\t'
map_end = ''

def print_bash(obj, level=1):
    if level > max_level:
        if sentinel: stdout.write(sentinel)
        return

    if isinstance(obj, int):
        stdout.write(str(obj))
    elif isinstance(obj, float):
        stdout.write(str(obj))
    elif isinstance(obj, str):
        stdout.write(obj)
    elif isinstance(obj, list):
        if list_begin: stdout.write(list_begin)
        first = True
        for item in obj:
            if not first:
                if list_separator: stdout.write(list_separator)
            print_bash(item, level + 1)
            first = False
        if list_end: stdout.write(list_end)
    elif isinstance(obj, dict):
        if map_begin: stdout.write(map_begin)
        first = True
        for (key, value) in obj.iteritems():
            if not first:
                if map_separator: stdout.write(map_separator)
            print_bash(key, level + 1)
            if map_key_value_separator: stdout.write(map_key_value_separator)
            print_bash(value, level + 1)
            first = False
        if map_end: stdout.write(map_end)
    else:
        print "Unknown type:", type(obj)

if __name__ == "__main__":
    obj = yson_parser.parse(sys.stdin)
    print obj
    print_bash(obj)
    stdout.flush()
    print "printed"
