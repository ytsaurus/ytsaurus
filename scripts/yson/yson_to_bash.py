import yson_parser
from optparse import OptionParser
from sys import stdin, stdout, stderr

options = None

def print_bash(obj, level):
    if not level:
        stdout.write(sentinel)
        return

    scalar_types = [int, float, str]
    if obj is None:
        stdout.write(options.none_literal)
    elif any(isinstance(obj, t) for t in scalar_types):
        stdout.write(str(obj))
    elif isinstance(obj, list):
        stdout.write(options.list_begin)
        first = True
        for item in obj:
            if not first:
                stdout.write(options.list_separator)
            print_bash(item, level + 1)
            first = False
        stdout.write(options.list_end)
    elif isinstance(obj, dict):
        stdout.write(options.map_begin)
        first = True
        for (key, value) in obj.iteritems():
            if not first:
                stdout.write(options.map_separator)
            print_bash(key, level + 1)
            stdout.write(options.map_key_value_separator)
            print_bash(value, level + 1)
            first = False
        stdout.write(options.map_end)
    else:
        raise Exception("Unknown type: %s" % type(obj))


if __name__ == "__main__":
    parser = OptionParser("Options")
    parser.add_option("--sentinel", default="")
    parser.add_option("--list_begin", default="")
    parser.add_option("--list_separator", default="\n")
    parser.add_option("--list_end", default="")
    parser.add_option("--none_literal", default="<None>")
    parser.add_option("--map_begin", default="")
    parser.add_option("--map_separator", default="\n")
    parser.add_option("--map_key_value_separator", default="\t")
    parser.add_option("--map_end", default="")
    parser.add_option("--print_depth", default=3)
    options, args = parser.parse_args()

    obj = yson_parser.parse(stdin)
    print_bash(obj, 3)
