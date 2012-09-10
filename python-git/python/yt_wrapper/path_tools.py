from common import require, YtError

def split_table_ranges(path):
    #! It is stupid and work correctly only in simple cases
    path = path.strip()
    used = set()
    ranges = ""
    range_brackets = [("[", "]"), ("{", "}")]
    for iter in xrange(2):
        for opening, closing in range_brackets:
            if closing in used: continue
            if path[-1] != closing: continue
            opening_pos = path.rfind(opening)
            if opening_pos == -1: continue
            ranges = path[opening_pos:] + ranges
            path = path[:opening_pos]
            used.add(closing)
    return path, ranges

def split_path(path):
    return filter(None, path.strip("/").split("/"))

def process_path(path, func):
    def add_slash(str): return "/" + str
    base_part, ranges_part = split_table_ranges(path)
    return "/" + "".join(map(add_slash, map(func, split_path(base_part)))) + ranges_part

SPECIAL_SYMBOLS = "*\"=:; "

def escape_name(name, process_attribute=True):
    require(name.find("\\") == -1, YtError("Using \\ in table names is forbidden, name " + name))

    if name.startswith("@"):
        return "@" + escape_name(name[1:], process_attribute=False)

    for sym in SPECIAL_SYMBOLS:
        name = name.replace(sym, "\\" + sym)
    if name:
        name = '"%s"' % name
    return name

def escape_path(path):
    return process_path(path, escape_name)

def dirs(path):
    dirs = ["/"]
    for name in split_path(path):
        dirs.append(dirs[-1] + "/" + name)
    return dirs


