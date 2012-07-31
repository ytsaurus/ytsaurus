from common import require, YtError

def split_path(path):
    return filter(None, path.strip("/").split("/"))

def process_path(path, func):
    def add_slash(str): return "/" + str
    return "/" + "".join(map(add_slash, map(func, split_path(path))))

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
