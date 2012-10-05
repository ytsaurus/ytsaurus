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

def dirs(path):
    dirs = ["/"]
    for name in split_path(path):
        dirs.append(dirs[-1] + "/" + name)
    return dirs


