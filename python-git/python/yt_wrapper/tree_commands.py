from common import add_quotes, bool_to_string
from path_tools import escape_path, split_path, dirs
from http import make_request

import os
from itertools import imap, izip

def get(path, with_attributes=False, check_errors=True):
    return make_request("GET", "get",
                        dict(path=escape_path(path),
                             with_attributes=bool_to_string(with_attributes)),
                        check_errors=check_errors)

def set(path, value):
    return make_request("PUT", "set", dict(path=escape_path(path)), value)

def copy(source_path, destination_path):
    return make_request("GET", "copy",
                        dict(source_path=escape_path(source_path),
                             destination_path=escape_path(destination_path)))

def list(path):
    if not exists(path):
        # TODO(ignat):think about throwing exception here
        return []
    return make_request("GET", "list", {"path": escape_path(path)})

def exists(path):
    if path == "/":
        return True
    objects = get("/")
    cur_path = "/"
    for elem in split_path(path):
        if objects is None:
            objects = get(cur_path)
        if not isinstance(objects, dict) or elem not in objects:
            return False
        else:
            objects = objects[elem]
            cur_path = os.path.join(cur_path, elem)
    return True

def remove(path):
    if exists(path):
        return make_request("POST", "remove", {"path": escape_path(path)})
    # TODO(ignat):think about throwing exception here
    return None

def mkdir(path):
    create = False
    for dir in dirs(path):
        if not create and not exists(dir):
            create = True
        if create:
            set(dir, "{}")

def get_attribute(path, attribute, check_errors=True):
    return get("%s/@%s" % (path, attribute), check_errors=check_errors)

def set_attribute(path, attribute, value):
    return set("%s/@%s" % (path, attribute), value)

def list_attributes(path):
    return list(path + "/@")

