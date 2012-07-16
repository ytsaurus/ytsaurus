import config
from common import require, YtError, add_quotes
from http import make_request
from tree_commands import remove, exists, set_attribute, set

import os

def download_file(path):
    return make_request("GET", "download", {"path": path})

def upload_file(file, destination=None, replace=False):
    # TODO(ignat): add auto checking of files similarity 
    require(os.path.isfile(file),
            YtError("Upload: %s should be file" % file))
    if not exists(config.FILE_STORAGE):
        set(config.FILE_STORAGE, "{}")

    if destination is None:
        destination = os.path.join(config.FILE_STORAGE,
                                   add_quotes(os.path.basename(file)))
    if replace or not exists(destination):
        if exists(destination):
            remove(destination)
        # TODO(ignat): open(file).read() holds file in RAM. It is bad for huge files. Add buffering here.
        operation = make_request("PUT", "upload", dict(path=destination), open(file).read())

        # Set executable flag if need
        if os.access(file, os.X_OK):
            set_attribute(destination, "executable", add_quotes("true"))
    return destination



