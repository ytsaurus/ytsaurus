import config
from common import require, YtError, add_quotes
from http import make_request
from tree_commands import remove, exists, set_attribute, set

import os

def download_file(path):
    content = make_request("GET", "download", {"path": path}, raw_response=True)
    return content.iter_lines()

def upload_file(filename, destination=None, replace=False):
    # TODO(ignat): add auto checking of files similarity by hash
    require(os.path.isfile(filename),
            YtError("Upload: %s should be file" % filename))
    if not exists(config.FILE_STORAGE):
        set(config.FILE_STORAGE, "{}")

    if destination is None:
        destination = os.path.join(config.FILE_STORAGE,
                                   add_quotes(os.path.basename(filename)))
    if replace or not exists(destination):
        if exists(destination):
            remove(destination)
        operation = make_request("PUT", "upload", dict(path=destination), data=open(filename))

        # Set executable flag if need
        if os.access(filename, os.X_OK):
            set_attribute(destination, "executable", add_quotes("true"))
    return destination



