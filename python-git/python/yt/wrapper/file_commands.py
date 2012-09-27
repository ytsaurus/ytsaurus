import config
from common import require, YtError
from path_tools import escape_path, escape_name
from http import make_request
from tree_commands import remove, exists, set_attribute, mkdir, find_free_subpath, set

import os
from itertools import imap

def download_file(path):
    def add_eoln(str):
        return str + "\n"
    content = make_request("GET", "download",
            {"path": escape_path(path),
             "transaction_id": config.TRANSACTION}, raw_response=True)
    return imap(add_eoln, content.iter_lines())

def upload_file(filename, yt_filename=None, destination=None, placement_strategy=None):
    """
    Upload file to specified path.
    If destination is not specified, than name is determined by placement strategy.
    If placement_strategy equals "replace" or "ignore", then destination is set up
    'config.FILE_STORAGE/basename'. In "random" case (default) destination is set up
    'config.FILE_STORAGE/basename<random_suffix>'
    If yt_filename is specified than set it as ytable system name
    (name that would be visible in operations).
    """
    require(os.path.isfile(filename),
            YtError("Upload: %s should be file" % filename))

    if placement_strategy is None: 
        placement_strategy = "random"
    require(placement_strategy in ["replace", "ignore", "random"],
            YtError("Incorrect file placement strategy " + placement_strategy))
    
    basename = os.path.basename(filename)
    if yt_filename is None:
        yt_filename = basename
    if destination is None:
        mkdir(config.FILE_STORAGE)
        destination = os.path.join(config.FILE_STORAGE, basename)
        if placement_strategy == "random":
            destination = find_free_subpath(destination)
        if placement_strategy == "replace" and exists(destination):
            remove(destination)
        if placement_strategy == "ignore" and exists(destination):
            return
    else:
        dirname = os.path.dirname(destination)
        if not exists(dirname):
            set(dirname, "{}")
        yt_filename = os.path.basename(destination)
    
    make_request(
            "PUT", "upload", 
            {"path": escape_path(destination),
             "transaction_id": config.TRANSACTION},
            data=open(filename))
    set_attribute(destination, "file_name", escape_name(yt_filename))
    # Set executable flag if need
    if os.access(filename, os.X_OK):
        set_attribute(destination, "executable", escape_name("true"))
    return destination

