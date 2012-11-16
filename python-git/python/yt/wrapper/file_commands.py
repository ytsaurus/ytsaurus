import config
from common import require, YtError
from http import make_request, read_content
from tree_commands import remove, exists, set_attribute, mkdir, find_free_subpath
from transaction_commands import add_transaction_params

import os

def download_file(path, response_type=None):
    if response_type is None: response_type = "iter_lines"
    response = make_request(
        "download",
        add_transaction_params({
            "path": path
        }),
        raw_response=True)
    return read_content(response, response_type)

def upload_file(stream, destination, yt_filename=None):
    """ Simply uploads data from stream to destination and
        set file_name attribute if yt_filename is specified"""
    mkdir(os.path.dirname(destination))
    make_request(
        "upload",
        add_transaction_params({
            "path": destination
        }),
        data=stream)
    if yt_filename is not None:
        set_attribute(destination, "file_name", yt_filename)

def smart_upload_file(filename, destination=None, yt_filename=None, placement_strategy=None):
    """
    Upload file specified by filename to destination path.
    If destination is not specified, than name is determined by placement strategy.
    If placement_strategy equals "replace" or "ignore", then destination is set up
    'config.FILE_STORAGE/basename'. In "random" case (default) destination is set up
    'config.FILE_STORAGE/basename<random_suffix>'
    If yt_filename is specified than file_name attribrute is set up
    (name that would be visible in operations).
    """
    require(os.path.isfile(filename),
            YtError("Upload: %s should be file" % filename))

    if placement_strategy is None:
        placement_strategy = "random"
    require(placement_strategy in ["replace", "ignore", "random"],
            YtError("Incorrect file placement strategy " + placement_strategy))

    if destination is None:
        mkdir(config.FILE_STORAGE)
        destination = os.path.join(config.FILE_STORAGE,
                                   os.path.basename(filename))
        if placement_strategy == "random":
            destination = find_free_subpath(destination)
        if placement_strategy == "replace" and exists(destination):
            remove(destination)
        if placement_strategy == "ignore" and exists(destination):
            return
        if yt_filename is None:
            yt_filename = os.path.basename(filename)
    else:
        mkdir(os.path.dirname(destination))
        if yt_filename is None:
            yt_filename = os.path.basename(destination)

    upload_file(open(filename), destination, yt_filename)

    # Set executable flag if need
    if os.access(filename, os.X_OK) or config.ALWAYS_SET_EXECUTABLE_FLAG_TO_FILE:
        set_attribute(destination, "executable", "true")
    return destination

