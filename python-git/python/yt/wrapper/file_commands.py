import config
from common import require, chunk_iter, partial, bool_to_string
from errors import YtError
from driver import read_content, get_host_for_heavy_operation
from tree_commands import remove, exists, set_attribute, mkdir, find_free_subpath, create
from transaction_commands import _make_transactional_request
from table import prepare_path

from yt.yson import convert_to_yson_type

import os
import hashlib

def md5sum(filename):
    with open(filename, mode='rb') as fin:
        h = hashlib.md5()
        for buf in iter(partial(fin.read, 1024), b''):
            h.update(buf)
    return h.hexdigest()

def download_file(path, response_type=None):
    """
    Downloads file from path.
    Response type means the output format. By default it is line generator.
    """
    if response_type is None: response_type = "iter_lines"
    response = _make_transactional_request("download", {"path": prepare_path(path)}, return_raw_response=True)
    return read_content(response, response_type)

def upload_file(stream, destination, yt_filename=None):
    """
    Simply uploads data from stream to destination and
    set file_name attribute if yt_filename is specified
    """
    if hasattr(stream, 'fileno'):
        # read files by chunks, not by lines
        stream = chunk_iter(stream)
    if config.CREATE_FILE_BEFORE_UPLOAD:
        create("file", destination, ignore_existing=True)
    _make_transactional_request("upload", {"path": prepare_path(destination)}, data=stream, proxy=get_host_for_heavy_operation())
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
        placement_strategy = config.FILE_PLACEMENT_STRATEGY
    require(placement_strategy in ["replace", "ignore", "random", "hash"],
            YtError("Incorrect file placement strategy " + placement_strategy))

    if destination is None:
        mkdir(config.FILE_STORAGE, recursive=True)
        destination = os.path.join(config.FILE_STORAGE,
                                   os.path.basename(filename))
        if placement_strategy == "random":
            destination = find_free_subpath(destination)
        if placement_strategy == "hash":
            destination = os.path.join(config.FILE_STORAGE, md5sum(filename))
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

    upload_file(open(filename), destination)
    set_attribute(destination, "file_name", yt_filename)

    executable = os.access(filename, os.X_OK) or config.ALWAYS_SET_EXECUTABLE_FLAG_TO_FILE
    return convert_to_yson_type(
        destination,
        {"file_name": yt_filename,
         "executable": bool_to_string(executable)})
