import config
from common import require, chunk_iter, partial, bool_to_string
from errors import YtError
from driver import read_content, get_host_for_heavy_operation
from heavy_commands import make_heavy_request
from tree_commands import remove, exists, set_attribute, mkdir, find_free_subpath, create, link
from transaction_commands import _make_transactional_request
from table import prepare_path

from yt.yson import to_yson_type

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
    response = _make_transactional_request(
        "download",
        {"path": prepare_path(path)},
        proxy=get_host_for_heavy_operation(),
        return_raw_response=True)
    return read_content(response, response_type)

def upload_file(stream, destination):
    """
    Simply uploads data from stream to destination and
    set file_name attribute if yt_filename is specified
    """
    if hasattr(stream, 'fileno'):
        # read files by chunks, not by lines
        stream = chunk_iter(stream, config.CHUNK_SIZE)

    def prepare_file(path):
        if config.API_VERSION == 2 and not exists(path):
            create("file", path, ignore_existing=True)

    make_heavy_request(
        "upload",
        stream,
        destination,
        {},
        prepare_file,
        config.USE_RETRIES_DURING_UPLOAD)

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

    def upload_with_check(path):
        require(not exists(path),
                YtError("Cannot upload file to '{0}', node already exists".format(path)))
        upload_file(open(filename), path)

    require(os.path.isfile(filename),
            YtError("Upload: %s should be file" % filename))

    if placement_strategy is None:
        placement_strategy = config.FILE_PLACEMENT_STRATEGY
    require(placement_strategy in ["replace", "ignore", "random", "hash"],
            YtError("Incorrect file placement strategy " + placement_strategy))

    if destination is None:
        # create file storage dir and hash subdir
        mkdir(os.path.join(config.FILE_STORAGE, "hash"), recursive=True)
        prefix = os.path.join(config.FILE_STORAGE, os.path.basename(filename))
        destination = prefix
        if placement_strategy == "random":
            destination = find_free_subpath(prefix)
        if placement_strategy == "replace" and exists(prefix):
            remove(destination)
        if placement_strategy == "ignore" and exists(destination):
            return
        if yt_filename is None:
            yt_filename = os.path.basename(filename)
    else:
        if placement_strategy in ["hash", "random"]:
            raise YtError("Destination should not be specified if strategy is hash or random")
        mkdir(os.path.dirname(destination))
        if yt_filename is None:
            yt_filename = os.path.basename(destination)

    if placement_strategy == "hash":
        md5 = md5sum(filename)
        destination = os.path.join(config.FILE_STORAGE, "hash", md5)
        if not exists(destination):
            real_destination = find_free_subpath(prefix)
            upload_with_check(real_destination)
            link(real_destination, destination, ignore_existing=True)
            set_attribute(real_destination, "hash", md5)
    else:
        upload_with_check(destination)

    set_attribute(destination, "file_name", yt_filename)

    executable = os.access(filename, os.X_OK) or config.ALWAYS_SET_EXECUTABLE_FLAG_TO_FILE
    set_attribute(destination, "executable", bool_to_string(executable))

    return to_yson_type(
        destination,
        {"file_name": yt_filename,
         "executable": bool_to_string(executable)})
