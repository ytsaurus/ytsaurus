import config
import yt.logger as logger
from common import require, chunk_iter, partial, bool_to_string, parse_bool
from errors import YtError, YtResponseError
from driver import read_content, get_host_for_heavy_operation
from heavy_commands import make_heavy_request
from tree_commands import remove, exists, set_attribute, mkdir, find_free_subpath, create, link, get_attribute
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

def download_file(path, response_type=None, file_reader=None, offset=None, length=None, client=None):
    """
    Downloads file from path.
    Response type means the output format. By default it is line generator.
    """
    if response_type is None: response_type = "iter_lines"
    
    params = {"path": prepare_path(path)}
    if file_reader is not None:
        params["file_reader"] = file_reader
    if offset is not None:
        params["offset"] = offset
    if length is not None:
        params["length"] = length

    response = _make_transactional_request(
        "download",
        params,
        proxy=get_host_for_heavy_operation(client=client),
        return_raw_response=True)
    return read_content(response, raw=True, format=None, response_type=response_type)

def upload_file(stream, destination, file_writer=None, client=None):
    """
    Simply uploads data from stream to destination and
    set file_name attribute if yt_filename is specified
    """
    if hasattr(stream, 'fileno'):
        # read files by chunks, not by lines
        stream = chunk_iter(stream, config.CHUNK_SIZE)

    params = {}
    if file_writer is not None:
        params["file_writer"] = file_writer

    make_heavy_request(
        "upload",
        stream,
        destination,
        params,
        lambda path: create("file", path, ignore_existing=True),
        config.USE_RETRIES_DURING_UPLOAD,
        client=client)

def smart_upload_file(filename, destination=None, yt_filename=None, placement_strategy=None, ignore_set_attributes_error=True, client=None):
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
        upload_file(open(filename), path, client=client)

    require(os.path.isfile(filename),
            YtError("Upload: %s should be file" % filename))

    if placement_strategy is None:
        placement_strategy = config.FILE_PLACEMENT_STRATEGY
    require(placement_strategy in ["replace", "ignore", "random", "hash"],
            YtError("Incorrect file placement strategy " + placement_strategy))

    if destination is None:
        # create file storage dir and hash subdir
        mkdir(os.path.join(config.FILE_STORAGE, "hash"), recursive=True, client=client)
        prefix = os.path.join(config.FILE_STORAGE, os.path.basename(filename))
        destination = prefix
        if placement_strategy == "random":
            destination = find_free_subpath(prefix, client=client)
        if placement_strategy == "replace" and exists(prefix, client=client):
            remove(destination, client=client)
        if placement_strategy == "ignore" and exists(destination, client=client):
            return
        if yt_filename is None:
            yt_filename = os.path.basename(filename)
    else:
        if placement_strategy in ["hash", "random"]:
            raise YtError("Destination should not be specified if strategy is hash or random")
        mkdir(os.path.dirname(destination), recursive=True)
        if yt_filename is None:
            yt_filename = os.path.basename(destination)

    logger.debug("Uploading file '%s' with strategy '%s'", filename, placement_strategy)

    if placement_strategy == "hash":
        md5 = md5sum(filename)
        destination = os.path.join(config.FILE_STORAGE, "hash", md5)
        link_exists = exists(destination + "&")

        # COMPAT(ignat): old versions of 0.14 have not support attribute broken
        try:
            broken = parse_bool(get_attribute(destination + "&", "broken"))
        except YtResponseError as rsp:
            if not rsp.is_resolve_error():
                raise
            broken = False

        if link_exists and broken:
            logger.debug("Link '%s' of file '%s' exists but is broken", destination, filename)
            remove(destination, client=client)
            link_exists = False
        if not link_exists:
            real_destination = find_free_subpath(prefix, client=client)
            upload_with_check(real_destination)
            link(real_destination, destination, ignore_existing=True, client=client)
            set_attribute(real_destination, "hash", md5, client=client)
        else:
            logger.debug("Link '%s' of file '%s' exists, skipping upload", destination, filename)
    else:
        upload_with_check(destination)

    executable = os.access(filename, os.X_OK) or config.ALWAYS_SET_EXECUTABLE_FLAG_TO_FILE
    
    try:
        set_attribute(destination, "file_name", yt_filename, client=client)
        set_attribute(destination, "executable", bool_to_string(executable), client=client)
    except YtResponseError as error:
        if error.is_concurrent_transaction_lock_conflict() and ignore_set_attributes_error:
            pass
        else:
            raise

    return to_yson_type(
        destination,
        {"file_name": yt_filename,
         "executable": bool_to_string(executable)})
