"""downloading and uploading data to YT commands"""

import yt.logger as logger
from config import get_config
from common import require, chunk_iter_stream, chunk_iter_string, bool_to_string, parse_bool
from errors import YtError, YtResponseError
from http import get_api_version
from heavy_commands import make_write_request, make_read_request
from cypress_commands import remove, exists, set_attribute, mkdir, find_free_subpath, create, link, get_attribute
from table import to_table

from yt.yson import to_yson_type

import os
import hashlib
from functools import partial

def md5sum(filename):
    with open(filename, mode='rb') as fin:
        h = hashlib.md5()
        for buf in iter(partial(fin.read, 1024), b''):
            h.update(buf)
    return h.hexdigest()

def download_file(path, response_type=None, file_reader=None, offset=None, length=None, client=None):
    """Download file from path in Cypress to local machine.

    :param path: (string of `TablePath`) path to file in Cypress
    :param response_type: (string) Deprecated! It means the output format. By default it is line generator.
    :param file_reader: (dict) spec of download command
    :param offset: (int) offset in input file in bytes, 0 by default
    :param length: (int) length in bytes of desired part of input file, all file without offset by default
    :return: some stream over downloaded file, string generator by default
    """
    if response_type is not None:
        logger.info("Option response_type is deprecated and ignored")

    path = to_table(path, client=client)
    params = {"path": path.to_yson_type()}
    if file_reader is not None:
        params["file_reader"] = file_reader
    if length is not None:
        params["length"] = length
    if offset is not None:
        params["offset"] = offset

    def process_response(response):
        pass

    class RetriableState(object):
        def __init__(self):
            if offset is not None:
                self.offset = offset
            else:
                self.offset = 0
            self.length = length

        def prepare_params_for_retry(self):
            params["offset"] = self.offset
            if self.length is not None:
                params["length"] = self.length
            return params

        def iterate(self, response):
            for chunk in chunk_iter_stream(response, get_config(client)["read_buffer_size"]):
                yield chunk
                if self.offset is not None:
                    self.offset += len(chunk)
                if self.length is not None:
                    self.length -= len(chunk)

    command_name = "download" if get_api_version(client=client) == "v2" else "read_file"
    return make_read_request(
        command_name,
        path,
        params,
        process_response_action=process_response,
        retriable_state_class=RetriableState,
        client=client)

def upload_file(stream, destination, file_writer=None, client=None):
    """Upload file to destination path from stream on local machine.

    :param stream: some stream, string generator or 'yt.wrapper.string_iter_io.StringIterIO' for example
    :param destination: (string or `TablePath`) destination path in Cypress
    :param file_writer: (dict) spec of upload operation
    """
    # Read stream by chunks. Also it helps to correctly process StringIO from cStringIO (it has bug with default iteration)
    if hasattr(stream, "read"):
        # read files by chunks, not by lines
        stream = chunk_iter_stream(stream, get_config(client)["write_retries"]["chunk_size"])
    if isinstance(stream, basestring):
        stream = chunk_iter_string(stream, get_config(client)["write_retries"]["chunk_size"])

    params = {}
    if file_writer is not None:
        params["file_writer"] = file_writer

    enable_retries = get_config(client)["write_retries"]["enable"]
    if get_config(client)["write_file_as_one_chunk"]:
        if "file_writer" not in params:
            params["file_writer"] = {}
        params["file_writer"]["desired_chunk_size"] = 1024 ** 4
        enable_retries = False

    make_write_request(
        "upload" if get_api_version(client=client) == "v2" else "write_file",
        stream,
        destination,
        params,
        lambda path: create("file", path, ignore_existing=True, client=client),
        enable_retries,
        client=client)

def smart_upload_file(filename, destination=None, yt_filename=None, placement_strategy=None, ignore_set_attributes_error=True, client=None):
    """
    Upload file to destination path with custom placement strategy.

    :param filename: (string) path to file on local machine
    :param destination: (string) desired file path in Cypress,
    :param yt_filename: (string) 'file_name' attribute of file in Cypress (visible in operation name of file), \
    by default basename of `destination` (or `filename` if `destination` is not set)
    :param placement_strategy: (one of "replace", "ignore", "random", "hash"), \
    "hash" by default.
    :param ignore_set_attributes_error: (bool) ignore `YtResponseError` during attributes setting
    :return: YSON structure with result destination path

    'placement_strategy':

    * "replace" or "ignore" -> destination path will be 'destination' \
    or 'config["remote_temp_files_directory"]/<basename>' if destination is not specified

    * "random" (only for None `destination` param) -> destination path will be 'config["remote_temp_files_directory"]/<basename><random_suffix>'\

    * "hash" (only for None `destination` param) -> destination path will be 'config["remote_temp_files_directory"]/hash/<md5sum_of_file>' \
    or this path will be link to some random Cypress path
    """

    def upload_with_check(path):
        require(not exists(path, client=client),
                YtError("Cannot upload file to '{0}', node already exists".format(path)))
        upload_file(open(filename), path, client=client)

    require(os.path.isfile(filename),
            YtError("Upload: %s should be file" % filename))

    if placement_strategy is None:
        placement_strategy = "hash"
    require(placement_strategy in ["replace", "ignore", "random", "hash"],
            YtError("Incorrect file placement strategy " + placement_strategy))

    if destination is None:
        # create file storage dir and hash subdir
        mkdir(os.path.join(get_config(client)["remote_temp_files_directory"], "hash"), recursive=True, client=client)
        prefix = os.path.join(get_config(client)["remote_temp_files_directory"], os.path.basename(filename))
        destination = prefix
        if placement_strategy == "random":
            destination = find_free_subpath(prefix, client=client)
        if placement_strategy == "ignore" and exists(destination, client=client):
            return
        if yt_filename is None:
            yt_filename = os.path.basename(filename)
    else:
        if placement_strategy in ["hash", "random"]:
            raise YtError("Destination should not be specified if strategy is hash or random")
        mkdir(os.path.dirname(destination), recursive=True, client=client)
        if yt_filename is None:
            yt_filename = os.path.basename(destination)

    if placement_strategy == "replace":
        remove(destination, force=True, client=client)

    logger.debug("Uploading file '%s' with strategy '%s'", filename, placement_strategy)

    if placement_strategy == "hash":
        md5 = md5sum(filename)
        destination = os.path.join(get_config(client)["remote_temp_files_directory"], "hash", md5)
        link_exists = exists(destination + "&", client=client)

        # COMPAT(ignat): old versions of 0.14 have not support attribute broken
        try:
            broken = parse_bool(get_attribute(destination + "&", "broken", client=client))
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

    executable = os.access(filename, os.X_OK) or get_config(client)["yamr_mode"]["always_set_executable_flag_on_files"]

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
