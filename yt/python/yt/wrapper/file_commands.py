import yt.logger as logger
from .auth_commands import get_current_user
from .config import get_config, get_option
from .common import require, parse_bool, set_param, get_value, get_disk_size, MB, chunk_iter_stream, update
from .compression import try_enable_parallel_write_gzip
from .driver import get_command_list, make_formatted_request
from .errors import YtError, YtResponseError, YtCypressTransactionLockConflict
from .heavy_commands import make_write_request, make_read_request
from .cypress_commands import (remove, exists, set_attribute, mkdir, find_free_subpath,
                               create, link, get, set)
from .default_config import DEFAULT_WRITE_CHUNK_SIZE
from .parallel_reader import make_read_parallel_request
from .parallel_writer import make_parallel_write_request
from .retries import Retrier, default_chaos_monkey
from .transaction import Transaction
from .ypath import FilePath, ypath_join, ypath_dirname, ypath_split
from .local_mode import is_local_mode
from .stream import RawStream

from yt.common import to_native_str, YT_NULL_TRANSACTION_ID
from yt.yson.parser import YsonParser
from yt.yson import to_yson_type

import os
import hashlib

from io import BytesIO


# TODO(ignat): avoid copypaste (same function presented in py_wrapper.py)
def md5sum(filename):
    with open(filename, mode="rb") as fin:
        h = hashlib.md5()
        for buf in chunk_iter_stream(fin, 1024):
            h.update(buf)
    return h.hexdigest()


class LocalFile(object):
    """Represents a local path of a file and its path in job's sandbox"""
    def __init__(self, path, file_name=None, attributes=None):
        if isinstance(path, LocalFile):
            self._path = path.path
            self._file_name = path.file_name
            self._attributes = path.attributes
            if attributes is not None:
                self._attributes = update(self._attributes, attributes)
            if file_name is not None:
                self._attributes["file_name"] = file_name
            return

        # Hacky way to split string into file path and file path attributes.
        if hasattr(os, 'fspath'):
            path = os.fspath(path)
        path_bytes = path.encode("utf-8")

        stream = BytesIO(path_bytes)
        parser = YsonParser(
            stream,
            encoding="utf-8",
            always_create_attributes=True)

        path_attributes = {}
        if parser._has_attributes():
            path_attributes = parser._parse_attributes()
            path = to_native_str(stream.read())
        if attributes is None:
            attributes = path_attributes
        else:
            attributes = update(path_attributes, attributes)

        if file_name is not None:
            attributes["file_name"] = file_name
        if "file_name" not in attributes:
            attributes["file_name"] = os.path.basename(path)

        self._path = path
        self._attributes = attributes

    @property
    def path(self):
        return self._path

    @property
    def file_name(self):
        return self._attributes["file_name"]

    @property
    def attributes(self):
        return self._attributes


def _prepare_ranges_for_parallel_read(offset, length, data_size, data_size_per_thread):
    offset = get_value(offset, 0)
    offset = min(offset, data_size)

    length = get_value(length, data_size)
    length = min(length, data_size - offset)

    result = []
    while offset < data_size and length > 0:
        range_size = min(data_size_per_thread, length)
        result.append({"range" : (offset, range_size)})
        offset += range_size
        length -= range_size

    return result


def _prepare_params_for_parallel_read(params, range):
    params["offset"], params["length"] = range["range"][0], range["range"][1]
    return params


class _ReadFileRetriableState(object):
    def __init__(self, params, client, process_response_action=None):
        self.offset = params.get("offset", 0)
        self.length = params.get("length")
        self.client = client
        self.params = params

    def prepare_params_for_retry(self):
        self.params["offset"] = self.offset
        if self.length is not None:
            self.params["length"] = self.length
        return self.params

    def iterate(self, response):
        for chunk in chunk_iter_stream(response, get_config(self.client)["read_buffer_size"]):
            if self.offset is not None:
                self.offset += len(chunk)
            if self.length is not None:
                self.length -= len(chunk)
            yield chunk


def read_file(path, file_reader=None, offset=None, length=None, enable_read_parallel=None, client=None):
    """Downloads file from path in Cypress to local machine.

    :param path: path to file in Cypress.
    :type path: str or :class:`FilePath <yt.wrapper.ypath.FilePath>`
    :param dict file_reader: spec of download command.
    :param int offset: offset in input file in bytes, 0 by default.
    :param int length: length in bytes of desired part of input file, all file without offset by default.
    :return: some stream over downloaded file, string generator by default.
    """
    path = FilePath(path, client=client)
    params = {"path": path}
    set_param(params, "file_reader", file_reader)
    set_param(params, "length", length)
    set_param(params, "offset", offset)

    enable_read_parallel = get_value(enable_read_parallel, get_config(client)["read_parallel"]["enable"])

    if enable_read_parallel:
        data_size = get(path + "/@uncompressed_data_size", client=client)
        ranges = _prepare_ranges_for_parallel_read(
            offset,
            length,
            data_size,
            get_config(client)["read_parallel"]["data_size_per_thread"])
        return make_read_parallel_request(
            "read_file",
            path,
            ranges,
            params,
            _prepare_params_for_parallel_read,
            prepare_meta_func=None,
            max_thread_count=get_config(client)["read_parallel"]["max_thread_count"],
            unordered=False,
            response_parameters=None,
            client=client)

    return make_read_request(
        "read_file",
        path,
        params,
        process_response_action=lambda response: None,
        retriable_state_class=_ReadFileRetriableState,
        client=client,
        filename_hint=str(path),
        request_size=True)


def _get_upload_replication_factor(desired_replication_factor: int, client):
    # NB: In local mode we have only one node and default replication factor equal to one for all tables and files.
    if is_local_mode(client):
        replication_factor = 1
    else:
        max_replication_factor = get_config(client)["max_replication_factor"]
        if not max_replication_factor and desired_replication_factor < 3:
            raise YtError("File cache replication factor cannot be set less than 3")
        replication_factor = desired_replication_factor
        if max_replication_factor:
            replication_factor = min(replication_factor, int(max_replication_factor))

    return replication_factor


def write_file(destination, stream,
               file_writer=None, is_stream_compressed=False, force_create=None, compute_md5=False,
               size_hint=None, filename_hint=None, progress_monitor=None, client=None):
    """Uploads file to destination path from stream on local machine.

    :param destination: destination path in Cypress.
    :type destination: str or :class:`FilePath <yt.wrapper.ypath.FilePath>`
    :param stream: stream or bytes generator.
    :param dict file_writer: spec of upload operation.
    :param bool is_stream_compressed: expect stream to contain compressed data.
        This data can be passed directly to proxy without recompression. Be careful! this option
        disables write retries.
    :param bool force_create: unconditionally create file and ignores existing file.
    :param bool compute_md5: compute md5 of file content.
    """
    if force_create is None:
        force_create = True

    def prepare_file(path, client, attributes=None):
        if not force_create:
            return
        create("file", path, attributes=attributes, ignore_existing=True, client=client)

    chunk_size = get_config(client)["write_retries"]["chunk_size"]
    if chunk_size is None:
        chunk_size = DEFAULT_WRITE_CHUNK_SIZE
    else:
        file_writer = update({"desired_chunk_size": chunk_size}, get_value(file_writer, {}))

    stream = RawStream(stream, chunk_size)

    if filename_hint is None:
        filename_hint = stream.filename_hint
    if size_hint is None:
        size_hint = stream.size
    if size_hint is not None and size_hint <= 16 * MB:
        replication_factor = _get_upload_replication_factor(desired_replication_factor=3, client=client)
        if replication_factor >= 3:
            file_writer = update(
                {
                    "enable_early_finish": True,
                    "upload_replication_factor": 3,
                    "min_upload_replication_factor": 2,
                },
                get_value(file_writer, {}))

    params = {}
    set_param(params, "file_writer", file_writer)
    set_param(params, "compute_md5", compute_md5)

    enable_retries = get_config(client)["write_retries"]["enable"]
    is_one_small_blob = stream.size is not None and stream.size <= chunk_size
    if not is_one_small_blob and is_stream_compressed:
        enable_retries = False

    config_enable_parallel_write = get_config(client)["write_parallel"]["enable"]

    default_chunk_size = get_config(client)["write_retries"]["chunk_size"] or DEFAULT_WRITE_CHUNK_SIZE
    input_size_is_large = size_hint is not None and size_hint >= 2 * default_chunk_size
    enable_parallel_write = config_enable_parallel_write == to_yson_type(True) \
        or (config_enable_parallel_write is None and input_size_is_large)
    if enable_parallel_write and get_config(client)["proxy"]["content_encoding"] == "gzip":
        enable_parallel_write = try_enable_parallel_write_gzip(config_enable_parallel_write)

    if enable_parallel_write and not is_stream_compressed and not compute_md5:
        force_create = True
        make_parallel_write_request(
            "write_file",
            stream,
            destination,
            params,
            False,
            prepare_file,
            _get_remote_temp_files_directory(client=client),
            size_hint=size_hint,
            filename_hint=filename_hint,
            progress_monitor=progress_monitor,
            client=client)
    else:
        make_write_request(
            "write_file",
            stream,
            destination,
            params,
            prepare_file,
            enable_retries,
            is_stream_compressed=is_stream_compressed,
            size_hint=size_hint,
            filename_hint=filename_hint,
            progress_monitor=progress_monitor,
            client=client)


def _append_default_path_with_user_level(path, client=None):
    if path in ("//tmp/yt_wrapper/file_storage", "//tmp/yt_wrapper/table_storage"):
        current_user = get_current_user(client=client)
        if current_user:
            path = ypath_join(path, current_user["user"])
            with Transaction(transaction_id=YT_NULL_TRANSACTION_ID, client=client):
                create("map_node", path, ignore_existing=True, recursive=True, client=client)
    return path


def _get_remote_temp_files_directory(client=None):
    path = get_config(client)["remote_temp_files_directory"]
    if path is None:
        path = "//tmp/yt_wrapper/file_storage"
    return path


def _get_cache_path(client):
    return ypath_join(_get_remote_temp_files_directory(client), "new_cache")


class PutFileToCacheRetrier(Retrier):
    def __init__(self, params, client=None):
        retry_config = get_config(client)["proxy"]["retries"]
        timeout = get_config(client)["proxy"]["request_timeout"]
        retries_timeout = timeout[1] if isinstance(timeout, tuple) else timeout

        chaos_monkey_enable = get_option("_ENABLE_HTTP_CHAOS_MONKEY", client)
        super(PutFileToCacheRetrier, self).__init__(
            retry_config=retry_config,
            timeout=retries_timeout,
            exceptions=(YtCypressTransactionLockConflict,),
            chaos_monkey=default_chaos_monkey(chaos_monkey_enable))

        self._params = params
        self._client = client

    def action(self):
        return make_formatted_request(
            "put_file_to_cache",
            self._params,
            format=None,
            client=self._client)


def put_file_to_cache(path, md5, cache_path=None, client=None):
    """Puts file to cache

    :param str path: path to file in Cypress
    :param str md5: Expected MD5 hash of file
    :param str cache_path: Path to file cache
    :return: path to file in cache
    """
    cache_path = get_value(cache_path, _get_cache_path(client))
    create("map_node", cache_path, ignore_existing=True, recursive=True, client=client)

    params = {
        "path": path,
        "md5": md5,
        "cache_path": cache_path}

    retrier = PutFileToCacheRetrier(params, client)
    return retrier.run()


def get_file_from_cache(md5, cache_path=None, client=None):
    """Gets file path in cache

    :param str md5: MD5 hash of file
    :param str cache_path: Path to file cache
    :return: path to file in Cypress if it was found in cache and YsonEntity otherwise
    """
    cache_path = get_value(cache_path, _get_cache_path(client))
    params = {
        "md5": md5,
        "cache_path": cache_path}

    return make_formatted_request("get_file_from_cache", params, format=None, client=client)


def is_executable(filename, client=None):
    return os.access(filename, os.X_OK) or get_config(client)["yamr_mode"]["always_set_executable_flag_on_files"]


def _upload_file_to_cache_legacy(filename, hash, client=None):
    last_two_digits_of_hash = ("0" + hash.split("-")[-1])[-2:]

    hash_path = ypath_join(_get_remote_temp_files_directory(client), "hash")
    destination = ypath_join(hash_path, last_two_digits_of_hash, hash)

    attributes = None
    try:
        attributes = get(destination + "&/@", client=client)
    except YtResponseError as rsp:
        if not rsp.is_resolve_error():
            raise

    link_exists = False
    if attributes is not None:
        if attributes["type"] == "link":
            if parse_bool(attributes["broken"]):
                remove(destination + "&", client=client)
            else:
                link_exists = True
        else:
            remove(destination + "&", client=client)

    should_upload_file = not link_exists
    if link_exists:
        logger.debug("Link %s of file %s exists, skipping upload and set /@touched attribute", destination, filename)
        try:
            set(destination + "/@touched", True, client=client)
            set(destination + "&/@touched", True, client=client)
        except YtError as err:
            if err.is_resolve_error():
                should_upload_file = True
            elif not err.is_concurrent_transaction_lock_conflict():
                raise

    if should_upload_file:
        logger.debug("Link %s of file %s missing, uploading file", destination, filename)
        prefix = ypath_join(
            _get_remote_temp_files_directory(client),
            last_two_digits_of_hash,
            os.path.basename(filename))

        real_destination = find_free_subpath(prefix, client=client)
        attributes = {
            "hash": hash,
            "touched": True,
            "replication_factor": _get_upload_replication_factor(get_config(client)["file_cache"]["replication_factor"], client)
        }

        create("file",
               real_destination,
               recursive=True,
               attributes=attributes,
               client=client)
        write_file(real_destination, open(filename, "rb"), force_create=False, client=client)
        link(real_destination, destination, recursive=True, ignore_existing=True,
             attributes={"touched": True}, client=client)

    return destination


def upload_file_to_cache(filename, hash=None, progress_monitor=None, client=None):
    if hash is None:
        hash = md5sum(filename)

    use_legacy = get_config(client)["use_legacy_file_cache"]
    if use_legacy is None:
        use_legacy = \
            "put_file_to_cache" not in get_command_list(client) or \
            "get_file_from_cache" not in get_command_list(client)

    if use_legacy:
        return _upload_file_to_cache_legacy(filename, hash, client=client)

    file_path = get_file_from_cache(hash, client=client)
    if file_path:
        if progress_monitor is not None:
            progress_monitor.update(get_disk_size(filename, round=False))
            progress_monitor.finish("cached")
        return file_path

    temp_directory = _get_remote_temp_files_directory(client)
    if not temp_directory.endswith("/"):
        temp_directory = temp_directory + "/"
    real_destination = find_free_subpath(temp_directory, client=client)

    create("file",
           real_destination,
           recursive=True,
           attributes={"replication_factor": _get_upload_replication_factor(get_config(client)["file_cache"]["replication_factor"], client)},
           client=client)
    with open(filename, "rb") as stream:
        if progress_monitor is None:
            size_hint = get_disk_size(filename, round=False)
            filename_hint = filename
        else:
            size_hint = None
            filename_hint = None
        write_file(real_destination, stream, compute_md5=True, force_create=False,
                   size_hint=size_hint, filename_hint=filename_hint, progress_monitor=progress_monitor,
                   client=client)

    destination = put_file_to_cache(real_destination, hash, client=client)
    remove(real_destination, force=True, client=client)

    return destination


def _touch_file_in_cache(filepath, client=None):
    use_legacy = get_config(client)["use_legacy_file_cache"]
    if use_legacy is None:
        use_legacy = \
            "put_file_to_cache" not in get_command_list(client) or \
            "get_file_from_cache" not in get_command_list(client)

    if use_legacy:
        try:
            set(filepath + "&/@touched", True, client=client)
        except YtError as err:
            if not err.is_concurrent_transaction_lock_conflict():
                raise
    else:
        dirname, hash = ypath_split(filepath)
        get_file_from_cache(hash, client=client)


def smart_upload_file(filename, destination=None, yt_filename=None, placement_strategy=None,
                      ignore_set_attributes_error=True, hash=None, client=None):
    """Uploads file to destination path with custom placement strategy.

    :param str filename: path to file on local machine.
    :param str destination: desired file path in Cypress.
    :param str yt_filename: "file_name" attribute of file in Cypress (visible in operation name of file),
        by default basename of `destination` (or `filename` if `destination` is not set)
    :param str placement_strategy: one of ["replace", "ignore", "random", "hash"], "hash" by default.
    :param bool ignore_set_attributes_error: ignore :class:`YtResponseError <yt.wrapper.errors.YtResponseError>`
        during attributes setting.
    :return: YSON structure with result destination path

    `placement_strategy` can be set to:

    * "replace" or "ignore" -> destination path will be `destination`
        or ``yt.wrapper.config["remote_temp_files_directory"]/<basename>`` if destination is not specified.

    * "random" (only if `destination` parameter is `None`) -> destination path will be
        ``yt.wrapper.config["remote_temp_files_directory"]/<basename><random_suffix>``.

    * "hash" (only if `destination` parameter is `None`) -> destination path will be
        ``yt.wrapper.config["remote_temp_files_directory"]/hash/<md5sum_of_file>`` or this path will be link
        to some random Cypress path.
    """

    def upload_with_check(path):
        require(not exists(path, client=client),
                lambda: YtError("Cannot upload file to '{0}', node already exists".format(path)))
        write_file(path, open(filename, "rb"), client=client)

    require(os.path.isfile(filename),
            lambda: YtError("Upload: %s should be file" % filename))

    if placement_strategy is None:
        placement_strategy = "hash"
    require(placement_strategy in ["replace", "ignore", "random", "hash"],
            lambda: YtError("Incorrect file placement strategy " + placement_strategy))

    executable = os.access(filename, os.X_OK) or get_config(client)["yamr_mode"]["always_set_executable_flag_on_files"]

    if placement_strategy == "hash":
        if destination is not None:
            raise YtError("Option 'destination' can not be specified if strategy is 'hash'")
        if yt_filename is not None:
            raise YtError("Option 'yt_filename' can not be specified if strategy is 'hash'")
        yt_filename = os.path.basename(filename)
        destination = upload_file_to_cache(filename, hash, client=client)
    else:
        if destination is None:
            # create file storage dir and hash subdir
            mkdir(ypath_join(_get_remote_temp_files_directory(client), "hash"), recursive=True, client=client)
            prefix = ypath_join(_get_remote_temp_files_directory(client), os.path.basename(filename))
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
            mkdir(ypath_dirname(FilePath(destination, client=client)), recursive=True, client=client)
            if yt_filename is None:
                yt_filename = os.path.basename(destination)

        if placement_strategy == "replace":
            remove(destination, force=True, client=client)

        logger.debug("Uploading file '%s' with strategy '%s'", filename, placement_strategy)
        upload_with_check(destination)

        try:
            set_attribute(destination, "file_name", yt_filename, client=client)
            set_attribute(destination, "executable", executable, client=client)
        except YtResponseError as error:
            if error.is_concurrent_transaction_lock_conflict() and ignore_set_attributes_error:
                pass
            else:
                raise

    return to_yson_type(
        destination,
        {"file_name": yt_filename,
         "executable": executable})
