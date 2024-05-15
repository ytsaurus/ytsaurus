# -*- coding: utf-8 -*-

"""mount-cypress -- mount a Cypress, an YT cluster metainformation tree.

Usage:
  mount-cypress <proxy> <mount_point>
  mount-cypress (-h | --help)

Arguments:
  <proxy>        Proxy alias like <cluster_host> or just <cluster_name>.
  <mount_point>  Mountpoint directory like "/mnt/<cluster_name>".

Options:
  -h, --help    Show this help.

"""

import yt.wrapper.client

try:
    from expiringdict import ExpiringDict
except ImportError:
    from yt.packages.expiringdict import ExpiringDict

import yt.packages.fuse as fuse
import yt.packages.requests as requests

import stat
import errno
import time
import logging
import functools
import collections
import os

LOGGER_NAME = "CypressFuse"
LOGGER = logging.getLogger(LOGGER_NAME)
BASIC_FORMATTER = logging.Formatter(
    fmt="%(name)s\t%(asctime)s.%(msecs)03d\t%(message)s",
    datefmt="%H:%M:%S")

if not LOGGER.handlers:
    LOGGER.addHandler(logging.StreamHandler())
LOGGER.handlers[0].setFormatter(BASIC_FORMATTER)


class Statistics(object):
    """Structure to collect execution timings."""

    def __init__(self, logger):
        self._timings = collections.defaultdict(float)
        self._calls = collections.defaultdict(int)
        self._logger = logger

    def update(self, name, time):
        self._timings[name] += time
        self._calls[name] += 1

    def report(self):
        self._logger.debug("Statistics:")
        for name in self._timings.keys():
            duration = self._timings[name]
            calls = self._calls[name]
            self._logger.debug("{0}: {1}, {2}".format(name, duration, calls))


class Timer(object):
    def __init__(self, name, statistics):
        self._name = name
        self._statistics = statistics

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()
        self.interval = self.end - self.start
        self._statistics.update(self._name, self.interval)


def log_calls(logger, message_format, statistics):
    """Creates a decorator for logging each wrapped function call.

    :param str message_format: an old-style format string.

    Items with names corresponding to function's arguments are allowed.
    A special key "__name__" corresponds to the wrapped function's name.
    """
    def get_logged_version(function):
        positional_names = function.__code__.co_varnames

        def log_call(*args, **kwargs):
            kwargs.update(zip(positional_names, args))
            kwargs["__name__"] = function.__name__
            logger.debug(message_format, kwargs)

        @functools.wraps(function)
        def logged_function(*args, **kwargs):
            log_call(*args, **kwargs)
            with Timer(function.__name__, statistics):
                return function(*args, **kwargs)

        return logged_function

    return get_logged_version


def handle_yt_errors(logger):
    """Modifies the function so it raises FuseOSError instead of :class:`YtError <yt.common.YtError>`."""
    def get_logged_version(function):

        @functools.wraps(function)
        def cautious_function(*args, **kwargs):
            try:
                return function(*args, **kwargs)
            except yt.wrapper.YtResponseError as error:
                if not error.is_resolve_error():
                    logger.exception("Exception caught in " + function.__name__)
                raise fuse.FuseOSError(errno.ENOENT)
            except requests.ConnectionError:
                logger.exception("Exception caught in " + function.__name__)
                raise fuse.FuseOSError(errno.EAGAIN)

        return cautious_function

    return get_logged_version


class CachedYtClient(yt.wrapper.client.Yt):
    """YT client which caches nodes and their attributes for some time."""

    _logger = logging.getLogger(LOGGER_NAME + ".CachedYtClient")
    _logger.setLevel(level=logging.DEBUG)

    _statistics = Statistics(_logger)

    class CacheEntry(object):
        def __init__(self, exists=None, error=None):
            # Assume that node exists to avoid false negatives.
            self.exists = exists
            self.attributes = {}
            self.children = None
            self.error = error

    def __init__(self, max_cache_size=16384, max_age_seconds=2, **kwargs):
        """Initialize the client.

        :param int max_cache_size: maximum number of cached nodes.
        :param int max_age_seconds: after this period the node is removed from cache.

        The rest of the arguments are passed to the parent constructor.
        """
        super(CachedYtClient, self).__init__(**kwargs)

        self._cache = ExpiringDict(
            max_len=max_cache_size,
            max_age_seconds=max_age_seconds
        )
        # Keys are paths of nodes and node attributes. Each value is either
        # (False, e), meaning that there is no such node/attribute, *e* being
        # an exception which should be raised on access attempt, or
        # (True, x), *x* being the list of children/the attribute value.

    @staticmethod
    def _attribute_error(attribute):
        error = yt.wrapper.YtError(message="No such attribute: " + attribute, code=500)
        return yt.wrapper.YtResponseError(error.simplify())

    @staticmethod
    def _node_error(path):
        error = yt.wrapper.YtError(message="No such node: " + path, code=500)
        return yt.wrapper.YtResponseError(error.simplify())

    @log_calls(_logger, "%(__name__)s(%(path)r)", _statistics)
    def get_attributes(self, path, attributes, use_list_optimization=True):
        """Get a subset of node's attributes."""
        # Firstly, check whether we are sure the node doesn't exist at all.
        cache_entry = self._cache.get(path, CachedYtClient.CacheEntry(exists=True))
        if not cache_entry.exists:
            raise cache_entry.error

        # Secondly, check whether all requested attributes are cached.
        try:
            cache_slice = ((a, cache_entry.attributes[a]) for a in attributes)
            return dict((a, v) for a, (f, v) in cache_slice if f)
        except KeyError:
            pass

        # Check parent children list
        parent_path = yt.wrapper.ypath_dirname(path)
        if path != parent_path:
            parent_cache_entry = self._cache.get(parent_path)
            if parent_cache_entry is not None \
                    and parent_cache_entry.children is not None \
                    and os.path.basename(path) not in parent_cache_entry.children:
                error = CachedYtClient._node_error(path)
                self._cache[path] = CachedYtClient.CacheEntry(exists=False, error=error)
                raise error

            if use_list_optimization:
                self.list(parent_path, attributes=attributes)
                return self.get_attributes(path, attributes, use_list_optimization=False)

        # Finally, fetch all node's attributes.
        self._logger.debug("\tmiss")
        try:
            all_attributes = super(CachedYtClient, self).get(path + "/@")
        except yt.wrapper.YtResponseError as error:
            if error.is_resolve_error():
                self._cache[path] = CachedYtClient.CacheEntry(exists=False, error=error)
            raise
        cache_entry.attributes.update(
            (a, (True, v)) for a, v in all_attributes.items()
        )

        requested_attributes = {}
        for attribute in attributes:
            if attribute in all_attributes:
                requested_attributes[attribute] = all_attributes[attribute]
            else:
                cache_entry.attributes[attribute] = (
                    False, CachedYtClient._attribute_error(attribute)
                )

        self._cache[path] = cache_entry
        return requested_attributes

    @log_calls(_logger, "%(__name__)s(%(path)r)", _statistics)
    def list(self, path, attributes=None):
        """Gets children of a node specified by a ypath."""
        cache_entry = self._cache.get(path, CachedYtClient.CacheEntry(exists=True))
        if not cache_entry.exists:
            raise cache_entry.error
        if cache_entry.children is not None and attributes is None:
            return cache_entry.children

        if attributes is None:
            attributes = []

        children = super(CachedYtClient, self).list(
            path, attributes=attributes
        )
        cache_entry.children = set(map(str, children))

        for child in children:
            child_path = path + "/" + child
            child_cache_entry = self._cache.get(child_path, CachedYtClient.CacheEntry(exists=True))
            for attribute in attributes:
                if attribute in child.attributes:
                    child_value = (True, child.attributes[attribute])
                else:
                    child_value = (False, CachedYtClient._attribute_error(attribute))
                child_cache_entry.attributes[attribute] = child_value
            self._cache[child_path] = child_cache_entry

        self._cache[path] = cache_entry
        return children

    @log_calls(_logger, "%(__name__)s(%(path)r)", _statistics)
    def create(self, type, path=None, recursive=False, ignore_existing=False, attributes=None):
        super(CachedYtClient, self).create(
            type, path=path, recursive=recursive,
            ignore_existing=ignore_existing, attributes=attributes
        )
        self._cache.pop(path)
        parent = self._cache.get(yt.wrapper.ypath_dirname(path))
        if parent is not None and parent.children is not None:
            parent.children.add(os.path.basename(path))

    @log_calls(_logger, "%(__name__)s(%(path)r)", _statistics)
    def remove(self, path, recursive=False, force=False):
        super(CachedYtClient, self).remove(
            path, recursive=recursive, force=force
        )
        self._cache.pop(path)
        parent = self._cache.get(yt.wrapper.ypath_dirname(path))
        if parent is not None and parent.children is not None:
            parent.children.remove(os.path.basename(path))

    @log_calls(_logger, "%(__name__)s(%(path)r)", _statistics)
    def create_transaction_and_take_snapshot_lock(self, path):
        title = "FUSE: read {0}".format(yt.wrapper.TablePath(path, client=self))
        tx = self.Transaction(attributes={"title": title})
        with self.Transaction(transaction_id=tx.transaction_id):
            self.lock(path, mode="snapshot")
        return tx


class OpenedFile(object):
    """Stores information and cache for currently opened regular file."""

    _logger = logging.getLogger(LOGGER_NAME + ".OpenedFile")
    _logger.setLevel(level=logging.DEBUG)

    def __init__(self, client, ypath, attributes, minimum_read_size):
        """Sets up cache.

        :param int minimum_read_size: the minimum number of bytes to read from the cluster at once.
        """
        self.ypath = ypath
        self.attributes = attributes

        self._client = client
        self._minimum_read_size = minimum_read_size
        self._length = 0
        self._offset = 0
        self._buffer = b""
        self._has_pending_write = False
        self._tx = client.create_transaction_and_take_snapshot_lock(ypath)

    def read(self, length, offset):
        # Read file from memory
        if self._has_pending_write:
            return self._buffer[offset:offset + length]

        if offset < self._offset \
                or offset + length > self._offset + self._length:
            self._logger.debug("\tmiss")
            self._length = max(length, self._minimum_read_size)
            if offset < self._offset:
                self._offset = max(offset + length - self._length, 0)
            else:
                self._offset = offset
            with self._client.Transaction(transaction_id=self._tx.transaction_id, attributes={"title": "Fuse read transaction"}):
                self._buffer = self._client.read_file(
                    self.ypath,
                    length=self._length, offset=self._offset
                ).read()

        assert self._offset <= offset
        assert self._offset + self._length >= offset + length

        buffer_offset = offset - self._offset
        return self._buffer[buffer_offset:(buffer_offset + length)]

    def _read_all(self):
        with self._client.Transaction(transaction_id=self._tx.transaction_id, attributes={"title": "Fuse read transaction"}):
            self._buffer = self._client.read_file(self.ypath).read()
            self._offset = 0
            self._length = len(self._buffer)

    def _extend_buffer(self, length):
        if len(self._buffer) < length:
            self._buffer += b"\0" * (length - len(self._buffer))

    def truncate(self, length):
        if not self._has_pending_write:
            self._read_all()
        self._has_pending_write = True

        self._extend_buffer(length)
        self._buffer = self._buffer[:length]
        self._length = len(self._buffer)

    def write(self, data, offset):
        if not self._has_pending_write:
            self._read_all()
        self._has_pending_write = True

        self._extend_buffer(offset + len(data))
        self._buffer = self._buffer[:offset] + data + self._buffer[offset + len(data):]
        self._length = len(self._buffer)
        return len(data)

    def flush(self):
        if self._has_pending_write:
            self._client.write_file(self.ypath, self._buffer)
            self._buffer = b""
            self._length = 0
            self._has_pending_write = False

    def close(self):
        self._tx.abort()


class OpenedTable(object):
    """Stores information and cache for currently opened table."""

    _logger = logging.getLogger(LOGGER_NAME + ".OpenedTable")
    _logger.setLevel(level=logging.DEBUG)

    def __init__(self, client, ypath, attributes, format, minimum_read_row_count):
        """Set up cache.

        :param int minimum_table_read_row_count: the minimum number of rows to read \
        from the cluster at once.
        """
        self.ypath = ypath
        self.attributes = attributes

        self._client = client
        self._format = format
        self._minimum_read_row_count = minimum_read_row_count
        self._lower_offset = self._upper_offset = 0
        self._lower_row = self._upper_row = 0
        self._buffer = []
        self._tx = client.create_transaction_and_take_snapshot_lock(ypath)

    def read(self, length, offset):
        while self._upper_offset < offset + length:
            next_upper_row = self._upper_row + self._minimum_read_row_count
            slice_ypath = yt.wrapper.TablePath(
                self.ypath,
                start_index=self._upper_row,
                end_index=next_upper_row,
                client=self._client
            )
            with self._client.Transaction(transaction_id=self._tx.transaction_id, attributes={"title": "Fuse read transaction"}):
                slice_content = b"".join(
                    self._client.read_table(slice_ypath, format=self._format, raw=True)
                )
            if len(slice_content) == 0:
                break
            self._upper_offset += len(slice_content)
            self._upper_row += self._minimum_read_row_count
            self._buffer += [slice_content]

        while self._lower_offset > offset:
            next_lower_row = self._lower_row - self._minimum_read_row_count
            slice_ypath = yt.wrapper.TablePath(
                self.ypath,
                start_index=next_lower_row,
                end_index=self._lower_row,
                client=self._client
            )
            with self._client.Transaction(transaction_id=self._tx.transaction_id, attributes={"title": "Fuse read transaction"}):
                slice_content = b"".join(
                    self._client.read_table(slice_ypath, format=self._format, raw=True)
                )
            self._lower_offset -= len(slice_content)
            self._lower_row -= self._minimum_read_row_count
            self._buffer = [slice_content] + self._buffer

        slices_offset = offset - self._lower_offset
        return b"".join(self._buffer)[slices_offset:(slices_offset + length)]

    def truncate(self, length):
        self._logger.debug("truncate is not implemented")

    def write(self, data, offset):
        self._logger.debug("write is not implemented")
        return 0

    def flush(self):
        self._logger.debug("flush is not implemented")

    def close(self):
        self._tx.abort()


class Cypress(fuse.Operations):
    """An implementation of FUSE operations on a Cypress tree."""

    _logger = logging.getLogger(LOGGER_NAME + ".Cypress")
    _logger.setLevel(level=logging.DEBUG)

    _statistics = Statistics(_logger)

    _system_attributes = [
        "type",
        "ref_counter",
        "access_time",
        "modification_time",
        "creation_time",
        "uncompressed_data_size"
    ]

    def __init__(
            self, client, enable_write_access,
            minimum_file_read_size=(4 * 1024 ** 2),
            table_format="json",
            minimum_table_read_row_count=10000
    ):
        super(fuse.Operations, self).__init__()

        self._client = client
        self._enable_write_access = enable_write_access
        self._minimum_file_read_size = minimum_file_read_size
        self._table_format = table_format
        self._minimum_table_read_row_count = minimum_table_read_row_count

        self._next_fh = 0
        self._opened_files = {}

    @staticmethod
    def _to_ypath(path):
        """Converts an absolute file path to :class:`YPath <yt.wrapper.ypath.YPath>`."""
        if path == u"/":
            return u"/"
        return u"/" + path

    def _validate_write_access(self):
        if not self._enable_write_access:
            raise fuse.FuseOSError(errno.EROFS)

    @staticmethod
    def _to_timestamp(timestring):
        """Converts a time string in YT format to UNIX timestamp."""
        parsed_time = time.strptime(timestring, "%Y-%m-%dT%H:%M:%S.%fZ")
        return time.mktime(parsed_time)

    @staticmethod
    def _get_st_mode(attributes):
        """Gets st_mode for a node based on its attributes."""
        node_type = attributes["type"]
        if node_type == "file":
            mask = stat.S_IFREG | 0o666
        elif node_type == "table":
            mask = stat.S_IFREG | stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH
        elif node_type in ["map_node", "sys_node"]:
            mask = stat.S_IFDIR | 0o755
        else:
            # Support links maybe?
            mask = stat.S_IFBLK
        return mask | stat.S_IRUSR

    @staticmethod
    def _get_st_size(attributes):
        """Gets st_size for a node based on its attributes."""
        node_type = attributes["type"]
        if node_type == "file":
            return attributes["uncompressed_data_size"]
        return 0

    @staticmethod
    def _get_time(name, attributes):
        if name in attributes:
            return Cypress._to_timestamp(attributes[name])
        return 0

    def _get_stat(self, attributes):
        """Gets stat structure for a node based on its attributes."""
        return {
            "st_dev": 0,
            "st_ino": 0,
            "st_mode": Cypress._get_st_mode(attributes),
            "st_nlink": 1,
            "st_uid": 0,
            "st_gid": 0,
            "st_atime": Cypress._get_time("access_time", attributes),
            "st_mtime": Cypress._get_time("modification_time", attributes),
            "st_ctime": Cypress._get_time("creation_time", attributes),
            "st_size": Cypress._get_st_size(attributes)
        }

    def _get_xattr(self, attribute):
        """Converts Cypress attribute name to Linux attribute name."""
        return "user." + attribute

    def _get_attribute(self, xattr):
        """Converts Linux attribute name to Cypress attribute name."""
        if not xattr.startswith("user."):
            raise fuse.FuseOSError(errno.ENODATA)
        return ".".join(xattr.split(".")[1:])

    @handle_yt_errors(_logger)
    @log_calls(_logger, "%(__name__)s(%(path)r)", _statistics)
    def getattr(self, path, fi):
        ypath = self._to_ypath(path)
        for opened_file in self._opened_files.values():
            if opened_file.ypath == ypath:
                attributes = opened_file.attributes
                break
        else:
            attributes = self._client.get_attributes(
                ypath,
                self._system_attributes
            )
        return self._get_stat(attributes)

    @handle_yt_errors(_logger)
    @log_calls(_logger, "%(__name__)s(%(path)r)", _statistics)
    def readdir(self, path, fi):
        ypath = self._to_ypath(path)
        # Attributes are queried to speed up subsequent "getattr" queries
        # about the node's children (for example, in case of "ls" command).
        children = self._client.list(ypath, attributes=self._system_attributes)
        # Still having encoding problems,
        # try listing //statbox/home/zahaaar at Plato.
        return [str(child) for child in children]

    @handle_yt_errors(_logger)
    @log_calls(_logger, "%(__name__)s(%(path)r)", _statistics)
    def open(self, path, fi):
        ypath = self._to_ypath(path)
        attributes = self._client.get_attributes(
            ypath,
            self._system_attributes
        )

        type_ = attributes["type"]
        if type_ == "file":
            opened_file = OpenedFile(
                self._client, ypath, attributes,
                self._minimum_file_read_size
            )
        elif type_ == "table":
            # Without this flag FUSE treats the file with st_size=0 as empty.
            fi.direct_io = True
            opened_file = OpenedTable(
                self._client, ypath, attributes,
                self._table_format, self._minimum_table_read_row_count
            )
        else:
            raise fuse.FuseOSError(errno.EINVAL)

        # Non-atomic :(
        fi.fh = self._next_fh
        self._next_fh += 1

        self._opened_files[fi.fh] = opened_file
        return 0

    @handle_yt_errors(_logger)
    @log_calls(_logger, "%(__name__)s()", _statistics)
    def release(self, _, fi):
        self._opened_files[fi.fh].close()
        del self._opened_files[fi.fh]
        return 0

    @handle_yt_errors(_logger)
    @log_calls(
        _logger,
        "%(__name__)s(%(path)r, offset=%(offset)r, length=%(length)r)",
        _statistics
    )
    def read(self, path, length, offset, fi):
        opened_file = self._opened_files[fi.fh]
        return opened_file.read(length, offset)

    @handle_yt_errors(_logger)
    @log_calls(_logger, "%(__name__)s(%(path)r)", _statistics)
    def listxattr(self, path):
        ypath = self._to_ypath(path)
        attributes = self._client.get(ypath + "/@")
        return (self._get_xattr(attribute) for attribute in attributes)

    @handle_yt_errors(_logger)
    @log_calls(_logger, "%(__name__)s(%(path)r, name=%(name)r)", _statistics)
    def getxattr(self, path, name, position=0):
        ypath = self._to_ypath(path)
        attribute = self._get_attribute(name)
        try:
            attr = self._client.get_attribute(ypath, attribute)
        except yt.wrapper.YtError:
            raise fuse.FuseOSError(errno.ENODATA)
        return repr(attr)

    @handle_yt_errors(_logger)
    @log_calls(_logger, "%(__name__)s(%(path)r, mode=%(mode)r)", _statistics)
    def mkdir(self, path, mode):
        self._validate_write_access()
        ypath = self._to_ypath(path)
        self._client.create("map_node", ypath)

    @log_calls(_logger, "%(__name__)s(%(path)r, mode=%(mode)r)", _statistics)
    def chmod(self, path, mode):
        self._validate_write_access()
        self._logger.debug("chmod is not implemented")
        return 0

    @log_calls(_logger, "%(__name__)s(%(path)r, uid=%(uid)r, gid=%(gid)r)", _statistics)
    def chown(self, path, uid, gid):
        self._validate_write_access()
        self._logger.debug("chown is not implemented")

    @handle_yt_errors(_logger)
    @log_calls(_logger, "%(__name__)s(%(path)r)", _statistics)
    def create(self, path, mode, fi):
        self._validate_write_access()
        ypath = self._to_ypath(path)
        self._client.create("file", ypath)
        attributes = self._client.get_attributes(
            ypath,
            self._system_attributes
        )

        fi.fh = self._next_fh
        self._next_fh += 1
        self._opened_files[fi.fh] = OpenedFile(
            self._client, ypath, attributes,
            self._minimum_file_read_size
        )
        return 0

    @handle_yt_errors(_logger)
    @log_calls(_logger, "%(__name__)s(%(path)r)", _statistics)
    def unlink(self, path):
        self._validate_write_access()
        ypath = self._to_ypath(path)
        self._client.remove(ypath)

    @handle_yt_errors(_logger)
    @log_calls(_logger, "%(__name__)s(%(path)r)", _statistics)
    def rmdir(self, path):
        self._validate_write_access()
        ypath = self._to_ypath(path)
        try:
            self._client.remove(ypath)
        except yt.wrapper.YtResponseError as error:
            if "non-empty" in error.error["message"]:
                raise fuse.FuseOSError(errno.ENOTEMPTY)
            raise

    @log_calls(_logger, "%(__name__)s(%(path)r)", _statistics)
    def statfs(self, path):
        return dict(f_bsize=512, f_blocks=1024 * 4096, f_bavail=1024 * 2048)

    @handle_yt_errors(_logger)
    @log_calls(_logger, "%(__name__)s(%(path)r, length=%(length)r)", _statistics)
    def truncate(self, path, length, fh=None):
        self._validate_write_access()
        ypath = self._to_ypath(path)
        for file_fh, opened_file in self._opened_files.items():
            if opened_file.ypath == ypath:
                fh = file_fh
                break
        self._opened_files[fh].truncate(length)

    @handle_yt_errors(_logger)
    @log_calls(
        _logger,
        "%(__name__)s(%(path)r, offset=%(offset)r)",
        _statistics
    )
    def write(self, path, data, offset, fi):
        self._validate_write_access()
        return self._opened_files[fi.fh].write(data, offset)

    @handle_yt_errors(_logger)
    @log_calls(_logger, "%(__name__)s(%(path)r)", _statistics)
    def flush(self, path, fi):
        self._opened_files[fi.fh].flush()
