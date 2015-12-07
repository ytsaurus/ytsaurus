#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""mount-cypress -- mount a Cypress, an YT cluster metainformation tree.

Usage:
  mount-cypress <proxy> <mountpoint>
  mount-cypress (-h | --help)

Arguments:
  <proxy>       Proxy alias like "aristotle.yt.yandex.net" or just "aristotle".
  <mountpoint>  Mountpoint directory like "/mnt/aristotle".

Options:
  -h, --help    Show this help.

"""
import yt.wrapper.client

from yt.packages.expiringdict import ExpiringDict
import yt.packages.fuse as fuse

import stat
import errno
import time
import logging
import functools
import sys


logging.basicConfig(
    format="%(name)s\t%(asctime)s.%(msecs)03d\t%(message)s",
    datefmt="%H:%M:%S"
)


def log_calls(logger, message_format):
    """Create a decorator for logging each wrapped function call.

    message_format:
      An old-style format string.
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
            return function(*args, **kwargs)

        return logged_function

    return get_logged_version


def handle_yt_errors(function):
    """Modify the function so it raises FuseOSError instead of YtError."""
    @functools.wraps(function)
    def cautious_function(*args, **kwargs):
        try:
            return function(*args, **kwargs)
        except yt.wrapper.YtResponseError:
            raise fuse.FuseOSError(errno.ENOENT)
        except yt.packages.requests.ConnectionError:
            raise fuse.FuseOSError(errno.EAGAIN)

    return cautious_function


class CachedYtClient(yt.wrapper.client.Yt):
    """An YT client which caches nodes and their attributes for some time."""

    _logger = logging.getLogger(__name__ + ".CachedYtClient")
    _logger.setLevel(level=logging.DEBUG)

    def __init__(self, max_len=16384, max_age_seconds=2, **kwargs):
        """Initialize the client.

        max_len:
          Maximum number of cached nodes; the default is 16384.
        max_age_seconds:
          After this period the node is removed from cache;
          the default value is 2 seconds.
        The rest of the arguments are passed to the parent constructor.
        """
        super(CachedYtClient, self).__init__(**kwargs)

        self._cache = ExpiringDict(
            max_len=max_len,
            max_age_seconds=max_age_seconds
        )
        # Keys are paths of nodes and node attributes. Each value is either
        # (False, e), meaning that there is no such node/attribute, *e* being
        # an exception which should be raised on access attempt, or
        # (True, x), *x* being the list of children/the attribute value.

    @staticmethod
    def _error(attribute):
        return yt.wrapper.YtResponseError("No such attribute: " + attribute)

    @log_calls(_logger, "%(__name__)s(%(path)r)")
    def get_attributes(self, path, attributes):
        """Get a subset of node's attributes."""
        # Firstly, check whether we are sure the node doesn't exist at all.
        flag, value = self._cache.get(path, (True, None))
        if not flag:
            raise value

        # Secondly, check whether all requested attributes are cached.
        try:
            cache_slice = ((a, self._cache[path + "/@" + a]) for a in attributes)
            return dict((a, v) for a, (f, v) in cache_slice if f)
        except KeyError:
            pass

        # Finally, fetch all node's attributes.
        self._logger.debug("\tmiss")
        try:
            all_attributes = super(CachedYtClient, self).get(path + "/@")
        except yt.wrapper.YtResponseError as error:
            if error.is_resolve_error():
                self._cache[path] = (False, error)
            raise
        self._cache.update(
            (path + "/@" + a, (True, v)) for a, v in all_attributes.iteritems()
        )

        requested_attributes = {}
        for attribute in attributes:
            if attribute in all_attributes:
                requested_attributes[attribute] = all_attributes[attribute]
            else:
                self._cache[path + "/@" + attribute] = (
                    False, self._error(attribute)
                )
        return requested_attributes

    @log_calls(_logger, "%(__name__)s(%(path)r)")
    def list(self, path, attributes=None):
        """Get children of a node specified by a ypath."""
        cached_value = self._cache.get(path)
        if cached_value is not None:
            flag, value = cached_value
            if flag:
                return value
            raise value

        if attributes is None:
            attributes = []

        children = super(CachedYtClient, self).list(
            path, attributes=attributes
        )
        self._cache[path] = (True, children)

        for attribute in attributes:
            for child in children:
                if attribute in child.attributes:
                    child_value = (True, child.attributes[attribute])
                else:
                    child_value = (False, self._error(attribute))
                child_path = path + "/" + child + "/@" + attribute
                self._cache[child_path] = child_value

        return children

    def create(self, type, path=None, recursive=False, ignore_existing=False, attributes=None, client=None):
        super(CachedYtClient, self).create(
            type, path=path, recursive=recursive,
            ignore_existing=ignore_existing, attributes=attributes
        )
        self._cache.pop(path)
        self._cache.pop(path + "/@")

def create_transaction_and_take_snapshot_lock(ypath, client):
    title = "FUSE: read {0}".format(yt.wrapper.to_name(ypath, client=client))
    tx = yt.wrapper.Transaction(attributes={"title": title}, client=client)
    with yt.wrapper.Transaction(transaction_id=tx.transaction_id, client=client):
        yt.wrapper.lock(ypath, mode="snapshot", client=client)
    return tx

class OpenedFile(object):
    """Stores information and cache for currently opened regular file."""

    _logger = logging.getLogger(__name__ + ".OpenedFile")
    _logger.setLevel(level=logging.DEBUG)

    def __init__(self, client, ypath, attributes, minimum_read_size):
        """Set up cache.

        minimum_read_size:
          The minimum number of bytes to read from the cluster at once.
        """
        self.ypath = ypath
        self.attributes = attributes

        self._client = client
        self._minimum_read_size = minimum_read_size
        self._length = 0
        self._offset = 0
        self._buffer = ""
        self._has_pending_write = False
        self._tx = create_transaction_and_take_snapshot_lock(ypath, client)

    def read(self, length, offset):
        if offset < self._offset \
                or offset + length > self._offset + self._length:
            self._logger.debug("\tmiss")
            self._length = max(length, self._minimum_read_size)
            if offset < self._offset:
                self._offset = max(offset + length - self._length, 0)
            else:
                self._offset = offset
            with yt.wrapper.Transaction(transaction_id=self._tx.transaction_id, client=self._client):
                self._buffer = self._client.read_file(
                    self.ypath,
                    length=self._length, offset=self._offset
                ).read()

        assert self._offset <= offset
        assert self._offset + self._length >= offset + length

        buffer_offset = offset - self._offset
        return self._buffer[buffer_offset:(buffer_offset + length)]

    def truncate(self, length):
        if not self._has_pending_write:
            with yt.wrapper.Transaction(transaction_id=self._tx.transaction_id, client=self._client):
                self._buffer = self._client.read_file(self.ypath).read()
        self._has_pending_write = True

        if len(self._buffer) < length:
            self._buffer += " " * (length - len(self._buffer))
        self._buffer = self._buffer[:length]

    def write(self, data, offset):
        if not self._has_pending_write:
            with yt.wrapper.Transaction(transaction_id=self._tx.transaction_id, client=self._client):
                self._buffer = self._client.read_file(self.ypath).read()
        self._has_pending_write = True

        self._buffer = self._buffer[:offset] + data
        return len(data)

    def flush(self):
        if self._has_pending_write:
            self._client.write_file(self.ypath, self._buffer)
            self._buffer = ""


class OpenedTable(object):
    """Stores information and cache for currently opened table."""

    _logger = logging.getLogger(__name__ + ".OpenedTable")
    _logger.setLevel(level=logging.DEBUG)

    def __init__(self, client, ypath, attributes, format, minimum_read_row_count):
        """Set up cache.

        minimum_table_read_row_count:
          The minimum number of rows to read from the cluster at once.
        """
        self.ypath = ypath
        self.attributes = attributes

        self._client = client
        self._format = format
        self._minimum_read_row_count = minimum_read_row_count
        self._lower_offset = self._upper_offset = 0
        self._lower_row = self._upper_row = 0
        self._buffer = []
        self._tx = create_transaction_and_take_snapshot_lock(ypath, client)

    def read(self, length, offset):
        while self._upper_offset < offset + length:
            next_upper_row = self._upper_row + self._minimum_read_row_count
            slice_ypath = yt.wrapper.TablePath(
                self.ypath,
                start_index=self._upper_row,
                end_index=next_upper_row,
                client=self._client
            )
            with yt.wrapper.Transaction(transaction_id=self._tx.transaction_id, client=self._client):
                slice_content = "".join(
                    self._client.read_table(slice_ypath, format=self._format)
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
            with yt.wrapper.Transaction(transaction_id=self._tx.transaction_id, client=self._client):
                slice_content = "".join(
                    self._client.read_table(slice_ypath, format=self._format)
                )
            self._lower_offset -= len(slice_content)
            self._lower_row -= self._minimum_read_row_count
            self._buffer = [slice_content] + self._buffer

        slices_offset = offset - self._lower_offset
        return "".join(self._buffer)[slices_offset:(slices_offset + length)]


class Cypress(fuse.Operations):
    """An implementation of FUSE operations on a Cypress tree."""

    _logger = logging.getLogger(__name__ + ".Cypress")
    _logger.setLevel(level=logging.DEBUG)

    _system_attributes = [
        "type",
        "ref_counter",
        "access_time",
        "modification_time",
        "creation_time",
        "uncompressed_data_size"
    ]

    def __init__(
            self, client,
            minimum_file_read_size=(4 * 1024 ** 2),
            table_format="json",
            minimum_table_read_row_count=10000
    ):
        super(fuse.Operations, self).__init__()

        self._client = client
        self._minimum_file_read_size = minimum_file_read_size
        self._table_format = table_format
        self._minimum_table_read_row_count = minimum_table_read_row_count

        self._next_fh = 0
        self._opened_files = {}

    @staticmethod
    def _to_ypath(path):
        """Convert an absolute file path to YPath."""
        if path == u"/":
            return u"/"
        return u"/" + path

    @staticmethod
    def _to_timestamp(timestring):
        """Convert a time string in YT format to UNIX timestamp."""
        parsed_time = time.strptime(timestring, "%Y-%m-%dT%H:%M:%S.%fZ")
        return time.mktime(parsed_time)

    def _get_st_mode(self, attributes):
        """Get st_mode for a node based on its attributes."""
        node_type = attributes["type"]
        if node_type == "file":
            mask = stat.S_IFREG | 0666
        elif node_type == "table":
            mask = stat.S_IFREG | stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH
        elif node_type == "map_node":
            mask = stat.S_IFDIR | 0755
        else:
            # Support links maybe?
            mask = stat.S_IFBLK
        return mask | stat.S_IRUSR

    def _get_st_size(self, attributes):
        """Get st_size for a node based on its attributes."""
        node_type = attributes["type"]
        if node_type == "file":
            return attributes["uncompressed_data_size"]
        return 0

    def _get_stat(self, attributes):
        """Get stat sturcture for a node based on its attributes."""
        return {
            "st_dev": 0,
            "st_ino": 0,
            "st_mode": self._get_st_mode(attributes),
            "st_nlink": attributes["ref_counter"],
            "st_uid": -1,
            "st_gid": -1,
            "st_atime": self._to_timestamp(attributes["access_time"]),
            "st_mtime": self._to_timestamp(attributes["modification_time"]),
            "st_ctime": self._to_timestamp(attributes["creation_time"]),
            "st_size": self._get_st_size(attributes)
        }

    def _get_xattr(self, attribute):
        """Convert Cypress attribute name to Linux attribute name."""
        return "user." + attribute

    def _get_attribute(self, xattr):
        """Convert Linux attribute name to Cypress attribute name."""
        if not xattr.startswith("user."):
            raise fuse.FuseOSError(errno.ENODATA)
        return ".".join(xattr.split(".")[1:])

    @handle_yt_errors
    @log_calls(_logger, "%(__name__)s(%(path)r)")
    def getattr(self, path, fi):
        ypath = self._to_ypath(path)
        for opened_file in self._opened_files.itervalues():
            if opened_file.ypath == ypath:
                attributes = opened_file.attributes
                break
        else:
            attributes = self._client.get_attributes(
                ypath,
                self._system_attributes
            )
        return self._get_stat(attributes)

    @handle_yt_errors
    @log_calls(_logger, "%(__name__)s(%(path)r)")
    def readdir(self, path, fi):
        ypath = self._to_ypath(path)
        # Attributes are queried to speed up subsequent "getattr" queries
        # about the node's children (for example, in case of "ls" command).
        children = self._client.list(ypath, attributes=self._system_attributes)
        # Still having encoding problems,
        # try listing //statbox/home/zahaaar at Plato.
        return (child.decode("utf-8") for child in children)

    @handle_yt_errors
    @log_calls(_logger, "%(__name__)s(%(path)r)")
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

    @log_calls(_logger, "%(__name__)s()")
    def release(self, _, fi):
        del self._opened_files[fi.fh]
        return 0

    @handle_yt_errors
    @log_calls(
        _logger,
        "%(__name__)s(offset=%(offset)r, length=%(length)r)"
    )
    def read(self, _, length, offset, fi):
        opened_file = self._opened_files[fi.fh]
        return opened_file.read(length, offset)

    # NB: implementing readdir could greatly improve unix find speed, no?

    @handle_yt_errors
    @log_calls(_logger, "%(__name__)s(%(path)r)")
    def listxattr(self, path):
        ypath = self._to_ypath(path)
        attributes = self._client.get(ypath + "/@")
        return (self._get_xattr(attribute) for attribute in attributes)

    @handle_yt_errors
    @log_calls(_logger, "%(__name__)s(%(path)r, name=%(name)r)")
    def getxattr(self, path, name, position=0):
        ypath = self._to_ypath(path)
        attribute = self._get_attribute(name)
        try:
            attr = self._client.get_attribute(ypath, attribute)
        except yt.wrapper.YtError:
            raise fuse.FuseOSError(errno.ENODATA)
        return repr(attr)

    @log_calls(_logger, "%(__name__)s(%(path)r)")
    def mkdir(self, path, mode):
        ypath = self._to_ypath(path)
        self._client.create("map_node", ypath)

    @log_calls(_logger, "%(__name__)s(%(path)r)")
    def create(self, path, mode, fi):
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

    @log_calls(_logger, "%(__name__)s(%(path)r)")
    def unlink(self, path):
        ypath = self._to_ypath(path)
        self._client.remove(ypath)

    @log_calls(_logger, "%(__name__)s(%(path)r)")
    def rmdir(self, path):
        ypath = self._to_ypath(path)
        try:
            self._client.remove(ypath)
        except yt.wrapper.YtResponseError as error:
            if "non-empty" in error.error["message"]:
                raise fuse.FuseOSError(errno.ENOTEMPTY)
            raise

    @log_calls(_logger, "%(__name__)s(%(path)r)")
    def truncate(self, path, length, fh=None):
        ypath = self._to_ypath(path)
        for file_fh, opened_file in self._opened_files.iteritems():
            if opened_file.ypath == ypath:
                fh = file_fh
                break
        self._opened_files[fh].truncate(length)

    @log_calls(_logger, "%(__name__)s(%(path)r)")
    def write(self, path, data, offset, fi):
        return self._opened_files[fi.fh].write(data, offset)

    @log_calls(_logger, "%(__name__)s(%(path)r)")
    def flush(self, path, fi):
        self._opened_files[fi.fh].flush()
