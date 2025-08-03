from .etc_commands import parse_ypath
from .common import flatten, parse_bool, update, require
from .errors import YtError
from .config import get_config
from .format import _ENCODING_SENTINEL

import yt.yson as yson

from copy import deepcopy
import string


# Storage attributes that can be specified as YPath attributes in write request
# TODO(YT-23841): Make "primary_medium" writable storage attribute
_WRITABLE_STORAGE_ATTRIBUTES = frozenset({"erasure_codec", "compression_codec", "optimize_for", "schema"})
# Storage attributes incompatible with append= flag
_APPEND_INCOMPATIBLE_ATTRIBUTES = frozenset({"erasure_codec", "compression_codec", "optimize_for", "schema"})
# All storage attributes
_STORAGE_ATTRIBUTES = frozenset({*_WRITABLE_STORAGE_ATTRIBUTES, "primary_medium"})


class TokensByPath(object):
    def __init__(self, path):
        if isinstance(path, bytes):
            self.slash = b"/"
            self.double_slash = b"//"
            self.sharp = b"#"
            self.raw_path = bytes(path)
            self.string_type = bytes
        else:
            self.slash = "/"
            self.double_slash = "//"
            self.sharp = "#"
            self.raw_path = str(path)
            self.string_type = str

    def to_path_type(self, text):
        return text if self.string_type == str else text.encode("latin1")


def _process_prefix(path, prefix):
    tokens = TokensByPath(path)
    if prefix is not None:
        prefix = TokensByPath(prefix).raw_path
    if prefix is not None and type(tokens.raw_path) is not type(prefix):
        raise YtError("Type mismatch of ypath %r and prefix %r" % (tokens.raw_path, prefix))

    if tokens.raw_path == tokens.slash or tokens.raw_path.startswith(tokens.double_slash) or \
            tokens.raw_path.startswith(tokens.sharp):
        return path
    else:
        require(prefix,
                lambda: YtError("Path %r should be absolute or you should specify a prefix" % tokens.raw_path))
        require(prefix.startswith("//"),
                lambda: YtError("PREFIX %r should start with //" % prefix))
        require(prefix.endswith("/"),
                lambda: YtError("PREFIX %r should end with /" % prefix))
        return yson.to_yson_type(prefix + tokens.raw_path if tokens.raw_path else prefix[:-1], path.attributes)


def ypath_join(*paths):
    """Joins parts of cypress paths."""
    def ends_with_slash(part):
        if part.endswith("/"):
            if part.endswith("\\/"):
                raise YtError("Path with \\\\/ found, failed to join it")
            return True
        return False

    result = []
    for path in paths:
        if isinstance(path, YPath):
            path = str(path)
        if path.startswith("//") or path == "/":
            result = []

        slash_count = 0
        if path != "/":
            if path.startswith("/"):
                slash_count += 1
            if result and ends_with_slash(result[-1]):
                slash_count += 1

        if slash_count == 2:
            result.append(path[1:])
        else:  # slash_count <= 1
            if (slash_count == 0 and result) or result == ["/"]:
                result.append("/")
            result.append(path)

    return "".join(result)


def ypath_split(path):
    """Splits the pathname path into a pair, (head, tail)
       where tail is the last pathname component and head is everything leading up to that.

       Equivalent of os.path.split for YPath.
    """
    parsed_ypath = YPath(path)

    tokens = TokensByPath(parsed_ypath._path_object)
    path = tokens.raw_path

    if path == tokens.slash:
        return tokens.slash, tokens.string_type()

    path_type = {tokens.slash: "root", tokens.sharp: "sharp"}.get(path[0])
    if path_type is None:
        raise YtError('Correct YPath should start with "/" or "#"')

    if path_type == "root" and not path.startswith(tokens.slash):
        raise YtError('Root YPath should start with "//"')

    slash_pos = None
    slash_escaped = False

    index = len(path) - 1
    index_lower_bound = int(path_type == "root")

    while index >= index_lower_bound:
        if path[index] == tokens.slash:
            if slash_pos is not None and not slash_escaped:
                raise YtError('Unexpected "/" at position ' + str(index))
            slash_pos = index
            slash_escaped = False
        elif path[index] == tokens.to_path_type("\\"):
            if slash_pos is not None:
                slash_escaped = not slash_escaped
        else:
            if slash_pos is None:
                index -= 1
                continue

            if not slash_escaped:
                break

            slash_pos = None
            slash_escaped = False

        index -= 1

    if slash_pos is None:
        return tokens.to_path_type(""), path

    if slash_pos == len(path) - 1 and not slash_escaped:
        raise YtError('Unexpected "/" at the end of YPath')

    return path[:slash_pos], path[slash_pos + 1:]


def escape_ypath_literal(literal, encoding=_ENCODING_SENTINEL):
    """Escapes string to use it as key in ypath."""
    def escape_char(ch):
        if isinstance(ch, int):
            ch = bytes([ch])

        assert isinstance(ch, bytes)

        if ch in [b"\\", b"/", b"@", b"&", b"[", b"{", b"*"]:
            return b"\\" + ch
        num = ord(ch)
        if num < 32 or num >= 128:
            return b"\\x" + string.hexdigits[num // 16].encode("ascii") + string.hexdigits[num % 16].encode("ascii")
        return ch

    if isinstance(literal, bytes):
        return b"".join(map(escape_char, literal))
    else:
        # NB: we suppose that cypress always has utf-8 representation for unicode strings.
        if encoding is _ENCODING_SENTINEL:
            encoding = "utf-8"
        if encoding is not None:
            literal = literal.encode(encoding)
        return "".join(map(lambda ch: escape_char(ch).decode("ascii"), literal))


def unescape_ypath_literal(key: bytes) -> bytes:
    """Converts a YPath key to string literal."""
    return b"\\".join(x.replace(b"\\", b"") for x in key.split(b"\\\\"))


class YPath(object):
    """Represents path with attributes (YPath).

    .. seealso:: `YPath in the docs <https://ytsaurus.tech/docs/en/user-guide/storage/ypath>`_
    """
    def __init__(self,
                 path,
                 simplify=None,
                 attributes=None,
                 client=None):
        """
        :param path: string representing cypress path, possible with YPath-encoded attributes.
        :param dict attributes: additional attributes.
        :param bool simplify: perform parsing of given path.
        """

        if simplify is None:
            simplify = True

        if isinstance(path, YPath):
            self._path_object = deepcopy(path._path_object)
        else:
            if simplify and path:
                self._path_object = parse_ypath(path, client=client)
                keys_to_replace = []
                for key in self._path_object.attributes:
                    if "-" in key:
                        keys_to_replace.append(key)

                for key in keys_to_replace:
                    self._path_object.attributes[key.replace("-", "_")] = self._path_object.attributes[key]
                for key in keys_to_replace:
                    del self._path_object.attributes[key]
            else:
                self._path_object = yson.to_yson_type(path)

        self._path_object = _process_prefix(self._path_object, get_config(client)["prefix"])
        if attributes is not None:
            self._path_object.attributes = update(self._path_object.attributes, attributes)

    @property
    def attributes(self):
        return self._path_object.attributes

    def to_yson_type(self):
        """Returns YSON representation of path."""
        return self._path_object

    def to_yson_string(self, sort_keys=False):
        """Returns YSON path with attributes as string."""
        if self.attributes:
            attributes_str = yson._dumps_to_native_str(
                self.attributes,
                yson_type="map_fragment",
                yson_format="text",
                sort_keys=sort_keys)
            # NB: in text format \n can appear only as separator.
            return "<{0}>{1}".format(
                attributes_str.replace("\n", ""), str(self._path_object))
        else:
            return str(self._path_object)

    def join(self, other):
        """Joins ypath with other path."""
        return YPath(ypath_join(str(self), other), simplify=False)

    def __eq__(self, other):
        # TODO(ignat): Fix it, compare with attributes!
        if isinstance(other, YPath):
            return str(self._path_object) == str(other._path_object)
        else:
            return str(self._path_object) == other

    def __ne__(self, other):
        return not (self == other)

    def __hash__(self):
        return hash(self._path_object)

    def __str__(self):
        return str(self._path_object)

    def __repr__(self):
        return self.to_yson_string()

    def __add__(self, other):
        return YPath(str(self) + other, simplify=False)


class YPathSupportingAppend(YPath):
    def __init__(self, path, simplify=True, attributes=None, append=None, client=None):
        super(YPathSupportingAppend, self).__init__(path, simplify=simplify, attributes=attributes, client=client)
        self._append = None
        if append is not None:
            self.append = append
        elif "append" in self.attributes:
            self.append = self.attributes["append"]

    @property
    def append(self):
        if self._append is not None:
            return parse_bool(self._append)
        else:
            return None

    @append.setter
    def append(self, value):
        self._append = value
        if self._append is not None:
            self.attributes["append"] = self._append
        else:
            if "append" in self.attributes:
                del self.attributes["append"]

    def clone_with_append_set(self):
        if not self.append:
            cls = type(self)
            new_path = cls(self, simplify=False, append=True)
            attributes = new_path.attributes
            for attribute_name in _APPEND_INCOMPATIBLE_ATTRIBUTES:
                if attribute_name in attributes:
                    del attributes[attribute_name]
            return new_path
        return type(self)(self)


def to_ypath(object, client=None):
    if isinstance(object, YPath):
        return object
    else:
        return YPath(object, client=client)


class TablePath(YPathSupportingAppend):
    """YPath descendant to be used in table commands.

    Supported attributes:

    * append -- append to table or overwrite.
    * columns -- list of string (column).
    * exact_key, lower_key, upper_key -- tuple of strings to identify range of rows.
    * exact_index, start_index, end_index -- tuple of indexes to identify range of rows.
    * ranges -- list of dicts, allows to specify arbitrary ranges on the table, see more details in the docs.
    * schema -- TableSchema (or list with column schemas -- deprecated), see
        `static schema doc <https://ytsaurus.tech/docs/en/user-guide/storage/static-schema#creating-a-table-with-a-schema>`_

    .. seealso:: `YPath in the docs <https://ytsaurus.tech/docs/en/user-guide/storage/ypath>`_
    """
    def __init__(self,
                 # TODO(ignat): rename to path
                 name,
                 append=None,
                 sorted_by=None,
                 columns=None,
                 exact_key=None,
                 lower_key=None,
                 upper_key=None,
                 exact_index=None,
                 start_index=None,
                 end_index=None,
                 ranges=None,
                 schema=None,
                 optimize_for=None,
                 compression_codec=None,
                 erasure_codec=None,
                 foreign=None,
                 rename_columns=None,
                 simplify=None,
                 attributes=None,
                 client=None):
        """
        :param str name: path with attributes.
        :param bool append: append to table or overwrite.
        :param sorted_by: list of sort keys.
        :type sorted_by: list[str]
        :param columns: list of string (column) or string pairs (column range).
        :param exact_key: exact key of row.
        :type exact_key: str or tuple[str]
        :param lower_key: lower key bound of rows.
        :type lower_key: str or tuple[str]
        :param upper_key: upper bound of rows.
        :type upper_key: str or tuple[str]
        :param int exact_index: exact index of row.
        :param int start_index: lower bound of rows.
        :param int end_index: upper bound of rows.
        :param list ranges: list of ranges of rows. It overwrites all other row limits.
        :param schema: table schema description.
        :type schema: TableSchema (or list of column schemas -- deprecated).
        :param string optimize_for: value of optimize_for mode.
        :param string compression_codec: compression codec.
        :param string erasure_codec: erasure codec.
        :param bool foreign: table is foreign for sorted reduce and joinreduce operations.
        :param dict attributes: attributes, it updates attributes specified in name.

        .. seealso:: `usage example <https://ytsaurus.tech/docs/en/user-guide/storage/ypath#known_attributes>`_
        .. note:: don't specify lower_key (upper_key) and start_index (end_index) simultaneously.
        """
        if isinstance(name, bytes):
            raise YtError("TablePath does not support binary strings")

        super(TablePath, self).__init__(name, simplify=simplify, attributes=attributes, append=append, client=client)

        attributes = self._path_object.attributes
        if "channel" in attributes:
            attributes["columns"] = attributes["channel"]
            del attributes["channel"]
        if sorted_by is not None:
            attributes["sorted_by"] = sorted_by
        if columns is not None:
            attributes["columns"] = columns
        if schema is not None:
            attributes["schema"] = schema
        if optimize_for is not None:
            attributes["optimize_for"] = optimize_for
        if compression_codec is not None:
            attributes["compression_codec"] = compression_codec
        if erasure_codec is not None:
            attributes["erasure_codec"] = erasure_codec
        if foreign is not None:
            attributes["foreign"] = foreign
        if rename_columns is not None:
            attributes["rename_columns"] = rename_columns

        if ranges is not None:
            def _check_option(value, option_name):
                if value is not None:
                    raise YtError("Option '{0}' cannot be specified with 'ranges' option".format(option_name))

            for value, name in [(exact_key, "exact_key"), (exact_index, "exact_index"),
                                (lower_key, "lower_key"), (start_index, "start_index"),
                                (upper_key, "upper_key"), (end_index, "end_index")]:
                _check_option(value, name)

            attributes["ranges"] = ranges

        else:
            if start_index is not None and lower_key is not None:
                raise YtError("You could not specify lower key bound and start index simultaneously")
            if end_index is not None and upper_key is not None:
                raise YtError("You could not specify upper key bound and end index simultaneously")

            range = {}
            if "exact" in attributes:
                range["exact"] = attributes["exact"]
                del attributes["exact"]
            if "lower_limit" in attributes:
                range["lower_limit"] = attributes["lower_limit"]
                del attributes["lower_limit"]
            if "upper_limit" in attributes:
                range["upper_limit"] = attributes["upper_limit"]
                del attributes["upper_limit"]

            if exact_key is not None:
                range["exact"] = {"key": flatten(exact_key)}
            if lower_key is not None:
                range["lower_limit"] = {"key": flatten(lower_key)}
            if upper_key is not None:
                if get_config(client)["yamr_mode"]["use_non_strict_upper_key"]:
                    upper_key = upper_key + "\0"
                range["upper_limit"] = {"key": flatten(upper_key)}
            if exact_index is not None:
                range["exact"] = {"row_index": exact_index}
            if start_index is not None:
                range["lower_limit"] = {"row_index": start_index}
            if end_index is not None:
                range["upper_limit"] = {"row_index": end_index}

            if range:
                attributes["ranges"] = [range]

    def has_delimiters(self):
        """Checks attributes for delimiters (channel, lower or upper limits)."""
        return any(key in self.attributes for key in ["columns", "lower_limit", "upper_limit", "ranges"])

    @property
    def columns(self):
        return self.attributes.get("columns")

    @columns.setter
    def columns(self, value):
        self.attributes["columns"] = value

    @property
    def ranges(self):
        return self.attributes.get("ranges", [])

    @ranges.setter
    def ranges(self, value):
        self.attributes["ranges"] = value

    @property
    def rename_columns(self):
        return self.attributes.get("rename_columns", [])

    @rename_columns.setter
    def rename_columns(self, value):
        self.attributes["rename_columns"] = value

    def canonize_exact_ranges(self):
        """Replaces all "exact" ranges with "lower_limit" and "upper_limit"."""
        for range in self.ranges:
            if "exact" in range:
                lower_limit = range["exact"]
                range["lower_limit"] = lower_limit
                upper_limit = deepcopy(lower_limit)
                if "key" in upper_limit:
                    sentinel = yson.YsonEntity()
                    sentinel.attributes["type"] = "max"
                    upper_limit["key"].append(sentinel)
                if "row_index" in upper_limit:
                    upper_limit["row_index"] += 1
                if "chunk_index" in upper_limit:
                    upper_limit["chunk_index"] += 1
                range["upper_limit"] = upper_limit
                del range["exact"]

    def has_key_limit_in_ranges(self):
        """Checks whether ranges contain key limits."""
        for range in self.attributes.get("ranges", []):
            for item, value in range.items():
                if "key" in value:
                    return True
        return False


class FilePath(YPathSupportingAppend):
    """YPath descendant to be used in file commands."""
    def __init__(self, path, append=None, executable=None, file_name=None, simplify=None, attributes=None, client=None):
        super(FilePath, self).__init__(path, attributes=attributes, simplify=simplify, append=append, client=client)
        if executable is not None:
            self.attributes["executable"] = executable
        if file_name is not None:
            self.attributes["file_name"] = file_name


def ypath_dirname(path):
    """Returns path one level above specified `path`.
       Equivalent of os.path.dirname for YPath.
    """

    # Dropping ranges and attributes.
    # Also checking that path is not empty.
    dirname, suffix = ypath_split(path)
    if not dirname:
        dirname = suffix

    return dirname
