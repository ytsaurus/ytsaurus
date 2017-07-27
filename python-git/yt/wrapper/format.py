"""YT data formats.

.. note:: In `Format <.Format>` descendants constructors default parameters are overridden by `attributes` \
parameters, and then by kwargs options.
"""

from .config import get_config
from .common import get_value, require, filter_dict, merge_dicts, YtError, parse_bool
from .mappings import FrozenDict
from .yamr_record import Record, SimpleRecord, SubkeyedRecord
from . import yson

from yt.common import to_native_str
import yt.logger as logger
import yt.json as json

from yt.packages.six import (iteritems, Iterator, add_metaclass, PY3, binary_type, text_type,
                             indexbytes, int2byte)
from yt.packages.six.moves import xrange, map as imap, zip as izip, filter as ifilter

import os
from abc import ABCMeta, abstractmethod
from codecs import getwriter
from types import MethodType
import copy
import struct
try:
    from cStringIO import StringIO as BytesIO
except ImportError:  # Python 3
    from io import BytesIO

try:
    from statbox_bindings2.string_utils import (
        simple_parsers as sb_simple_parsers,
        misc as sb_misc
    )
except ImportError:
    sb_simple_parsers = None
    sb_misc = None

_ENCODING_SENTINEL = object()

class _AttributeDict(dict):
    def __init__(self, *args, **kwargs):
        super(_AttributeDict, self).__init__(*args, **kwargs)
        self.__dict__ = self

    def __deepcopy__(self, memo=None):
        return _AttributeDict(**self.__dict__)

    def __copy__(self):
        return _AttributeDict(**self.__dict__)

class YtFormatError(YtError):
    """Wrong format."""
    pass

class YtFormatReadError(YtFormatError):
    """Problem with parsing that can be caused by network problems."""
    pass

@add_metaclass(ABCMeta)
class Format(object):
    """YT data representations.

       Abstract base class for different formats.
    """
    _COLUMN_KEY_DEFAULT_ENCODING = "utf-8"

    def __init__(self, name, attributes=None, raw=None, encoding=_ENCODING_SENTINEL):
        """
        :param str name: format name.
        :param dict attributes: format parameters.
        """
        require(isinstance(name, str), lambda: YtFormatError("Incorrect format %r" % name))
        self._name = yson.to_yson_type(name)
        self._name.attributes = get_value(attributes, {})
        self._raw = raw
        self._encoding = self._get_encoding(encoding)

    def to_yson_type(self):
        """Returns YSON representation of format."""
        return self._name

    def name(self):
        """Returns string name of format."""
        return str(self._name)

    def _get_attributes(self):
        return self._name.attributes

    def _set_attributes(self, value):
        self._name.attributes = value

    attributes = property(_get_attributes, _set_attributes)

    def _encode_string(self, string):
        if not PY3:
            return string

        if isinstance(string, binary_type):
            if self._encoding is not None:
                raise YtError('Bytes object "{0}" cannot be encoded to "{1}"". Consider passing None to "encoding" '
                              'parameter in format constructor'.format(string, self._encoding))
            return string
        elif isinstance(string, text_type):
            if self._encoding is None:
                raise YtError('Cannot encode unicode string "{0}" because encoding is not specified. '
                              'Consider passing "encoding" parameter to format constructor'.format(string))
            return string.encode(self._encoding)
        else:
            raise YtError("Object {0} is not string object".format(repr(string)))

    def _coerce_column_key(self, key):
        if not PY3:
            return key
        if self._encoding is None and isinstance(key, text_type):
            return key.encode(self._COLUMN_KEY_DEFAULT_ENCODING)
        if self._encoding is not None and isinstance(key, binary_type):
            return key.decode(self._encoding)
        return key

    def _is_raw(self, raw):
        if raw is None:
            if self._raw is None:
                return False
            return self._raw
        return raw

    def __repr__(self):
        return to_native_str(yson.dumps(self._name))

    def __eq__(self, other):
        if isinstance(other, Format):
            return self._name == other._name
        return False

    def __ne__(self, other):
        return not self.__eq__(other)

    @staticmethod
    def _check_output_table_index(table_index, table_count):
        if table_index >= table_count or table_index < 0:
            raise YtError("Incorrect table index", attributes={"table_index": table_index})

    @staticmethod
    def _escape(string, escape_dict):
        string = string.replace(b"\\", b"\\\\")
        for sym, escaped in iteritems(escape_dict):
            string = string.replace(sym, escaped)
        return string

    @staticmethod
    def _unescape(string, escape_dict):
        for sym, unescaped in iteritems(escape_dict):
            string = string.replace(sym, unescaped)
        return string.replace(b"\\", b"")

    @staticmethod
    def _get_encoding(encoding):
        if encoding is _ENCODING_SENTINEL:
            return "utf-8" if PY3 else None

        if not PY3 and encoding is not _ENCODING_SENTINEL and encoding is not None:
            raise YtError('Parameter "encoding" is not supported for Python 2')

        return encoding

    @abstractmethod
    def load_row(self, stream, raw=None):
        """Reads bytes from the binary stream, parses (optionally) and returns one row.

        :return: parsed row.
        :rtype: dict or :class:`Record <yt.wrapper.yamr_record.Record>`. \
        If stream is empty - `None`. If `raw` is `True` - bytes.
        """
        pass

    @abstractmethod
    def load_rows(self, stream, raw=None):
        """Reads bytes from the stream, parses, processes input table switcher and yields all rows."""
        pass

    def dump_row(self, row, stream, raw=None):
        """Serializes row and writes to the stream."""
        if self._is_raw(raw):
            stream.write(row)
            return
        self._dump_row(row, stream)

    def dump_rows(self, rows, stream_or_streams, raw=None):
        """Serializes rows, creates output table switchers and writes to the stream."""
        if self._is_raw(raw):
            stream = stream_or_streams[0] if isinstance(stream_or_streams, list) else stream_or_streams
            for row in rows:
                stream.write(row)
            return
        self._dump_rows(rows, stream_or_streams)

    def dumps_row(self, row):
        """Converts parsed row to string."""
        stream = BytesIO()
        self.dump_row(row, stream)
        return stream.getvalue()

    def loads_row(self, string):
        """Converts string to parsed row."""
        stream = BytesIO(string)
        return self.load_row(stream)

    @staticmethod
    def _create_property(property_name, conversion=None):
        if conversion is None:
            get_func = lambda self: self.attributes[property_name]
        else:
            get_func = lambda self: conversion(self.attributes[property_name])
        return property(get_func)

    @staticmethod
    def _make_attributes(attributes, defaults, options):
        not_none_options = filter_dict(lambda key, value: value is not None, options)
        return merge_dicts(defaults, attributes, not_none_options)

    def is_raw_load_supported(self):
        """Returns true if format supports loading raw YSON rows."""
        return self.name() in ("dsv", "yamr", "yamred_dsv", "json", "yson", "schemaful_dsv")

    @staticmethod
    def _copy_docs():
        """Magic for copying docs in subclasses.
           Call once after all subclasses are created.
        """
        for cl in Format.__subclasses__():
            cl_dict = cl.__dict__
            for name in ("load_row", "load_rows", "dump_row", "dump_rows", "loads_row", "dumps_row"):
                if name in cl_dict and not cl_dict[name].__doc__:
                    cl_dict[name].__doc__ = Format.__dict__[name].__doc__

    @staticmethod
    def _process_input_rows(rows, process_table_index, control_attributes_mode,
                            extract_control_attributes, table_index_column_name, transform_column_name):
        table_index_attribute_name, row_index_attribute_name, range_index_attribute_name = \
            list(imap(transform_column_name, [b"table_index", b"row_index", b"range_index"]))
        table_index_column_name, range_index_column_name, row_index_column_name = \
            list(imap(transform_column_name, [table_index_column_name, "@range_index", "@row_index"]))

        def generator():
            table_index = None
            row_index = None
            range_index = None
            for row in rows:
                attributes = extract_control_attributes(row)
                if attributes is not None:
                    if table_index_attribute_name in attributes:
                        table_index = attributes[table_index_attribute_name]
                    if row_index_attribute_name in attributes:
                        row_index = attributes[row_index_attribute_name]
                    if range_index_attribute_name in attributes:
                        range_index = attributes[range_index_attribute_name]
                    continue

                if table_index_column_name is not None:
                    row[table_index_column_name] = table_index
                if range_index is not None:
                    row[range_index_column_name] = range_index
                if row_index is not None:
                    row[row_index_column_name] = row_index

                yield row

                if row_index is not None:
                    row_index += 1

        class RowsIterator(Iterator):
            def __init__(self):
                self.table_index = None
                self.row_index = None
                self.range_index = None
                self._increment_row_index = False

            def __next__(self):
                for row in rows:
                    attributes = extract_control_attributes(row)
                    if attributes is not None:
                        self._increment_row_index = False
                        if table_index_attribute_name in attributes:
                            self.table_index = attributes[table_index_attribute_name]
                        if row_index_attribute_name in attributes:
                            self.row_index = attributes[row_index_attribute_name]
                        if range_index_attribute_name in attributes:
                            self.range_index = attributes[range_index_attribute_name]
                        continue
                    else:
                        if self._increment_row_index and self.row_index is not None:
                            self.row_index += 1
                        self._increment_row_index = True
                        return row

                raise StopIteration()

            def __iter__(self):
                return self

        if process_table_index is None:
            if control_attributes_mode == "row_fields":
                return generator()
            elif control_attributes_mode == "iterator":
                return RowsIterator()
            else:
                return rows
        elif process_table_index:
            return generator()
        else:
            return rows

class DsvFormat(Format):
    """Tabular format widely used in Statistics.

    .. seealso:: `DSV on wiki <https://wiki.yandex-team.ru/yt/userdoc/formats/#dsv>`_
    """

    def __init__(self, enable_escaping=None, enable_table_index=None, table_index_column=None,
                 attributes=None, raw=None, encoding=_ENCODING_SENTINEL):
        """
        :param bool enable_escaping: process escaped symbols, `True` by default.
        :param bool enable_table_index: process input table indexes in load_rows. \
        NB! specify it only for operations!
        :param str table_index_column: name for special row field (exists only inside \
        operation binary!), "@table_index" by default.
        """
        defaults = {"enable_escaping": True, "enable_table_index": False,
                    "table_index_column": "@table_index"}
        options = {"enable_escaping": enable_escaping,
                   "enable_table_index": enable_table_index,
                   "table_index_column": table_index_column}

        all_attributes = Format._make_attributes(get_value(attributes, {}), defaults, options)
        super(DsvFormat, self).__init__("dsv", all_attributes, raw, encoding)
        self._coerced_table_index_column = self._coerce_column_key(self.table_index_column)

    enable_escaping = Format._create_property("enable_escaping", parse_bool)

    enable_table_index = Format._create_property("enable_table_index", parse_bool)

    table_index_column = Format._create_property("table_index_column")

    def load_row(self, stream, raw=None):
        line = stream.readline()

        if not line:
            return None

        if self._is_raw(raw):
            return line

        # NOTE: sb_simple_parsers module outputs bytes only. Not using it
        # if encoding is specified.
        if sb_simple_parsers is not None and self._encoding is None:
            parsed_line = sb_simple_parsers.parse_tskv_chomp(line)
        else:
            parsed_line = self._parse(line)

        if self.enable_table_index:
            parsed_line[self._coerced_table_index_column] = int(parsed_line[self._coerced_table_index_column])

        return parsed_line

    def load_rows(self, stream, raw=None):
        while True:
            row = self.load_row(stream, raw=raw)
            if row is None:
                break
            yield row

    def dump_row(self, row, stream):
        def escape_key(string):
            if not self.enable_escaping:
                return string
            return self._escape(string, {b'\n': b'\\n', b'\r': b'\\r', b'\t': b'\\t', b'\0': b'\\0', b'=': b'\\='})

        def escape_value(string):
            if not self.enable_escaping:
                return string
            return self._escape(string, {b'\n': b'\\n', b'\r': b'\\r', b'\t': b'\\t', b'\0': b'\\0'})

        def convert_to_bytes(value):
            if not PY3:  # Fast path
                return str(value)

            if isinstance(value, (text_type, binary_type)):
                return self._encode_string(value)
            else:
                return str(value).encode("ascii")

        # NOTE: Statbox bindings work with byte strings only. But
        # it is likely that row will contain unicode key or value in Python 3
        # so bindings are disabled for Python 3.
        if sb_misc is not None and self._encoding is None and not PY3:
            stream.write(sb_misc.tskv_encode(row))
            stream.write(b"\n")
        else:
            length = len(row)
            for i, item in enumerate(iteritems(row)):
                stream.write(escape_key(convert_to_bytes(item[0])))
                stream.write(b"=")
                stream.write(escape_value(convert_to_bytes(item[1])))
                stream.write(b"\n" if i == length - 1 else b"\t")

    def _dump_rows(self, rows, stream_or_streams):
        stream = stream_or_streams[0] if isinstance(stream_or_streams, list) else stream_or_streams
        for row in rows:
            self.dump_row(row, stream)

    def loads_row(self, string):
        if sb_simple_parsers is not None and self._encoding is None:
            return sb_simple_parsers.parse_tskv_chomp(string)
        return self._parse(string)

    def _parse(self, string):
        def decode(key, value):
            if self._encoding is None:
                return key, value
            return key.decode(self._encoding), value.decode(self._encoding)

        def unescape_dsv_field(field):
            if not self.enable_escaping:
                return decode(*field.split(b"=", 1))

            tokens = field.split(b"\\\\")
            key_tokens = []
            value_tokens = []
            inside_key = True
            for token in tokens:
                if inside_key:
                    index = -1
                    while True:
                        index = token.find(b"=", index + 1)
                        if index == -1:
                            key_tokens.append(token)
                            break
                        if index == 0 or int2byte(indexbytes(token, index - 1)) != b"\\":
                            key_tokens.append(token[:index])
                            value_tokens.append(token[index + 1:])
                            inside_key = False
                            break
                else:
                    value_tokens.append(token)

            value_dict = {b'\\n': b'\n', b'\\r': b'\r', b'\\t': b'\t', b'\\0': b'\0'}
            key_dict = copy.deepcopy(value_dict)
            key_dict[b'\\='] = b'='

            key = b"\\".join(imap(lambda t: self._unescape(t, key_dict), key_tokens))
            value = b"\\".join(imap(lambda t: self._unescape(t, value_dict), value_tokens))
            return decode(key, value)

        return dict(imap(unescape_dsv_field, ifilter(None, string.strip(b"\n").split(b"\t"))))

class YsonFormat(Format):
    """Main and default YT data format.

    .. seealso:: `YSON on wiki <https://wiki.yandex-team.ru/yt/userdoc/formats#yson>`_
    """

    def __init__(self, format=None, process_table_index=None, control_attributes_mode=None,
                 ignore_inner_attributes=None, boolean_as_string=None, table_index_column="@table_index",
                 attributes=None, raw=None, always_create_attributes=None, encoding=_ENCODING_SENTINEL,
                 require_yson_bindings=None):
        """
        :param str format: output format (must be one of ["text", "pretty", "binary"], "text" be default).
        :param bool process_table_index: DEPRECATED! process input and output table switchers in `dump_rows`\
         and `load_rows`. See `wiki <https://wiki.yandex-team.ru/yt/userdoc/tableswitch#yson>`_.
        :param str control_attributes_mode: mode of processing rows with control attributes, must be one of \
        ["row_fields", "iterator", "none"]. In "row_fields" mode attributes are put in the regular rows with \
        as "@row_index", "@range_index" and "@table_index". Also "@table_index" key is parsed from output rows. \
        In "iterator" mode attributes rows object is iterator and control attributes are available as fields of the iterator. \
        In "none" mode rows are unmodified.
        """
        defaults = {"boolean_as_string": False,
                    "ignore_inner_attributes": False,
                    "always_create_attributes": False,
                    "format": "binary"}
        options = {"boolean_as_string": boolean_as_string,
                   "ignore_inner_attributes": ignore_inner_attributes,
                   "always_create_attributes": always_create_attributes,
                   "format": format}

        all_attributes = Format._make_attributes(get_value(attributes, {}), defaults, options)
        super(YsonFormat, self).__init__("yson", all_attributes, raw, encoding)

        if control_attributes_mode is None:
            # Deprecated, only for transition period.
            control_attributes_mode = os.environ.get("YT_YSON_CONTROL_ATTRIBUTES_MODE")
        if control_attributes_mode is None:
            control_attributes_mode = "iterator"

        if control_attributes_mode not in ["row_fields", "iterator", "none"]:
            raise YtFormatError("Incorrect control attributes mode: {0}".format(control_attributes_mode))

        self.process_table_index = process_table_index
        self.control_attributes_mode = control_attributes_mode
        self.table_index_column = table_index_column

        self.require_yson_bindings = get_value(require_yson_bindings, self.attributes.get("require_yson_bindings", True))
        if "require_yson_bindings" in self.attributes:
            del self.attributes["require_yson_bindings"]

        self._coerced_table_index_column = self._coerce_column_key(table_index_column)

    def _check_bindings(self):
        if self.require_yson_bindings and yson.TYPE != "BINARY":
            raise YtFormatError('YSON bindings required. Try to use other format or install bindings. '
                                'Bindings are shipped as additional package and '
                                'can be installed as Debian package "yandex-yt-python-yson" or as pip '
                                'package "yandex-yt-yson-bindings"')

    def load_row(self, stream, raw=None):
        """Not supported."""
        raise YtFormatError("load_row is not supported in Yson")

    def load_rows(self, stream, raw=None):
        def control_attributes_extractor(row):
            if isinstance(row, yson.YsonEntity):
                return row.attributes

        self._check_bindings()
        rows = yson.load(stream,
                         yson_type="list_fragment",
                         always_create_attributes=self.attributes["always_create_attributes"],
                         raw=raw,
                         encoding=self._encoding)
        if raw:
            return rows
        else:
            return self._process_input_rows(rows, self.process_table_index, self.control_attributes_mode,
                                            control_attributes_extractor, self._coerced_table_index_column,
                                            self._coerce_column_key)

    def _dump_row(self, row, stream):
        self._check_bindings()
        self._dump_rows([row], stream)

    def _process_output_rows(self, rows):
        table_index = 0
        table_index_column = self._coerce_column_key("table_index")

        for row in rows:
            new_table_index = row.get(self._coerced_table_index_column, 0)
            if new_table_index is not None and new_table_index != table_index:
                attributes = {table_index_column: new_table_index}
                yield yson.to_yson_type(None, attributes=attributes)
                table_index = new_table_index
            if hasattr(row, "attributes"):
                row.attributes = {}
            row.pop(self._coerced_table_index_column, None)
            yield row

    def _dump_rows(self, rows, stream_or_streams):
        self._check_bindings()
        if (self.process_table_index is None and self.control_attributes_mode == "row_fields") or self.process_table_index:
            rows = self._process_output_rows(rows)

        kwargs = {}
        if yson.TYPE == "BINARY":
            kwargs = {"ignore_inner_attributes": self.attributes["ignore_inner_attributes"]}
        if PY3:
            kwargs["encoding"] = self._encoding

        if not isinstance(stream_or_streams, list):
            yson.dump(rows, stream_or_streams,
                      yson_type="list_fragment",
                      yson_format=self.attributes["format"],
                      boolean_as_string=self.attributes["boolean_as_string"],
                      **kwargs)
            return

        table_index = 0
        for row in rows:
            if isinstance(row, yson.YsonEntity):
                if self._coerced_table_index_column in row.attributes:
                    table_index = row.attributes[self._coerced_table_index_column]
                    self._check_output_table_index(table_index, len(stream_or_streams))
                    continue

            yson.dump([row], stream_or_streams[table_index],
                      yson_type="list_fragment",
                      yson_format=self.attributes["format"],
                      boolean_as_string=self.attributes["boolean_as_string"],
                      **kwargs)

class YamrFormat(Format):
    """YAMR legacy data format.

    Supported two mutually exclusive modes: text mode with delimiter and \
    binary mode ('lenval') with length before each field.

    .. note:: In delimiter mode implemented just standard delimiter "\\\\t" and terminator "\\\\n".
    .. seealso:: `YAMR on wiki <https://wiki.yandex-team.ru/yt/userdoc/formats#yamr>`_
    """

    def __init__(self, has_subkey=None, lenval=None,
                 field_separator=None, record_separator=None,
                 enable_table_index=None, attributes=None, raw=None, encoding=_ENCODING_SENTINEL):
        """
        :param bool has_subkey: `False` by default.
        :param bool lenval: `False` by default.
        :param str field_separator: "\\\\t" by default.
        :param str record_separator: is not implemented yet! "\\\\n" by default.
        :param bool enable_table_index: specify it for table switching in load_rows.
        """
        defaults = {"has_subkey": False, "lenval": False, "fs": '\t', "rs": '\n',
                    "enable_table_index": False}
        options = {"has_subkey": has_subkey, "lenval": lenval,
                   "fs": field_separator, "rs": record_separator,
                   "enable_table_index": enable_table_index}
        attributes = get_value(attributes, {})
        all_attributes = Format._make_attributes(attributes, defaults, options)

        for sep in ["fs", "rs"]:  # For Python 3
            setattr(self, "_" + sep, all_attributes[sep].encode("ascii"))

        super(YamrFormat, self).__init__("yamr", all_attributes, raw, encoding)
        self._load_row = self._read_lenval_values if self.lenval else self._read_delimited_values
        self.fields_number = 3 if self.has_subkey else 2

    has_subkey = Format._create_property("has_subkey", parse_bool)
    lenval = Format._create_property("lenval", parse_bool)
    field_separator = Format._create_property("fs")
    record_separator = Format._create_property("rs")

    def __deepcopy__(self, memodict=None):
        # Fix python2.6 bug http://bugs.python.org/issue1515
        result = type(self)()

        for attr_name, attr_value in iteritems(self.__dict__):
            if attr_name == "_load_row":
                continue
            setattr(result, attr_name, copy.deepcopy(attr_value, memo=memodict))

        if result.lenval:
            result._load_row = result._read_lenval_values
        else:
            result._load_row = result._read_delimited_values

        return result

    def load_row(self, stream, raw=None):
        unparsed = self._is_raw(raw)
        result_of_loading = self._load_row(stream, unparsed)

        if unparsed:
            return result_of_loading

        if not result_of_loading or type(result_of_loading[0]) is int:
            return None

        if self._encoding is not None:
            result_of_loading = list(imap(lambda field: field.decode(self._encoding), result_of_loading))

        return Record(*result_of_loading)

    def load_rows(self, stream, raw=None):
        self._table_index_read = False

        unparsed = self._is_raw(raw)
        # NB: separate processing of unparsed mode for optimization
        if unparsed and not self.lenval:
            prefix = []
            while True:
                chunk = stream.read(1024 * 1024)
                if not chunk:
                    break
                lines = chunk.split(self._rs)
                if len(lines) == 1:
                    prefix.append(lines[0])
                else:
                    yield b"".join(prefix + [lines[0]]) + self._rs
                    for line in lines[1:-1]:
                        yield line + self._rs
                    if lines[-1]:
                        prefix = [lines[-1]]
                    else:
                        prefix = []
            if prefix:
                if len(prefix) > 1:
                    yield b"".join(prefix) + self._rs
                else:
                    yield prefix[0] + self._rs
            return

        table_index = 0
        row_index = None
        while True:
            row = self._load_row(stream, unparsed=unparsed)
            if unparsed and row:
                yield row
                continue

            fields = row
            if not fields:
                break
            if fields[0] == -1:
                table_index = fields[1]
                continue
            if fields[0] == -4:
                row_index = fields[1]
                continue

            if self._encoding is not None:
                fields = list(imap(lambda s: s.decode(self._encoding), fields))

            yield Record(*fields, tableIndex=table_index, recordIndex=row_index)

            if row_index is not None:
                row_index += 1

    def _read_delimited_values(self, stream, unparsed):
        if self._rs != b'\n':
            raise NotImplementedError("Implemented just for standard terminator ('\\n')")
        row = stream.readline()
        if not row:
            return None
        if unparsed:
            return row
        fields = row.rstrip(b"\n").split(self._fs, self.fields_number - 1)
        if len(fields) == 1:
            index = int(fields[0])
            if not hasattr(self, "_table_index_read"):
                self._table_index_read = False
            if self._table_index_read:
                return (-4, index)
            else:
                self._table_index_read = True
                return (-1, index)

        self._table_index_read = False
        return fields

    def _read_lenval_values(self, stream, unparsed):
        fields = []
        for iter in xrange(self.fields_number):
            len_bytes = stream.read(4)
            if iter == 0 and not len_bytes:
                return None

            if unparsed:
                fields.append(len_bytes)

            try:
                length = struct.unpack('i', len_bytes)[0]
                if length < 0:
                    if unparsed:
                        return len_bytes + stream.read(4)
                    field = stream.read(4)
                    return (length, struct.unpack("i", field)[0])
            except struct.error:
                raise YtFormatError("Incomplete record in yamr lenval")

            field = stream.read(length)
            if len(field) != length:
                 raise YtFormatReadError("Incorrect length field in yamr lenval,\
                                          expected {0}, received {1}".format(length, len(field)))
            fields.append(field)
        if unparsed:
            return b''.join(fields)
        return fields

    def _dump_row(self, row, stream):
        fields = row.items()

        if self.lenval:
            for field in fields:
                encoded_field = self._encode_string(field)
                stream.write(struct.pack("i", len(encoded_field)))
                stream.write(encoded_field)
        else:
            for index, field in enumerate(fields):
                stream.write(self._encode_string(field))
                if index != len(fields) - 1:
                    stream.write(self._fs)
                else:
                    stream.write(self._rs)

    def _dump_rows(self, rows, stream_or_streams):
        if isinstance(stream_or_streams, list):
            for row in rows:
                table_index = row.tableIndex
                self._check_output_table_index(table_index, len(stream_or_streams))
                self.dump_row(row, stream_or_streams[table_index])
            return

        table_index = 0
        for row in rows:
            new_table_index = row.tableIndex
            if new_table_index != table_index:
                if self.lenval:
                    table_switcher = struct.pack("ii", -1, new_table_index)
                else:
                    table_switcher = "{0}\n".format(new_table_index)
                    if PY3:
                        table_switcher = table_switcher.encode("ascii")
                stream_or_streams.write(table_switcher)
                table_index = new_table_index

            self.dump_row(row, stream_or_streams)

class JsonFormat(Format):
    """Open standard text data format for attribute-value data.

    .. seealso:: `JSON on wiki <https://wiki.yandex-team.ru/yt/userdoc/formats#json>`_
    """
    _ENCODING = "utf-8" if PY3 else None

    @staticmethod
    def _wrap_json_module(json_module):
        return _AttributeDict({
            "load": json_module.load,
            "loads": json_module.loads,
            "dump": json_module.dump,
            "dumps": json_module.dumps,
        })

    @staticmethod
    def _get_json_module(enable_ujson):
        module = json
        if enable_ujson:
            try:
                import ujson
                module = ujson
            except ImportError:
                pass

        return JsonFormat._wrap_json_module(module)

    def __init__(self, process_table_index=None, control_attributes_mode="generator",
                 table_index_column="@table_index", attributes=None, raw=None, enable_ujson=False):
        """
        :param bool process_table_index: DEPRECATED! process input and output table switchers in `dump_rows`\
         and `load_rows`. See `wiki <https://wiki.yandex-team.ru/yt/userdoc/tableswitch#yson>`_.
        :param str control_attributes_mode: mode of processing rows with control attributes, must be one of \
        ["row_fields", "iterator", "generator", "none"]. In "row_fields" mode attributes are put in the regular rows with \
        as "@row_index", "@range_index" and "@table_index". Also "@table_index" key is parsed from output rows. \
        In "iterator" mode attributes rows object is iterator and control attributes are available as fields of the iterator. \
        In "none" (or deprecated "generator") mode rows are unmodified.
        """
        attributes = get_value(attributes, {})
        super(JsonFormat, self).__init__("json", attributes, raw, self._ENCODING)
        self.process_table_index = process_table_index
        self.control_attributes_mode = control_attributes_mode
        self.table_index_column = table_index_column

        self.enable_ujson = enable_ujson and not PY3
        self.json_module = JsonFormat._get_json_module(enable_ujson=enable_ujson)

    def _loads(self, string, raw):
        if raw:
            return string
        string = string.rstrip(b"\n")
        if PY3:
            string = string.decode(self._encoding)
        return self.json_module.loads(string)

    def _dump(self, obj, stream):
        writer = lambda stream: stream
        if PY3:
            writer = getwriter(self._encoding)
        return self.json_module.dump(obj, writer(stream))

    def _dumps(self, obj):
        value = self.json_module.dumps(obj)
        if PY3:
            value = value.encode(self._encoding)
        return value

    def load_row(self, stream, raw=None):
        row = stream.readline()
        if not row:
            return None
        return self._loads(row, self._is_raw(raw))

    def load_rows(self, stream, raw=None):
        raw = self._is_raw(raw)

        def _load_rows(stream):
            for line in stream:
                yield self._loads(line, raw)

        def control_attributes_extractor(row):
            if "$value" in row:
                if row["$value"] is not None:
                    raise YtFormatError("Incorrect $value of table switch in JSON format")
                return row["$attributes"]

        rows = _load_rows(stream)
        if raw:
            return rows
        else:
            return self._process_input_rows(rows, self.process_table_index, self.control_attributes_mode,
                                            control_attributes_extractor, self.table_index_column,
                                            to_native_str)

    def _dump_row(self, row, stream):
        self._dump(row, stream)
        stream.write(b"\n")

    def _process_output_rows(self, rows):
        table_index = 0
        for row in rows:
            new_table_index = row[self.table_index_column] \
                if self.table_index_column in row \
                else 0
            if new_table_index is not None and new_table_index != table_index:
                table_index = new_table_index
                yield {"$value": None, "$attributes": {"table_index": table_index}}
            if self.table_index_column in row:
                del row[self.table_index_column]
            yield row

    def _dump_rows(self, rows, stream_or_streams):
        if (self.process_table_index is None and self.control_attributes_mode == "row_fields") or self.process_table_index:
            rows = self._process_output_rows(rows)

        if not isinstance(stream_or_streams, list):
            for row in rows:
                self.dump_row(row, stream_or_streams)
            return

        table_index = 0
        for row in rows:
            if "$attributes" in row and "table_index" in row["$attributes"]:
                table_index = row["$attributes"]["table_index"]
                self._check_output_table_index(table_index, len(stream_or_streams))
                continue

            self.dump_row(row, stream_or_streams[table_index])

    def dumps_row(self, row):
        return self._dumps(row)

    def loads_row(self, string):
        return self._loads(string, raw=False)

    def __getstate__(self):
        attrs = self.__dict__.copy()
        del attrs["json_module"]
        return attrs

    def __setstate__(self, d):
        self.__dict__.update(d)
        self.json_module = JsonFormat._get_json_module(self.enable_ujson)

class YamredDsvFormat(YamrFormat):
    """Hybrid of Yamr and DSV formats. It is used to support yamr representations of tabular data.

    .. seealso:: `Yamred DSV on wiki <https://wiki.yandex-team.ru/yt/userdoc/formats#yamreddsv>`_
    """

    def __init__(self, key_column_names=None, subkey_column_names=None,
                 has_subkey=None, lenval=None, attributes=None, raw=None, encoding=_ENCODING_SENTINEL):
        """
        :param key_column_names: key column names.
        :type key_column_names: list[str]
        :param subkey_column_names: subkey column names.
        :type subkey_column_names: list[str]
        :param bool has_subkey: has subkey.
        :param bool lenval: lenval.
        """
        defaults = {"has_subkey": False, "lenval": False,
                    "key_column_names": [], "subkey_column_names": []}
        options = {"key_column_names": key_column_names, "subkey_column_names": subkey_column_names,
                   "has_subkey": has_subkey, "lenval": lenval}
        attributes = get_value(attributes, {})
        all_attributes = Format._make_attributes(attributes, defaults, options)
        require("key_column_names" in all_attributes,
                lambda: YtFormatError("YamredDsvFormat requires 'key_column_names' attribute"))
        super(YamredDsvFormat, self).__init__(attributes=all_attributes, raw=raw, encoding=encoding)
        self._name = yson.to_yson_type("yamred_dsv", self.attributes)

    key_column_names = Format._create_property("key_column_names")

    subkey_column_names = Format._create_property("subkey_column_names")

class SchemafulDsvFormat(Format):
    """Schemaful dsv format. It accepts column names and outputs values of these columns.

    .. seealso:: `SchemafulDsvFormat on wiki \
    <https://wiki.yandex-team.ru//yt/userdoc/formats#schemafuldsv>`_
    """

    def __init__(self, columns=None, enable_escaping=None, enable_table_index=None, table_index_column=None,
                 attributes=None, raw=None, encoding=_ENCODING_SENTINEL):
        """
        :param columns: mandatory parameter!
        :type columns: list[str]
        :param bool enable_escaping: process escaped symbols, `True` by default.
        :param bool enable_table_index: process input table indexes in load_rows and \
        output table indexes in dump_rows, `False` by default.
        :param str table_index_column: name for special row field (exists only inside \
        operation binary!), "@table_index" by default.
        """
        defaults = {"enable_escaping": True, "enable_table_index": False,
                    "table_index_column": "@table_index"}
        options = {"columns": columns,
                   "enable_escaping": enable_escaping,
                   "enable_table_index": enable_table_index,
                   "table_index_column": table_index_column}
        attributes = get_value(attributes, {})
        all_attributes = Format._make_attributes(attributes, defaults, options)
        super(SchemafulDsvFormat, self).__init__("schemaful_dsv", all_attributes, raw, encoding)
        if self.enable_table_index:
            self._columns = [self.attributes["table_index_column"]] + self.columns
        else:
            self._columns = self.columns

        self._coerced_columns = list(imap(self._coerce_column_key, self._columns))
        self._coerced_table_index_column = self._coerce_column_key(self.attributes["table_index_column"])

    columns = Format._create_property("columns")

    enable_escaping = Format._create_property("enable_escaping", parse_bool)

    enable_table_index = Format._create_property("enable_table_index", parse_bool)

    table_index_column = Format._create_property("table_index_column")

    def load_row(self, stream, raw=None):
        line = stream.readline()

        if not line:
            return None

        if self._is_raw(raw):
            return line

        parsed_line = self._parse(line)
        if self.enable_table_index:
            parsed_line[self._coerced_table_index_column] = int(parsed_line[self._coerced_table_index_column])

        return parsed_line

    def load_rows(self, stream, raw=None):
        while True:
            row = self.load_row(stream, raw)
            if not row:
                break
            yield row

    def _dump_row(self, row, stream):
        def escape(string):
            if not self.enable_escaping:
                return string
            return self._escape(string, {b'\n': b'\\n', b'\r': b'\\r', b'\t': b'\\t', b'\0': b'\\0'})

        def convert_table_index_to_str(row):
            index_str = str(row[self._coerced_table_index_column])
            if PY3 and self._encoding is None:
                index_str = index_str.encode("ascii")
            row[self._coerced_table_index_column] = index_str

        if self.enable_table_index:
            convert_table_index_to_str(row)

        for index, key in enumerate(self._coerced_columns):
            value = row[key]
            stream.write(escape(self._encode_string(value)))
            if index != len(self._coerced_columns) - 1:
                stream.write(b"\t")
            else:
                stream.write(b"\n")

    def _dump_rows(self, rows, stream_or_streams):
        for row in rows:
            stream = stream_or_streams[0] if isinstance(stream_or_streams, list) else stream_or_streams
            self._dump_row(row, stream)

    def _parse(self, line):
        def decode(value):
            if self._encoding is not None:
                return value.decode(self._encoding)
            return value

        def unescape_field(field):
            if not self.enable_escaping:
                return field
            unescape_dict = {b'\\n': b'\n', b'\\r': b'\r', b'\\t': b'\t', b'\\0': b'\0'}
            return decode(b"\\".join(imap(lambda token: self._unescape(token, unescape_dict),
                                          field.split(b"\\\\"))))

        return dict(izip(imap(self._coerce_column_key, self._columns),
                         imap(unescape_field, line.rstrip(b"\n").split(b"\t"))))

# TODO(veronikaiv): do it beautiful way!
Format._copy_docs()

def create_format(yson_name, attributes=None, **kwargs):
    """Creates format by YSON string.

    :param yson_name: YSON string like ``'<lenval=false;has_subkey=false>yamr'``.
    :param attributes: Deprecated! Don't use it! It will be removed!
    """
    if attributes is not None:
        logger.warning("Usage deprecated parameter 'attributes' of create_format. "
                       "It will be removed!")
    else:
        attributes = {}

    yson_string = yson._loads_from_native_str(yson_name)
    attributes.update(yson_string.attributes)
    name = str(yson_string)

    NAME_TO_FORMAT = {
        "yamr": YamrFormat,
        "dsv": DsvFormat,
        "yamred_dsv": YamredDsvFormat,
        "schemaful_dsv": SchemafulDsvFormat,
        "yson": YsonFormat,
        "json": JsonFormat
    }

    if name not in NAME_TO_FORMAT:
        raise YtFormatError("Incorrect format " + name)

    return NAME_TO_FORMAT[name](attributes=attributes, **kwargs)

def loads_row(string, format=None, client=None):
    """Converts string to parsed row."""
    format = get_value(format, get_config(client)["tabular_data_format"])
    return format.loads_row(string)

def dumps_row(row, format=None, client=None):
    """Converts parsed row to string."""
    format = get_value(format, get_config(client)["tabular_data_format"])
    return format.dumps_row(row)

def extract_key(rec, fields):
    if isinstance(rec, SimpleRecord) or isinstance(rec, SubkeyedRecord):
        return rec.key
    else:
        return FrozenDict((key, rec[key]) for key in fields if key in rec)

def create_table_switch(table_index):
    """Returns YSON that represents table switch row."""
    table_switch = yson.YsonEntity()
    table_switch.attributes["table_index"] = table_index
    return table_switch

