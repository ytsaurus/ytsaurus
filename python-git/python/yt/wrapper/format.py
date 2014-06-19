"""YT data formats

.. note:: In `Format` descendants constructors default parameters are overridden by `attributes` parameters,\
 and then by kwargs options.
"""

import format_config
from common import get_value, require, filter_dict, merge_dicts
from errors import YtError, YtFormatError
from yamr_record import Record
import yt.yson as yson
import yt.logger as logger

from abc import ABCMeta, abstractmethod
import copy
import struct
import itertools
import simplejson as json
from cStringIO import StringIO

class Format(object):
    """ YT data representations.

        Abstract base class for different formats.
    """
    __metaclass__ = ABCMeta

    def __init__(self, name, attributes=None):
        """
        :param name: (string) format name
        :param attributes: (dict) format parameters
        """
        require(isinstance(name, str), YtFormatError("Incorrect format %r" % name))
        self._name = yson.YsonString(name)
        self._name.attributes = get_value(attributes, {})

    def json(self):
        """
        Return JSON representation of format.
        """
        return yson.yson_to_json(self._name)

    def name(self):
        """
        Return string name of format.
        """
        return str(self._name)

    def _get_attributes(self):
        return self._name.attributes

    def _set_attributes(self, value):
        self._name.attributes = value

    attributes = property(_get_attributes, _set_attributes)

    def __repr__(self):
        return yson.dumps(self._name)

    def __eq__(self, other):
        if isinstance(other, Format):
            return self._name == other._name
        return False

    def __ne__(self, other):
        return not self.__eq__(other)

    @staticmethod
    def _escape(string, escape_dict):
        string = string.replace("\\", "\\\\")
        for sym, escaped in escape_dict.items():
            string = string.replace(sym, escaped)
        return string

    @staticmethod
    def _unescape(string, escape_dict):
        for sym, unescaped in escape_dict.items():
            string = string.replace(sym, unescaped)
        return string.replace("\\", "")

    @abstractmethod
    def load_row(self, stream, unparsed=False):
        """Read from the stream, parse (optionally) and return one row"""
        pass

    @abstractmethod
    def load_rows(self, stream):
        """Read from the stream, parse and yield all rows"""
        pass

    @abstractmethod
    def dump_row(self, row, stream):
        """Serialize row and write to the stream."""
        pass

    @abstractmethod
    def dump_rows(self, rows, stream):
        """Serialize rows and write to the stream"""
        pass

    def dumps_row(self, row):
        """Convert parsed row to string"""
        stream = StringIO()
        self.dump_row(row, stream)
        return stream.getvalue()

    def loads_row(self, string):
        """Convert string to parsed row"""
        stream = StringIO(string)
        return self.load_row(stream)

    @staticmethod
    def _create_property(property_name):
        get_func = lambda self: self.attributes[property_name]
        set_func = lambda self, value: self.attributes.update({property_name: value})
        return property(get_func, set_func)

    @staticmethod
    def _make_attributes(attributes, defaults, options):
        not_none_options = filter_dict(lambda key, value: value is not None, options)
        return merge_dicts(defaults, attributes, not_none_options)

    def is_read_row_supported(self):
        """.. note:: Deprecated."""
        return self.name() in ("dsv", "yamr", "yamred_dsv")

    def read_row(self, stream):
        """.. note:: Deprecated. Use instead load_row method with parameter unparsed=True"""
        return self.load_row(stream, unparsed=True)

    @staticmethod
    def _copy_docs():
        """Magic for coping docs in subclasses.

        Call once after creating all subclasses."""
        for cl in Format.__subclasses__():
            cl_dict = cl.__dict__
            for name in ('load_row', 'load_rows', 'dump_row', 'dump_rows', 'loads_row', 'dumps_row'):
                if name in cl_dict and not cl_dict[name].__doc__:
                    cl_dict[name].__doc__ = Format.__dict__[name].__doc__


class DsvFormat(Format):
    """
    Statbox favorite data format a la ``'time=10\\tday=monday\\n'``.

    .. seealso:: `DSV on wiki <https://wiki.yandex-team.ru/yt/userdoc/formats/#dsv>`_
    """

    def __init__(self, enable_escaping=None, attributes=None):
        all_attributes = Format._make_attributes(get_value(attributes, {}),
                                                 {"enable_escaping": True},
                                                 {"enable_escaping": enable_escaping})
        super(DsvFormat, self).__init__("dsv", all_attributes)


    enable_escaping = Format._create_property("enable_escaping")

    def load_row(self, stream, unparsed=False):
        line = stream.readline()
        if unparsed:
            return line
        return self._parse(line)

    def load_rows(self, stream):
        for line in stream:
            yield self._parse(line)

    def dump_row(self, row, stream):
        def escape_key(string):
            if not self.enable_escaping:
                return string
            return self._escape(string, {'\n': '\\n', '\r': '\\r', '\t': '\\t', '\0': '\\0', '=': '\\='})

        def escape_value(string):
            if not self.enable_escaping:
                return string
            return self._escape(string, {'\n': '\\n', '\r': '\\r', '\t': '\\t', '\0': '\\0'})

        length = len(row)
        for i, item in enumerate(row.iteritems()):
            stream.write(escape_key(str(item[0])))
            stream.write("=")
            stream.write(escape_value(str(item[1])))
            stream.write("\n" if i == length - 1 else "\t")

    def dump_rows(self, rows, stream):
        for row in rows:
            self.dump_row(row, stream)

    def loads_row(self, string):
        return self._parse(string)

    def _parse(self, string):
        def unescape_dsv_field(field):
            if not self.enable_escaping:
                return field.split("=", 1)

            tokens = field.split("\\\\")
            key_tokens = []
            value_tokens = []
            inside_key = True
            for token in tokens:
                if inside_key:
                    index = -1
                    while True:
                        index = token.find("=", index + 1)
                        if index == -1:
                            key_tokens.append(token)
                            break
                        if index == 0 or token[index - 1] != "\\":
                            key_tokens.append(token[:index])
                            value_tokens.append(token[index + 1:])
                            inside_key = False
                            break
                else:
                    value_tokens.append(token)

            value_dict = {'\\n': '\n', '\\r': '\r', '\\t': '\t', '\\0': '\0'}
            key_dict = copy.deepcopy(value_dict)
            key_dict['\\='] = '='

            return ["\\".join(map(lambda t: self._unescape(t, key_dict), key_tokens)),
                    "\\".join(map(lambda t: self._unescape(t, value_dict), value_tokens))]

        return dict(map(unescape_dsv_field, filter(None, string.strip("\n").split("\t"))))

class YsonFormat(Format):
    """
    Main, default and the best YT data format.

    .. seealso:: `YSON on wiki <https://wiki.yandex-team.ru/yt/userdoc/formats#yson>`_
    """

    def __init__(self, format=None, attributes=None):
        all_attributes = Format._make_attributes(get_value(attributes, {}),
                                                 {"format": "text"},
                                                 {"format": format})
        super(YsonFormat, self).__init__("yson", all_attributes)

    def load_row(self, stream, unparsed=False):
        raise YtFormatError("load_row is not supported in Yson")

    def load_rows(self, stream):
        return yson.load(stream, yson_type="list_fragment")

    def dump_row(self, row, stream):
        # write ';' ?
        yson.dump(row, stream)

    def dump_rows(self, rows, stream):
        yson.dump(rows, stream, yson_type="list_fragment")

class YamrFormat(Format):
    """
    YAMR legacy data format.

    .. seealso:: `YAMR on wiki <https://wiki.yandex-team.ru/yt/userdoc/formats#yamr>`_
    """

    def __init__(self, has_subkey=None, lenval=None, field_separator=None, record_separator=None, attributes=None):
        defaults = {"has_subkey": False, "lenval": False, "fs": '\t', "rs": '\n'}
        options = {"has_subkey": has_subkey, "lenval": lenval, "fs": field_separator, "rs": record_separator}
        attributes = get_value(attributes, {})
        all_attributes = Format._make_attributes(attributes, defaults, options)
        super(YamrFormat, self).__init__("yamr", all_attributes)

    has_subkey = Format._create_property("has_subkey")

    lenval = Format._create_property("lenval")

    def load_row(self, stream, unparsed=False):
        if unparsed:
            if self.lenval:
                fields = self._read_lenval(stream, unparsed=True)
            else:
                fields = stream.readline()

            if not fields:
                return None
            return fields

        if self.lenval:
            fields = self._read_lenval(stream, unparsed=False)
            if not fields:
                return None
        else:
            fields = stream.readline()
            # empty string splits to non empty list, so this check should be here
            if not fields:
                return None
            fields = fields.rstrip("\n").split("\t", self._get_field_count() - 1)
        return Record(*fields)

    def load_rows(self, stream):
        while True:
            row = self.load_row(stream)
            if row is None:
                break
            yield row

    def dump_row(self, row, stream):
        rec = row
        if self.has_subkey:
            fields = [rec.key, rec.subkey, rec.value]
        else:
            fields = [rec.key, rec.value]
        if self.lenval:
            for field in fields:
                stream.write(struct.pack("i", len(field)))
                stream.write(field)
        else:
            for i, field in enumerate(fields):
                stream.write(field)
                if i == len(fields) - 1:
                    stream.write("\n")
                else:
                    stream.write("\t")

    def dump_rows(self, rows, stream):
        for row in rows:
            self.dump_row(row, stream)

    def _get_field_count(self):
        return 3 if self.has_subkey else 2

    def _read_lenval(self, stream, unparsed=False):
        fields = []

        result = StringIO()
        for iter in xrange(self._get_field_count()):
            len_bytes = stream.read(4)
            if not len_bytes:
                if iter > 0:
                    raise YtError("Incomplete record in yamr lenval")
                return ""
            result.write(len_bytes)
            length = struct.unpack('i', len_bytes)[0]
            field = stream.read(length)
            if len(field) != length:
                raise YtError("Incorrect length field in yamr lenval, expected {0}, received {1}".format(length, len(field)))
            if unparsed:
                result.write(field)
            else:
                fields.append(field)

        if unparsed:
            return result.getvalue()
        else:
            return fields

class JsonFormat(Format):
    """
    Open standard text data format for attribute-value data.

    .. seealso:: `JSON on wiki <https://wiki.yandex-team.ru/yt/userdoc/formats#json>`_
    """

    def __init__(self, attributes=None):
        attributes = get_value(attributes, {})
        super(JsonFormat, self).__init__("json", attributes)

    def load_row(self, stream, unparsed=False):
        row = stream.readline()
        if unparsed:
            return row
        return json.loads(row.rstrip("\n"))

    def load_rows(self, stream):
        for line in stream:
            yield json.loads(line)

    def dump_row(self, row, stream):
        json.dump(row, stream)
        stream.write("\n")

    def dump_rows(self, rows, stream):
        for row in rows:
            self.dump_row(row, stream)

    def dumps_row(self, row):
        return json.dumps(row)

    def loads_row(self, string):
        return json.loads(string)

class YamredDsvFormat(YamrFormat):
    """
    Evil tabular data format for YAMR data.

    .. seealso:: `Yamred DSV on wiki <https://wiki.yandex-team.ru/yt/userdoc/formats#yamreddsv>`_
    """

    def __init__(self, key_column_names=None, subkey_column_names=None, has_subkey=None, lenval=None, attributes=None):
        defaults = {"has_subkey": False, "lenval": False, "subkey_column_names": []}
        options = {"key_column_names": key_column_names, "subkey_column_names": subkey_column_names,
                   "has_subkey": has_subkey, "lenval": lenval}
        attributes = get_value(attributes, {})
        all_attributes = Format._make_attributes(attributes, defaults, options)
        require(all_attributes.has_key("key_column_names"),
                YtFormatError("YamredDsvFormat require 'key_column_names' attribute"))
        super(YamredDsvFormat, self).__init__(attributes=all_attributes)
        self._name = yson.to_yson_type("yamred_dsv", self.attributes)


class SchemafulDsvFormat(Format):
    """
    Yet another DSV-like data format.

    .. seealso:: `SchemafulDsvFormat on wiki <https://wiki.yandex-team.ru/yt/userdoc/formats#schemeddsvschemafuldsv>`_
    """

    def __init__(self, columns=None, enable_escaping=None, attributes=None):
        defaults = {"enable_escaping": True}
        options = {"columns": columns, "enable_escaping": enable_escaping}
        attributes = get_value(attributes, {})
        all_attributes = Format._make_attributes(attributes, defaults, options)
        require(all_attributes.has_key("columns"),
                YtFormatError("SchemafulDsvFormat require 'columns' attribute"))
        super(SchemafulDsvFormat, self).__init__("schemaful_dsv", all_attributes)

    columns = Format._create_property("columns")

    enable_escaping = Format._create_property("enable_escaping")

    def load_row(self, stream, unparsed=False):
        line = stream.readline()
        if unparsed:
            return line
        if not line:
            return None
        return self._parse(line)

    def load_rows(self, stream):
        for line in stream:
            yield self._parse(line)

    def dump_row(self, row, stream):
        def escape(string):
            if not self.enable_escaping:
                return string
            return self._escape(string, {'\n': '\\n', '\r': '\\r', '\t': '\\t', '\0': '\\0'})

        for i, key in enumerate(self.columns):
            stream.write(escape(row[key]))
            if i == len(self.columns) - 1:
                stream.write("\n")
            else:
                stream.write("\t")

    def dump_rows(self, rows, stream):
        for row in rows:
            self.dump_row(row, stream)

    def _parse(self, line):
        def unescape_field(field):
            if not self.enable_escaping:
                return field
            unescape_dict = {'\\n': '\n', '\\r': '\r', '\\t': '\t', '\\0': '\0'}
            return "\\".join(map(lambda token: self._unescape(token, unescape_dict), field.split("\\\\")))

        return dict(itertools.izip(self.columns, map(unescape_field, line.rstrip("\n").split("\t"))))

class SchemedDsvFormat(SchemafulDsvFormat):
    """.. note:: Deprecated."""

    def __init__(self, columns, attributes=None):
        attributes = get_value(attributes, {})
        super(SchemedDsvFormat, self).__init__(columns=columns, attributes=attributes)
        self._name = yson.to_yson_type("schemed_dsv", self.attributes)

# TODO(veronikaiv): do it beautiful way!
Format._copy_docs()

def create_format(yson_name, attributes=None):
    """Create format by yson string.

    :param yson_name: YSON string like ``'<lenval=false;has_subkey=false>yamr'``
    :param attributes: Deprecated! Don't use it! It will be removed!
    """
    if attributes is not None:
        logger.warning("Usage deprecated parameter 'attributes' of create_format. It will be removed!")
    else:
        attributes = {}

    yson_string = yson.loads(yson_name)
    attributes.update(yson_string.attributes)
    name = str(yson_string)

    NAME_TO_FORMAT = {"yamr": YamrFormat,
                      "dsv": DsvFormat,
                      "yamred_dsv": YamredDsvFormat,
                      "schemaful_dsv": SchemafulDsvFormat,
                      "yson": YsonFormat,
                      "json": JsonFormat}
    try:
        return NAME_TO_FORMAT[name](attributes=attributes)
    except KeyError:
        raise YtFormatError("Incorrect format " + name)

def loads_row(string, format=None):
    """Convert string to parsed row"""
    format = get_value(format, format_config.TABULAR_DATA_FORMAT)
    return format.loads_row(string)

def dumps_row(row, format=None):
    """Convert parsed row to string"""
    format = get_value(format, format_config.TABULAR_DATA_FORMAT)
    return format.dumps_row(row)
