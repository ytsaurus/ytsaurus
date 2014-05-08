import config
from common import get_value, require, update
from errors import YtError
from yamr_record import Record
import yt.yson as yson

import copy
import struct
import itertools
import simplejson as json
from cStringIO import StringIO

class Format(object):
    """Format represented by raw description: name + attributes"""
    def __init__(self, name, attributes=None):
        require(isinstance(name, str), YtError("Incorrect format %r" % name))

        self._name = yson.YsonString(name)

        if attributes is None:
            attributes = {}
        self._name.attributes = attributes

    def json(self):
        return yson.yson_to_json(self._name)

    def name(self):
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

    def _escape(self, string, escape_dict):
        string = string.replace("\\", "\\\\")
        for sym, escaped in escape_dict.items():
            string = string.replace(sym, escaped)
        return string

    def _unescape(self, string, escape_dict):
        for sym, unescaped in escape_dict.items():
            string = string.replace(sym, unescaped)
        return string.replace("\\", "")

    # deprecated
    def is_read_row_supported(self):
        return self.name() in ["dsv", "yamr", "yamred_dsv"]

    # deprecated
    def read_row(self, stream):
        return self.load_unparsed_row(stream)

class DsvFormat(Format):
    def __init__(self, enable_escaping=True):
        self._enable_escaping = enable_escaping
        super(DsvFormat, self).__init__(
            "dsv",
            attributes={
                "enable_escaping": enable_escaping
            })

    def _get_enable_escaping(self):
        return self.attributes.get("enable_escaping", False)

    def _set_enable_escaping(self, value):
        self.attributes["enable_escaping"] = value

    enable_escaping = property(_get_enable_escaping, _set_enable_escaping)

    def load_unparsed_row(self, stream):
        return stream.readline()

    def load_row(self, stream):
        return self._parse(self.load_unparsed_row(stream))

    def loads_row(self, string):
        return self._parse(string)

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

    def dumps_row(self, row):
        stream = StringIO()
        self.dump_row(row, stream)
        return stream.getvalue()

    def dump_rows(self, rows, stream):
        for row in rows:
            self.dump_row(row, stream)

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
    def __init__(self, format=None):
        if format is None:
            format = "text"
        super(YsonFormat, self).__init__("yson", attributes={"format": format})

    def load_unparsed_row(self, stream):
        raise YtError("load_unparsed_row is not supported in Yson")

    def load_row(self, stream):
        raise YtError("load_row is not supported in Yson")

    def load_rows(self, stream):
        return yson.load(stream, yson_type="list_fragment")

    def dump_row(self, row, stream):
        # write ';' ?
        yson.dump(row, stream)

    def dump_rows(self, rows, stream):
        yson.dump(rows, stream, yson_type="list_fragment")


class YamrFormat(Format):
    def __init__(self, has_subkey=False, lenval=False, field_separator=None, record_separator=None):
        super(YamrFormat, self).__init__(
            "yamr",
            attributes={
                "fs": get_value(field_separator, '\t'),
                "rs": get_value(record_separator, '\n'),
                "has_subkey": has_subkey,
                "lenval": lenval
            })

    def _get_has_subkey(self):
        return self.attributes.get("has_subkey", False)

    def _set_has_subkey(self, value):
        self.attributes["has_subkey"] = value

    has_subkey = property(_get_has_subkey, _set_has_subkey)

    def _get_lenval(self):
        return self.attributes.get("lenval", False)

    def _set_lenval(self, value):
        self.attributes["lenval"] = value

    lenval = property(_get_lenval, _set_lenval)

    def load_unparsed_row(self, stream):
        if self.lenval:
            return self._read_lenval(stream, unparsed=True)
        else:
            return stream.readline()

    def load_row(self, stream):
        if self.lenval:
            fields = self._read_lenval(stream)
        else:
            fields = self._read_simple(stream)
        if not fields:
            return None
        return Record(*fields)

    def loads_row(self, string):
        stream = StringIO(string)
        return self.load_row(stream)

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

    def dumps_row(self, row):
        stream = StringIO()
        self.dump_row(row, stream)
        return stream.getvalue()

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

    def _read_simple(self, stream):
        line = stream.readline()
        if not line:
            return []
        return line.rstrip("\n").split("\t", self._get_field_count() - 1)


class JsonFormat(Format):
    def __init__(self):
        super(JsonFormat, self).__init__("json")

    def load_unparsed_row(self, stream):
        return stream.readline()

    def load_row(self, stream):
        return json.loads(stream.readline().rstrip("\n"))

    def loads_row(self, string):
        return json.loads(string)

    def load_rows(self, stream):
        for line in stream:
            yield json.loads(line)

    def dump_row(self, row, stream):
        json.dump(row, stream)
        stream.write("\n")

    def dumps_row(self, row):
        return json.dumps(row)

    def dump_rows(self, rows, stream):
        for row in rows:
            self.dump_row(row, stream)


class YamredDsvFormat(YamrFormat):
    def __init__(self, key_column_names, subkey_column_names=None, has_subkey=False, lenval=False):
        if subkey_column_names is None:
            subkey_column_names = []
        super(YamrFormat, self).__init__(
            "yamred_dsv",
            attributes={
                "key_column_names": key_column_names,
                "subkey_column_names": subkey_column_names,
                "has_subkey": has_subkey,
                "lenval": lenval
            })

class SchemafulDsvFormat(Format):
    def __init__(self, columns, enable_escaping=True):
        super(SchemafulDsvFormat, self).__init__(
            "schemaful_dsv",
            attributes={
                "columns": columns,
                "enable_escaping": True
            })

    def _get_columns(self):
        return self.attributes.get("columns", False)

    def _set_columns(self, value):
        self.attributes["columns"] = value

    columns = property(_get_columns, _set_columns)

    def _get_enable_escaping(self):
        return self.attributes.get("enable_escaping", False)

    def _set_enable_escaping(self, value):
        self.attributes["enable_escaping"] = value

    enable_escaping = property(_get_enable_escaping, _set_enable_escaping)


    def load_unparsed_row(self, stream):
        return stream.readline()

    def load_row(self, stream):
        line = stream.readline()
        if not line:
            return None
        return self._parse(line)

    def loads_row(self, string):
        return self._parse(string)

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

    def dumps_row(self, row):
        stream = StringIO()
        self.dump_row(row, stream)
        return stream.getvalue()

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

# Deprecated
class SchemedDsvFormat(SchemafulDsvFormat):
    def __init__(self, columns):
        super(SchemedDsvFormat, self).__init__(columns)
        self._name = yson.to_yson_type("schemed_dsv", self.attributes)

def create_format(name, attributes=None):
    if attributes is None:
        attributes = {}

    name = yson.loads(name)
    attributes = update(attributes, name.attributes)
    name = str(name)

    if name == "yamr":
        format = YamrFormat()
    elif name == "dsv":
        format = DsvFormat()
    elif name == "yamred_dsv":
        format = YamredDsvFormat(key_column_names=attributes["key_column_names"])
    elif name == "schemaful_dsv":
        format = SchemafulDsvFormat(columns=attributes["columns"])
    elif name == "yson":
        format = YsonFormat()
    elif name == "json":
        format = JsonFormat()
    else:
        raise YtError("Incorrect format " + name)

    format.attributes = attributes
    return format

def dumps_row(self, row, format=None):
    if format is None:
        format = config.format.TABULAR_DATA_FORMAT
    return format.dumps_row(row)

def loads_row(self, string, format=None):
    if format is None:
        format = config.format.TABULAR_DATA_FORMAT
    return format.loads_row(string)
