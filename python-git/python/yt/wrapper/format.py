"""YT data formats

.. note:: In `Format` descendants constructors default parameters are overridden by `attributes` \
parameters, and then by kwargs options.
"""

import format_config
from common import get_value, require, filter_dict, merge_dicts
from errors import YtError, YtFormatError
from yamr_record import Record, SimpleRecord, SubkeyedRecord
import yt.yson as yson
import yt.logger as logger

from abc import ABCMeta, abstractmethod
import copy
import struct
import itertools
import simplejson as json
from cStringIO import StringIO

try:
    import cjson
except ImportError:
    cjson = None


class Format(object):
    """ YT data representations.

        Abstract base class for different formats.
    """
    __metaclass__ = ABCMeta

    def __init__(self, name, attributes=None, raw=None):
        """
        :param name: (string) format name
        :param attributes: (dict) format parameters
        """
        require(isinstance(name, str), YtFormatError("Incorrect format %r" % name))
        self._name = yson.YsonString(name)
        self._name.attributes = get_value(attributes, {})
        self._raw = raw

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

    def _is_raw(self, raw):
        if raw is None:
            if self._raw is None:
                return False
            return self._raw
        return raw

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
    def load_row(self, stream, raw=None):
        """Read from the stream, parse (optionally) and return one row.

        :return: parsed row (dict or Record), (string) if unparsed, None if stream is empty
        """
        pass

    @abstractmethod
    def load_rows(self, stream, raw=None):
        """Read from the stream, parse, process input table switcher and yield all rows.
        """
        pass

    def dump_row(self, row, stream, raw=None):
        """Serialize row and write to the stream.
        """
        if self._is_raw(raw):
            stream.write(raw)
            return
        self._dump_row(row, stream)

    def dump_rows(self, rows, stream, raw=None):
        """Serialize rows, create output table switchers and write to the stream.
        """
        if self._is_raw(raw):
            for row in rows:
                stream.write(row)
            return
        self._dump_rows(rows, stream)

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
        return property(get_func)

    @staticmethod
    def _make_attributes(attributes, defaults, options):
        not_none_options = filter_dict(lambda key, value: value is not None, options)
        return merge_dicts(defaults, attributes, not_none_options)

    def is_raw_load_supported(self):
        """Returns true if format supports loading raw YSON rows"""
        return self.name() in ("dsv", "yamr", "yamred_dsv", "json", "yson")

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
    Tabular format is widely used in Statistics.

    .. seealso:: `DSV on wiki <https://wiki.yandex-team.ru/yt/userdoc/formats/#dsv>`_
    """

    def __init__(self, enable_escaping=None,
                 enable_table_index=None, table_index_column=None, attributes=None, raw=None):
        """
        :param enable_escaping: (bool) process escaped symbols, True by default
        :param enable_table_index: (bool) process input table indexes in load_rows. \
        NB! specify it only for operations!
        :param table_index_column: (string) name for special row field (exists only inside \
        operation binary!), "@table_index" by default
        """
        defaults = {"enable_escaping": True, "enable_table_index": False,
                    "table_index_column": "@table_index"}
        options = {"enable_escaping": enable_escaping,
                   "enable_table_index": enable_table_index,
                   "table_index_column": table_index_column}

        all_attributes = Format._make_attributes(get_value(attributes, {}), defaults, options)
        super(DsvFormat, self).__init__("dsv", all_attributes, raw)


    enable_escaping = Format._create_property("enable_escaping")

    enable_table_index = Format._create_property("enable_table_index")

    table_index_column = Format._create_property("table_index_column")

    def load_row(self, stream, raw=None):
        line = stream.readline()
        if not line:
            return None
        if self._is_raw(raw):
            return line
        parsed_line = self._parse(line)
        if self.enable_table_index:
            parsed_line[self.table_index_column] = int(parsed_line[self.table_index_column])
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

    def _dump_rows(self, rows, stream):
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
    Main and default YT data format.

    .. seealso:: `YSON on wiki <https://wiki.yandex-team.ru/yt/userdoc/formats#yson>`_
    """

    def __init__(self, format=None, process_table_index=True, ignore_inner_attributes=False, boolean_as_string=True,
                 attributes=None, raw=None):
        """
        :param format: (one of "text" (default), "pretty", "binary") output format \
        (actual only for output).
        :param process_table_index: (bool) process input and output table switchers in `dump_rows`\
         and `load_rows`. `See also <https://wiki.yandex-team.ru/yt/userdoc/tableswitch#yson>`_
        """
        all_attributes = Format._make_attributes(get_value(attributes, {}),
                                                 {"format": "binary"},
                                                 {"format": format})
        super(YsonFormat, self).__init__("yson", all_attributes, raw)
        self.process_table_index = process_table_index
        self.ignore_inner_attributes = ignore_inner_attributes
        self.boolean_as_string = boolean_as_string

    def _check_bindings(self):
        require(yson.TYPE == "BINARY", YtError("Yson bindings required"))

    def load_row(self, stream, raw=None):
        """Not supported"""
        raise YtFormatError("load_row is not supported in Yson")

    @staticmethod
    def _process_input_rows(rows):
        table_index = 0
        for row in rows:
            if isinstance(row, yson.YsonEntity):
                try:
                    table_index = row.attributes["table_index"]
                except KeyError:
                    raise YtError("Wrong table switcher in Yson rows")
                continue

            row["input_table_index"] = table_index
            yield row

    def load_rows(self, stream, raw=None):
        self._check_bindings()
        rows = yson.load(stream, yson_type="list_fragment", always_create_attributes=False, raw=raw)
        if raw:
            return rows
        else:
            if self.process_table_index:
                return self._process_input_rows(rows)
            else:
                return rows

    def _dump_row(self, row, stream):
        self._check_bindings()
        self._dump_rows([row], stream)

    @staticmethod
    def _process_output_rows(rows):
        table_index = 0
        for row in rows:
            new_table_index = row.get("output_table_index", 0)
            if new_table_index != table_index:
                yield yson.to_yson_type(None, attributes={"table_index": new_table_index})
                table_index = new_table_index
            if hasattr(row, "attributes"):
                row.attributes = {}
            row.pop("output_table_index", None)
            row.pop("input_table_index", None)
            yield row

    def _dump_rows(self, rows, stream):
        self._check_bindings()
        if self.process_table_index:
            rows = self._process_output_rows(rows)

        kwargs = {}
        if yson.TYPE == "BINARY":
            kwargs = {"ignore_inner_attributes": self.ignore_inner_attributes,
                      "boolean_as_string": self.boolean_as_string}
        yson.dump(rows, stream, yson_type="list_fragment", yson_format=self.attributes["format"], **kwargs)

class YamrFormat(Format):
    """
    YAMR legacy data format.

    Supported two mutually exclusive modes: text mode with delimiter and \
    binary mode ('lenval') with length before each field.
    .. note:: In delimiter mode implemented just standard delimiter `\t` and terminator `\n`.

    .. seealso:: `YAMR on wiki <https://wiki.yandex-team.ru/yt/userdoc/formats#yamr>`_
    """

    def __init__(self, has_subkey=None, lenval=None,
                 field_separator=None, record_separator=None,
                 enable_table_index=None, attributes=None, raw=None):
        """
        :param has_subkey: (bool) False by default
        :param lenval: (bool) False by default
        :param field_separator: (string) "\t" by default
        :param record_separator: (string) is not implemented yet! '\n' by default
        :param enable_table_index: (bool) specify it for table switching in load_rows
        """
        defaults = {"has_subkey": False, "lenval": False, "fs": '\t', "rs": '\n',
                    "enable_table_index": False}
        options = {"has_subkey": has_subkey, "lenval": lenval,
                   "fs": field_separator, "rs": record_separator,
                   "enable_table_index": enable_table_index}
        attributes = get_value(attributes, {})
        all_attributes = Format._make_attributes(attributes, defaults, options)
        super(YamrFormat, self).__init__("yamr", all_attributes, raw)
        self._load_row = self._read_lenval_values if self.lenval else self._read_delimited_values
        self.fields_number = 3 if self.has_subkey else 2

    has_subkey = Format._create_property("has_subkey")
    lenval = Format._create_property("lenval")
    field_separator = Format._create_property("fs")
    record_separator = Format._create_property("rs")

    def load_row(self, stream, raw=None):
        unparsed = self._is_raw(raw)
        result_of_loading = self._load_row(stream, unparsed)
        if unparsed:
            return result_of_loading
        if not result_of_loading or len(result_of_loading) not in (2, 3):
            return None
        return Record(*result_of_loading)

    def load_rows(self, stream, raw=None):
        unparsed = self._is_raw(raw)
        table_index = 0
        while True:
            row = self._load_row(stream, unparsed=unparsed)
            if unparsed and row:
                yield row
                continue

            fields = row
            if not fields:
                break
            if len(fields) == 1:
                try:
                    table_index = int(fields[0])
                except ValueError:
                    raise YtFormatError("Invalid table switch '{0}'".format(fields[0]))
                continue
            yield Record(*fields, tableIndex=table_index)

    def _read_delimited_values(self, stream, unparsed):
        if self.record_separator != '\n':
            raise NotImplementedError("Implemented just for standard terminator ('\\n')")
        row = stream.readline()
        if not row:
            return None
        if unparsed:
            return row
        return row.rstrip("\n").split(self.field_separator, self.fields_number - 1)

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
                if length == -1:
                    field = stream.read(4)
                    if unparsed:
                        return len_bytes + field
                    return struct.unpack("i", field)
            except struct.error:
                raise YtError("Incomplete record in yamr lenval")

            field = stream.read(length)
            if len(field) != length:
                 raise YtError("Incorrect length field in yamr lenval,\
                                expected {0}, received {1}".format(length, len(field)))
            fields.append(field)
        if unparsed:
            return ''.join(fields)
        return fields

    def _dump_row(self, row, stream):
        fields = row.items()

        if self.lenval:
            for field in fields:
                stream.write(struct.pack("i", len(field)))
                stream.write(field)
        else:
            for field in fields[:-1]:
                stream.write(field)
                stream.write(self.attributes["fs"])
            stream.write(fields[-1])
            stream.write(self.attributes["rs"])

    def _dump_rows(self, rows, stream):
        table_index = 0
        for row in rows:
            new_table_index = row.tableIndex
            if new_table_index != table_index:
                if self.lenval:
                    table_switcher = struct.pack("ii", -1, new_table_index)
                else:
                    table_switcher = str(new_table_index) + "\n"
                stream.write(table_switcher)
                table_index = new_table_index

            self.dump_row(row, stream)

class JsonFormat(Format):
    """
    Open standard text data format for attribute-value data.

    .. seealso:: `JSON on wiki <https://wiki.yandex-team.ru/yt/userdoc/formats#json>`_
    """

    def __init__(self, process_table_index=True, table_index_field_name="table_index", attributes=None, raw=None):
        attributes = get_value(attributes, {})
        super(JsonFormat, self).__init__("json", attributes, raw)
        self.process_table_index = process_table_index
        self.table_index_field_name = table_index_field_name

    def _loads(self, string, raw):
        if raw:
            return string
        string = string.rstrip("\n")
        if cjson is not None:
            return cjson.decode(string)
        return json.loads(string)

    def _dump(self, obj, stream):
        if cjson is not None:
            return stream.write(cjson.encode(obj))
        return json.dump(obj, stream)

    def _dumps(self, obj):
        if cjson is not None:
            return cjson.encode(obj)
        return json.dumps(obj)

    def load_row(self, stream, raw=None):
        row = stream.readline()
        if not row:
            return None
        return self._loads(row, self._is_raw(raw))

    def _process_input_rows(self, rows):
        table_index = None
        for row in rows:
            if "$value" in row:
                require(row["$value"] == None,
                        YtError("Incorrect $value of table switch in JSON format"))
                table_index = row["$attributes"]["table_index"]
            else:
                if table_index is not None:
                    row[self.table_index_field_name] = table_index
                yield row

    def load_rows(self, stream, raw=None):
        raw = self._is_raw(raw)
        def _load_rows(stream):
            for line in stream:
                yield self._loads(line, raw)

        rows = _load_rows(stream)
        if self.process_table_index and not raw:
            return self._process_input_rows(rows)
        return rows

    def _dump_row(self, row, stream):
        self._dump(row, stream)
        stream.write("\n")

    def _process_output_rows(self, rows):
        table_index = 0
        for row in rows:
            new_table_index = row[self.table_index_field_name] \
                if self.table_index_field_name in row \
                else 0
            if new_table_index != table_index:
                table_index = new_table_index
                yield {"$value": None, "$attributes": {"table_index": table_index}}
            if self.table_index_field_name in row:
                del row[self.table_index_field_name]
            yield row

    def _dump_rows(self, rows, stream):
        if self.process_table_index:
            rows = self._process_output_rows(rows)
        for row in rows:
            self.dump_row(row, stream)

    def dumps_row(self, row):
        return self._dumps(row)

    def loads_row(self, string):
        return self._loads(string, raw=False)

class YamredDsvFormat(YamrFormat):
    """
    Hybrid of Yamr and DSV formats. It is used to support yamr representations of tabular data.

    .. seealso:: `Yamred DSV on wiki <https://wiki.yandex-team.ru/yt/userdoc/formats#yamreddsv>`_
    """

    def __init__(self, key_column_names=None, subkey_column_names=None,
                 has_subkey=None, lenval=None, attributes=None, raw=None):
        """
        :param key_column_names: (list of strings)
        :param subkey_column_names: (list of strings)
        :param has_subkey: (bool)
        :param lenval: (bool)
        """
        defaults = {"has_subkey": False, "lenval": False,
                    "key_column_names": [], "subkey_column_names": []}
        options = {"key_column_names": key_column_names, "subkey_column_names": subkey_column_names,
                   "has_subkey": has_subkey, "lenval": lenval}
        attributes = get_value(attributes, {})
        all_attributes = Format._make_attributes(attributes, defaults, options)
        require(all_attributes.has_key("key_column_names"),
                YtFormatError("YamredDsvFormat require 'key_column_names' attribute"))
        super(YamredDsvFormat, self).__init__(attributes=all_attributes, raw=raw)
        self._name = yson.to_yson_type("yamred_dsv", self.attributes)

    key_column_names = Format._create_property("key_column_names")

    subkey_column_names = Format._create_property("subkey_column_names")

class SchemafulDsvFormat(Format):
    """
    Schemaful dsv format. It accepts column names and outputs values of these columns.

    .. seealso:: `SchemafulDsvFormat on wiki \
    <https://wiki.yandex-team.ru/yt/userdoc/formats#schemafuldsvschemeddsv>`_
    """

    def __init__(self, columns=None, enable_escaping=None,
                 enable_table_index=None, table_index_column=None,
                 attributes=None, raw=None):
        """
        :param columns: (list of strings) mandatory parameter!
        :param enable_escaping: (bool) process escaped symbols, True by default
        :param process_table_index: (bool) process input table indexes in load_rows and \
        output table indexes in dump_rows, False by default
        :param table_index_column: (string) name for special row field (exists only inside\
         operation binary!), "@table_index" by default
        """
        defaults = {"enable_escaping": True, "enable_table_index": False,
                    "table_index_column": "@table_index"}
        options = {"columns": columns,
                   "enable_escaping": enable_escaping,
                   "enable_table_index": enable_table_index,
                   "table_index_column": table_index_column}
        attributes = get_value(attributes, {})
        all_attributes = Format._make_attributes(attributes, defaults, options)
        require(all_attributes.has_key("columns"),
                YtFormatError("SchemafulDsvFormat require 'columns' attribute"))
        super(SchemafulDsvFormat, self).__init__("schemaful_dsv", all_attributes, raw)
        if self.enable_table_index:
            self._columns = [table_index_column] + self.columns
        else:
            self._columns = self.columns

    columns = Format._create_property("columns")

    enable_escaping = Format._create_property("enable_escaping")

    enable_table_index = Format._create_property("enable_table_index")

    table_index_column = Format._create_property("table_index_column")

    def load_row(self, stream, raw=None):
        line = stream.readline()
        if not line:
            return None
        if self._is_raw(raw):
            return line
        parsed_line = self._parse(line)
        if self.enable_table_index:
            parsed_line[self.table_index_column] = int(parsed_line[self.table_index_column])
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
            return self._escape(string, {'\n': '\\n', '\r': '\\r', '\t': '\\t', '\0': '\\0'})
        if self.enable_table_index:
            row[self.table_index_column] = str(row[self.table_index_column])
        for key in self._columns[:-1]:
            stream.write(escape(row[key]))
            stream.write("\t")
        stream.write(escape(row[self._columns[-1]]))
        stream.write("\n")


    def _dump_rows(self, rows, stream):
        for row in rows:
            self.dump_row(row, stream)

    def _parse(self, line):
        def unescape_field(field):
            if not self.enable_escaping:
                return field
            unescape_dict = {'\\n': '\n', '\\r': '\r', '\\t': '\t', '\\0': '\0'}
            return "\\".join(map(lambda token: self._unescape(token, unescape_dict),
                                 field.split("\\\\")))

        return dict(itertools.izip(self._columns,
                                   map(unescape_field, line.rstrip("\n").split("\t"))))

# TODO(veronikaiv): do it beautiful way!
Format._copy_docs()

def create_format(yson_name, attributes=None, **kwargs):
    """Create format by YSON string.

    :param yson_name: YSON string like ``'<lenval=false;has_subkey=false>yamr'``
    :param attributes: Deprecated! Don't use it! It will be removed!
    """
    if attributes is not None:
        logger.warning("Usage deprecated parameter 'attributes' of create_format. "
                       "It will be removed!")
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
        return NAME_TO_FORMAT[name](attributes=attributes, **kwargs)
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

def extract_key(rec, fields):
    if isinstance(rec, SimpleRecord) or isinstance(rec, SubkeyedRecord):
        return rec.key
    else:
        return dict((key, rec[key]) for key in fields if key in rec)
