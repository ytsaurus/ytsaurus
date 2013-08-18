from common import get_value, require, update
from errors import YtError, YtFormatError
from yt.yson import loads, dumps, yson_to_json

import struct
from cStringIO import StringIO

class Format(object):
    """Format represented by raw description: name + attributes"""
    def __init__(self, format_string, attributes=None):
        self.format = loads(format_string)
        require(isinstance(self.format, str), YtError("Incorrect format"))

        if attributes is not None:
            update(self.format.attributes, attributes)


    def json(self):
        return yson_to_json(self.format)

    def name(self):
        return str(self.format)

    def attributes(self):
        return self.format.attributes

    def __repr__(self):
        return dumps(self.format)

    def __eq__(self, other):
        if hasattr(other, "format"):
            return self.format == other.format
        return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def is_read_row_supported(self):
        return self.name() in ["dsv", "yamr", "yamred_dsv"]

    def read_row(self, stream):
        def read_lenval(stream, has_subkey):
            field_count = 2
            if has_subkey:
                field_count += 1

            result = StringIO()
            for iter in xrange(field_count):
                len_bytes = stream.read(4)
                if not len_bytes:
                    return ""
                result.write(len_bytes)
                length = struct.unpack('i', len_bytes)[0]
                result.write(stream.read(length))
            return result.getvalue()


        if self.name() == "dsv":
            return stream.readline()
        elif self.name() in ["yamr", "yamred_dsv"]:
            if self.attributes().get("lenval", False):
                return read_lenval(stream, self.attributes().get("has_subkey", False))
            else:
                return stream.readline()
        else:
            raise YtFormatError("Reading rows in %s format isn't supported" % self.name())

class DsvFormat(Format):
    def __init__(self):
        super(DsvFormat, self).__init__("dsv")

class YsonFormat(Format):
    def __init__(self, format=None):
        if format is None:
            format = "text"
        super(YsonFormat, self).__init__("yson", attributes={"format": format})

class YamrFormat(Format):
    def __init__(self, has_subkey, lenval, field_separator=None, record_separator=None):
        super(YamrFormat, self).__init__(
            "yamr",
            attributes={
                "fs": get_value(field_separator, '\t'),
                "rs": get_value(record_separator, '\n'),
                "has_subkey": has_subkey,
                "lenval": lenval
            })

    def _get_has_subkey(self):
        return self.attributes().get("has_subkey", False)

    def _set_has_subkey(self, value):
        self.attributes()["has_subkey"] = value

    has_subkey = property(_get_has_subkey, _set_has_subkey)

    def _get_lenval(self):
        return self.attributes().get("lenval", False)

    def _set_lenval(self, value):
        self.attributes()["lenval"] = value

    lenval = property(_get_lenval, _set_lenval)

class JsonFormat(Format):
    def __init__(self):
        super(JsonFormat, self).__init__("json")
