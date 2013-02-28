from common import bool_to_string, get_value, YtError
from yt.yson import loads, yson_types
import simplejson as json

import struct
from cStringIO import StringIO

# TODO(ignat): Add custom field separator
class Format(object):
    """ Represents format to read/write and process records"""
    def to_input_http_header(self):
        return {"Content-Type": self._mime_type()}

    def to_output_http_header(self):
        return {"Accept": self._mime_type()}

    def read_row(self, stream):
        raise YtError("Reading record from stream is not implemented for format " + repr(self))

    def __eq__(self, other):
        if hasattr(self, "to_json") and hasattr(other, "to_json"):
            return self.to_json() == other.to_json()
        return False

    def __ne__(self, other):
        return not self.__eq__(other)



class DsvFormat(Format):
    def __init__(self):
        pass

    def _mime_type(self):
        return "text/tab-separated-values"

    def to_json(self):
        return "dsv"

    def read_row(self, stream):
        return stream.readline()
    


class YsonFormat(Format):
    def __init__(self, format=None):
        self.format = get_value(format, "pretty")
        pass

    def _mime_type(self):
        return "application/x-yt-yson-" + self.format

    def to_json(self):
        return {"$value": "yson",
                "$attributes":
                    {"format": self.format}}

class YamrFormat(Format):
    def __init__(self, has_subkey, lenval, field_separator=None, record_separator=None):
        self.field_separator = get_value(field_separator, '\t')
        self.record_separator = get_value(record_separator, '\n')
        self.has_subkey = has_subkey
        self.lenval = lenval

    #def _mime_type(self):
    #    return "application/x-yamr%s-%s" % \
    #        ("-subkey" if self.has_subkey else "",
    #         "lenval" if self.lenval else "delimited")

    def to_input_http_header(self):
        return {"X-YT-Input-Format": self.to_str()}

    def to_output_http_header(self):
        return {"X-YT-Output-Format": self.to_str()}

    def to_json(self):
        return {"$value": "yamr",
                "$attributes": {
                    "has_subkey": bool_to_string(self.has_subkey),
                    "lenval": bool_to_string(self.lenval),
                    "fs": self.field_separator,
                    "rs": self.record_separator}
               }

    def to_str(self):
        return json.dumps(self.to_json())

    def read_row(self, stream):
        if not self.lenval:
            return stream.readline()
        else:
            field_count = 2
            if self.has_subkey:
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


class JsonFormat(Format):
    def _mime_type(self):
        return "application/json"

class RawFormat(Format):
    """Format represented by raw description: name + attributes"""
    @staticmethod
    def from_yson(yson):
        format = RawFormat()
        format._format = yson
        return format

    @staticmethod
    def from_yson_string(str):
        format = RawFormat()
        format._format = loads(str)
        return format

    @staticmethod
    def from_tree(json_tree):
        format = RawFormat()
        format._format = yson_types.convert_to_yson_tree(json_tree)
        return format

    def to_input_http_header(self):
        return {"X-YT-Input-Format": self.to_str()}

    def to_output_http_header(self):
        return {"X-YT-Output-Format": self.to_str()}

    def to_json(self):
        return {"$value": str(self._format),
                "$attributes": self._format.attributes}

    def to_str(self):
        return json.dumps(self.to_json())

