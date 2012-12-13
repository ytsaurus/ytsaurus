from common import bool_to_string, get_value
from yt.yson import loads, yson_types
import simplejson as json

import sys


# TODO(ignat): Add custom field separator
class Format(object):
    """ Represents format to read/write and process records"""
    def to_input_http_header(self):
        return {"Content-Type": self._mime_type()}

    def to_output_http_header(self):
        return {"Accept": self._mime_type()}


class DsvFormat(Format):
    def __init__(self):
        pass

    def _mime_type(self):
        return "text/tab-separated-values"

    def to_json(self):
        return "dsv"

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
    def __init__(self, has_subkey, lenval):
        self.has_subkey = has_subkey
        self.lenval = lenval

    def _mime_type(self):
        return "application/x-yamr%s-%s" % \
            ("-subkey" if self.has_subkey else "",
             "lenval" if self.lenval else "delimited")

    def to_json(self):
        return {"$value": "yamr",
                "$attributes":
                    {"has_subkey": bool_to_string(self.has_subkey),
                     "lenval": bool_to_string(self.lenval)}}

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
    def from_tree(tree):
        format = RawFormat()
        format._format = yson_types.convert_to_yson_type_from_tree(tree)
        return format

    def to_input_http_header(self):
        return {"X-YT-Input-Format": self.to_str()}

    def to_output_http_header(self):
        return {"X-YT-Output-Format": self.to_str()}

    def to_json(self):
        print >>sys.stderr, type(self._format), self._format
        return {"$value": str(self._format),
                "$attributes": self._format.attributes}

    def to_str(self):
        return json.dumps(self.to_json())

