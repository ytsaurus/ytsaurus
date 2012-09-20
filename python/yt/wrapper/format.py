from common import bool_to_string
from yt.yson import parse_string, yson_types, dumps


# TODO(ignat): Add custom field separator
class Format(object):
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
    def __init__(self):
        pass

    def _mime_type(self):
        return "application/x-yt-yson-text"

    def to_json(self):
        return {"$value": "yson",
                "$attributes":
                    {"format": "text" }}

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

class RawFormat(Format):
    @staticmethod
    def from_string(str):
        print "STR", str
        format = RawFormat()
        format._str = str
        format._format = parse_string(str)
        return format

    @staticmethod
    def from_tree(tree):
        format = RawFormat()
        format._format = yson_types.convert_to_YSON_type_from_tree(tree)
        format._str = dumps(format._format)
        return format

    def to_input_http_header(self):
        return {"X-YT-Input-Format": self._str}

    def to_output_http_header(self):
        return {"X-YT-Output-Format": self._str}

    def to_json(self):
        return {"$value": str(self._format),
                "$attributes": self._format.attributes}

class JsonFormat(Format):
    def _mime_type(self):
        return "application/json"
