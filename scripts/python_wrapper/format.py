from common import bool_to_string

# TODO(ignat): Add custom field separator
class Format(object):
    pass

class DsvFormat(Format):
    def __init__(self):
        pass

    def to_mime_type(self):
        return "text/tab-separated-values"

    def to_json(self):
        return "dsv"

class YamrFormat(Format):
    def __init__(self, has_subkey, lenval):
        self.has_subkey = has_subkey
        self.lenval = lenval

    def to_mime_type(self):
        return "application/x-yamr%s-%s" % \
            ("-subkey" if self.has_subkey else "",
             "lenval" if self.lenval else "delimited")

    def to_json(self):
        return {"$value": "yamr",
                "$attributes":
                    {"has_subkey": bool_to_string(self.has_subkey),
                     "lenval": bool_to_string(self.lenval)}}

