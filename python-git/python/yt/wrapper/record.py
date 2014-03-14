import config
from common import require
from errors import YtError

import yt.yson as yson

import copy

""" Old style mapreduce records.
    Copy-pasted from mapreducelib.py with some additions"""
class SimpleRecord:
    def __init__(self, key, value, tableIndex=0):
        self.key = key
        self.value = value
        self.tableIndex = tableIndex
    def items(self):
        return self.key, self.value
    def getTableIndex(self):
        return self.tableIndex

class SubkeyedRecord(SimpleRecord):
    def __init__(self, key, subkey, value, tableIndex=0):
        SimpleRecord.__init__(self, key, value)
        self.subkey = subkey
        self.tableIndex = tableIndex
    def items(self):
        return self.key, self.subkey, self.value
    def getTableIndex(self):
        return self.tableIndex

    def __str__(self):
        return "Record('%s', '%s', '%s')" % (self.key, self.subkey, self.value)

    def __repr__(self):
        return self.__str__()

    def __cmp__(self, other):
        cmps = [cmp(getattr(self, field), getattr(other, field))
                for field in ["key", "subkey", "value"]]
        non_zeroes = filter(None, cmps) + [0]
        return non_zeroes[0]

    def __hash__(self):
        return hash(frozenset([self.key, self.subkey, self.value]))


def Record(*args, **kws):
    """Represents mapreduce-like record with key, subkey, value"""
    assert len(args) >= 2, "incorrect arguments count [ARGS: %s]" % repr(args)
    if len(args) < 3:
        return SimpleRecord(*args, **kws)
    return SubkeyedRecord(*args[:3], **kws)


""" Methods for records conversion """
# TODO(ignat): builtin full support of this methods to read/write and python operations
def record_to_line(rec, format=None, eoln=True):
    def escape(string, escape_dict):
        string = string.replace("\\", "\\\\")
        for sym, escaped in escape_dict.items():
            string = string.replace(sym, escaped)
        return string
    def escape_key(string):
        return escape(string, {'\n': '\\n', '\r': '\\r', '\t': '\\t', '\0': '\\0', '=': '\\='})
    def escape_value(string):
        return escape(string, {'\n': '\\n', '\r': '\\r', '\t': '\\t', '\0': '\\0'})

    if format is None: format = config.format.TABULAR_DATA_FORMAT
    
    if format.name() == "yamr":
        require(not format.attributes().get("lenval", False),
                YtError("Lenval conversion is not supported now."))
        if format.has_subkey:
            fields = [rec.key, rec.subkey, rec.value]
        else:
            fields = [rec.key, rec.value]
        body = "\t".join(fields)
    elif format.name() == "dsv":
        body = "\t".join("%s=%s" % (escape_key(str(item[0])), escape_value(str(item[1]))) for item in rec.iteritems())
    elif format.name() == "yson":
        body = yson.dumps(rec, yson_format="text") + ";"
    elif format.name() == "schemed_dsv":
        body = "\t".join(map(escape_value, (rec[key] for key in format.attributes()["columns"])))
    else:
        raise YtError("Unrecognized format " + repr(format))
    if eoln:
        body = body + "\n"
    return body

def line_to_record(line, format=None):
    def unescape_token(token, escape_dict):
        for sym, unescaped in escape_dict.items():
            token = token.replace(sym, unescaped)
        return token.replace("\\", "")
    def unescape_dsv_field(field):
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

        return ["\\".join(map(lambda t: unescape_token(t, key_dict), key_tokens)),
                "\\".join(map(lambda t: unescape_token(t, value_dict), value_tokens))]

    def unescape_field(field):
        unescape_dict = {'\\n': '\n', '\\r': '\r', '\\t': '\t', '\\0': '\0'}
        return "\\".join(map(lambda t: unescape_token(t, unescape_dict), field.split("\\\\")))

    
    if format is None: format = config.format.TABULAR_DATA_FORMAT
    
    if format.name() in ["yamr", "yamred_dsv"]:
        return Record(*line.strip("\n").split("\t", 1 + (1 if format.attributes().get("has_subkey", False) else 0)))
    elif format.name() == "dsv":
        return dict(map(unescape_dsv_field, filter(None, line.strip("\n").split("\t"))))
    elif format.name() == "yson":
        return yson.loads(line.rstrip(";\n"))
    elif format.name() == "schemed_dsv":
        return dict(zip(format.attributes()["columns"], map(unescape_field, line.rstrip("\n").split("\t"))))
    else:
        raise YtError("Unrecognized format " + repr(format))

def extract_key(rec, fields, format=None):
    if format is None: format = config.format.TABULAR_DATA_FORMAT

    if format.name() in ["yamr", "yamred_dsv"]:
        return rec.key
    elif format.name() in ["dsv", "yson"]:
        return dict((key, rec[key]) for key in fields if key in rec)
    else:
        raise YtError("Unrecognized format " + repr(format))
