import config
from common import require
from errors import YtError
from format import DsvFormat, YamrFormat, YsonFormat

import yt.yson as yson

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
    def escape_dsv(value):
        escape_dict = {'\\': '\\\\', '\n': '\\n', '\t': '\\t', '=': '\\=', '\0': '\\0'}
        for sym, escaped in escape_dict.items():
            value = value.replace(sym, escaped)
        return value
    if format is None: format = config.DEFAULT_FORMAT
    
    if isinstance(format, YamrFormat):
        require(not format.lenval, YtError("Lenval conversion is not supported now."))
        if format.has_subkey:
            fields = [rec.key, rec.subkey, rec.value]
        else:
            fields = [rec.key, rec.value]
        body = "\t".join(fields)
    elif isinstance(format, DsvFormat):
        body = "\t".join("=".join(map(escape_dsv, map(str, item))) for item in rec.iteritems())
    elif isinstance(format, YsonFormat):
        body = yson.dumps(rec) + ";"
    else:
        raise YtError("Unrecognized format " + repr(format))
    if eoln:
        body = body + "\n"
    return body

def line_to_record(line, format=None):
    if format is None: format = config.DEFAULT_FORMAT
    
    if isinstance(format, YamrFormat):
        return Record(*line.strip("\n").split("\t", 1 + (1 if format.has_subkey else 0)))
    elif isinstance(format, DsvFormat):
        return dict(field.split("=", 1) for field in line.strip("\n").split("\t") if field)
    elif isinstance(format, YsonFormat):
        return yson.loads(line.rstrip(";\n"))
    else:
        raise YtError("Unrecognized format " + repr(format))

def extract_key(rec, fields, format=None):
    if format is None: format = config.DEFAULT_FORMAT

    if isinstance(format, YamrFormat):
        return rec.key
    elif isinstance(format, DsvFormat) or isinstance(format, YsonFormat):
        return dict((key, rec[key]) for key in fields if key in rec)
    else:
        raise YtError("Unrecognized format " + repr(format))
