import config
from common import require, YtError
from format import DsvFormat, YamrFormat

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
    assert len(args) >= 2, "incorrect arguments count [ARGS: %s]" % repr(args)
    if len(args) < 3:
        return SimpleRecord(*args, **kws)
    return SubkeyedRecord(*args[:3], **kws)

""" Methods for records convertion """
# TODO(ignat): builtin full support of this methods to read/write and python operations
def record_to_line(rec, eoln=True, format=None):
    if format is None: format = config.DEFAULT_FORMAT
    
    if isinstance(format, YamrFormat):
        require(not format.lenval, YtError("Lenval convertion is not supported now."))
        if format.has_subkey:
            fields = [rec.key, rec.subkey, rec.value]
        else:
            fields = [rec.key, rec.value]
        body = "\t".join(fields)
    else:
        body = "\t".join("=".join(map(str, item)) for item in rec.iteritems())
    return "%s%s" % (body, "\n" if eoln else "")

def line_to_record(line, format=None):
    if format is None: format = config.DEFAULT_FORMAT
    
    if isinstance(format, YamrFormat):
        return Record(*line.strip("\n").split("\t", 1 + (1 if format.has_subkey else 0)))
    else:
        return dict(field.split("=", 1) for field in line.strip("\n").split("\t") if field)
