""" Old style mapreduce records.
    Copy-pasted from mapreducelib.py with some additions.
"""

try:
    from itertools import ifilter
except ImportError:  # Python 3
    ifilter = filter


class SimpleRecord:
    """Mapreduce-like record represents (key, value) pair, without subkey."""
    def __init__(self, key, value, tableIndex=0, recordIndex=None):
        self.key = key
        self.value = value
        self.tableIndex = tableIndex
        self.recordIndex = recordIndex

    def __str__(self):
        return "Record('%s', '%s')" % (self.key, self.value)

    def __repr__(self):
        return self.__str__()

    def __eq__(self, other):
        return self.key == other.key and self.value == other.value

    def __lt__(self, other):
        return (self.key, self.value) < (other.key, other.value)

    def __le__(self, other):
        return self < other or self == other

    def __ge__(self, other):
        return not self < other

    def __gt__(self, other):
        return not (self < other or self == other)

    def __hash__(self):
        return hash(frozenset([self.key, self.value]))

    def items(self):
        return self.key, self.value

    def getTableIndex(self):
        return self.tableIndex

    def getRecordIndex(self):
        return self.recordIndex


class SubkeyedRecord(SimpleRecord):
    """Mapreduce-like record with key, subkey and value."""
    def __init__(self, key, subkey, value, tableIndex=0, recordIndex=None):
        SimpleRecord.__init__(self, key, value, tableIndex, recordIndex)
        self.subkey = subkey

    def items(self):
        return self.key, self.subkey, self.value

    def getTableIndex(self):
        return self.tableIndex

    def __str__(self):
        return "Record('%s', '%s', '%s')" % (self.key, self.subkey, self.value)

    def __repr__(self):
        return self.__str__()

    def __eq__(self, other):
        return self.key == other.key and self.subkey == other.subkey and self.value == other.value

    def __lt__(self, other):
        return (self.key, self.subkey, self.value) < (other.key, other.subkey, other.value)

    def __hash__(self):
        return hash(frozenset([self.key, self.subkey, self.value]))


def Record(*args, **kws):
    """Returns mapreduce-like record with key, subkey, value."""
    assert len(args) >= 2, "incorrect arguments count [ARGS: %s]" % repr(args)
    if len(args) < 3:
        return SimpleRecord(*args, **kws)
    return SubkeyedRecord(*args[:3], **kws)
