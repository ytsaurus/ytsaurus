from itertools import imap, chain, izip

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

    def __eq__(self, other):
        return self.key == other.key and \
               self.subkey == other.subkey and \
               self.value == other.value


def Record(*args, **kws):
    assert len(args) >= 2, "incorrect arguments count [ARGS: %s]" % repr(args)
    if len(args) < 3:
        return SimpleRecord(*args, **kws)
    return SubkeyedRecord(*args[:3], **kws)

def record_to_line(rec, eoln=True):
    return "{0!s}\t{1!s}\t{2!s}{3}".\
            format(rec.key, rec.subkey, rec.value, "\n" if eoln else "")

def line_to_record(line):
    return Record(*line.strip("\n").split("\t", 2))

""" Methods for processing records """
def yt_to_record(line):
    record_dict = dict([word.split("=") for word in line.strip("\n").split("\t", 2)])
    return Record(record_dict["key"], record_dict["subkey"], record_dict["value"])

def record_to_yt(rec):
    keys = ["key", "subkey", "value"]
    return "\t".join("=".join(x) for x in izip(keys, [rec.key, rec.subkey, rec.value]))

def line_to_yt(line):
    return record_to_yt(line_to_record(line))

def yt_to_line(line):
    return record_to_line(yt_to_record(line))

def python_map(function, records):
    return chain(*imap(function, records))

