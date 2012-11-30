from common import flatten, require, YtError, bool_to_string
from path_tools import split_table_ranges

class TablePath(object):
    """
    Represents path to table with attributes:
    append -- append to table or overwrite
    columns -- list of string (column) or string pairs (column range).
    lower_key, upper_key -- tuple of strings to identify range of records
    start_index, end_index
    """
    def __init__(self, name, append=False, columns=None,
                 lower_key=None, upper_key=None,
                 start_index=None, end_index=None):
        self.name, self.specificators = split_table_ranges(name)
        self.append = append
        self.columns = columns
        self.lower_key = lower_key
        self.upper_key = upper_key
        self.start_index = start_index
        self.end_index = end_index
        require(not (self.specificators and (columns is not None or
                                             lower_key is not None or
                                             upper_key is not None or
                                             start_index is not None or
                                             end_index is not None)),
                YtError("You should not use ranges both in the name and in the variables"))

        self.has_index = start_index is not None or end_index is not None
        self.has_key = lower_key is not None or upper_key is not None
        require(not (self.has_index and self.has_key),
                YtError("You could not specify key bound and index bound simultaneously"))

    def get_name(self, use_ranges=False, use_overwrite=False):
        def column_to_str(column):
            column = flatten(column)
            require(len(column) <= 2,
                    YtError("Incorrect column " + str(column)))
            if len(column) == 1:
                return column[0]
            else:
                return ":".join(column)

        def key_to_str(key):
            if key is None:
                return ""
            return '("%s")' % ",".join(flatten(key))

        def index_to_str(index):
            if index is None:
                return ""
            return '#%d' % index

        name = self.name
        if use_ranges:
            name = name + self.specificators
            if self.columns is not None:
                name = "%s{%s}" % \
                    (name, ",".join(map(column_to_str, self.columns)))
            if self.has_key:
                name = "%s[%s]" % \
                    (name, ":".join(map(key_to_str, [self.lower_key, self.upper_key])))

            if self.has_index:
                name = "%s[%s]" % \
                    (name, ":".join(map(index_to_str, [self.start_index, self.end_index])))

        if use_overwrite:
            name = {
                "$value": name,
                "$attributes": {
                    "overwrite": bool_to_string(not self.append)
                }
            }

        return name

    def has_delimiters(self):
        return \
            self.columns is not None or \
            self.lower_key is not None or \
            self.upper_key is not None or \
            self.start_index is not None or \
            self.end_index is not None


    def __eq__(self, other):
        return self.name == other.name

    def __hash__(self):
        return hash(self.name)

    def __str__(self):
        return self.get_name(use_ranges=True)
    
    def __repr__(self):
        return str(self)

def to_table(object):
    if isinstance(object, TablePath):
        return object
    else:
        return TablePath(object)

def to_name(object):
    return to_table(object).get_name()
