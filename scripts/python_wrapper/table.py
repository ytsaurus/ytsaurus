from common import flatten, require, YtError

class Table(object):
    """ Columns should be list of string (column) or string pairs(column range) """
    def __init__(self, name, append=False, columns=None, lower_key=None, upper_key=None):
        self.name = name
        self.append = append
        self.columns = columns
        self.lower_key = lower_key
        self.upper_key = upper_key

    def yson_name(self):
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
            return "(%s)" % ",".join(flatten(key))
        
        name = self.name
        if self.columns is not None:
            name = "%s{%s}" % \
                (name, ",".join(map(column_to_str, self.columns)))
        if self.lower_key is not None or self.upper_key is not None:
            name = "%s[%s]" % \
                (name, ":".join(map(key_to_str, [self.lower_key, self.upper_key])))

        return name

    def __eq__(self, other):
        return self.name == other.name

    def __hash__(self):
        return hash(self.name)

def get_yson_name(table):
    return table.yson_name()

def to_table(object):
    if isinstance(object, Table):
        return object
    else:
        return Table(object)
