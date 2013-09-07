from common import flatten, require, bool_to_string
from errors import YtError
from etc_commands import parse_ypath
import config

from yt.yson import YsonString

def check_prefix(prefix):
    require(prefix.startswith("//"),
            YtError("PREFIX should start with //"))
    require(prefix.endswith("/"),
            YtError("PREFIX should end with /"))

class TablePath(object):
    """
    Represents path to table with attributes:
    append -- append to table or overwrite
    columns -- list of string (column) or string pairs (column range).
    lower_key, upper_key -- tuple of strings to identify range of records
    start_index, end_index
    """
    def __init__(self, name,
                 append=None, sorted_by=None,
                 columns=None,
                 lower_key=None, upper_key=None,
                 start_index=None, end_index=None,
                 simplify=True):
        self._append = append
        if simplify:
            self.name = parse_ypath(name)
            for key, value in self.name.attributes.items():
                if "-" in key:
                    self.name.attributes[key.replace("-", "_")] = value
                    del self.name.attributes[key]
        else:
            self.name = YsonString(name)

        if self.name != "/" and not self.name.startswith("//") and not self.name.startswith("#"):
            prefix = config.PREFIX
            require(prefix,
                    YtError("Path '%s' should be absolute or you should specify a prefix" % self.name))
            require(prefix.startswith("//"),
                    YtError("PREFIX '%s' should start with //" % prefix))
            require(prefix.endswith("/"),
                    YtError("PREFIX '%s' should end with /" % prefix))
            # TODO(ignat): refactor YsonString to fix this hack
            attributes = self.name.attributes
            self.name = YsonString(prefix + self.name if self.name else prefix[:-1])
            self.name.attributes = attributes

        attributes = self.name.attributes
        if append is not None:
            attributes["append"] = bool_to_string(append)
        else:
            attributes["append"] = attributes.get("append", "false")
        if sorted_by is not None:
            attributes["sorted_by"] = sorted_by
        if columns is not None:
            attributes["channel"] = columns

        has_index = start_index is not None or end_index is not None
        has_key = lower_key is not None or upper_key is not None
        require(not (has_index and has_key),
                YtError("You could not specify key bound and index bound simultaneously"))
        if lower_key is not None:
            attributes["lower_limit"] = {"key": flatten(lower_key)}
        if upper_key is not None:
            if config.USE_NON_STRICT_UPPER_KEY:
                upper_key = upper_key + "\0"
            attributes["upper_limit"] = {"key": flatten(upper_key)}
        if start_index is not None:
            attributes["lower_limit"] = {"row_index": start_index}
        if end_index is not None:
            attributes["upper_limit"] = {"row_index": end_index}

    @property
    def append(self):
        return self._append

    @append.setter
    def append(self, value):
        self._append = value
        self.name.attributes["append"] = bool_to_string(self._append)

    def has_delimiters(self):
        return any(key in self.name.attributes for key in ["channel", "lower_limit", "upper_limit"])

    def get_json(self):
        return {"$value": str(self.name), "$attributes": self.name.attributes}

    def __eq__(self, other):
        return self.name == other.name

    def __hash__(self):
        return hash(self.name)

    def __str__(self):
        return str(self.name)

    def __repr__(self):
        return str(self)

def to_table(object):
    if isinstance(object, TablePath):
        return object
    else:
        return TablePath(object)

def to_name(object):
    return to_table(object).name

def prepare_path(object):
    return to_table(object).get_json()

