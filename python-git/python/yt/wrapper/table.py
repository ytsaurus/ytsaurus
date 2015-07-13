from common import flatten, require, bool_to_string, parse_bool
from errors import YtError
from etc_commands import parse_ypath
from config import get_config

from yt.yson import YsonString

def check_prefix(prefix):
    require(prefix.startswith("//"),
            YtError("PREFIX should start with //"))
    require(prefix.endswith("/"),
            YtError("PREFIX should end with /"))

class TablePath(object):
    """
    Table address in Cypress tree with some modifiers.

    Attributes:

    * append -- append to table or overwrite

    * columns -- list of string (column) or string pairs (column range).

    * lower_key, upper_key -- tuple of strings to identify range of rows

    * start_index, end_index -- tuple of indexes to identify range of rows

    * simplify -- request proxy to parse YPATH

    .. seealso:: `YPath on wiki <https://wiki.yandex-team.ru/yt/Design/YPath>`_
    """
    def __init__(self,
                 name,
                 append=None,
                 sorted_by=None,
                 columns=None,
                 lower_key=None, upper_key=None,
                 start_index=None, end_index=None,
                 simplify=True,
                 client=None):
        """
        :param name: (Yson string) path with attribute
        :param append: (bool) append to table or overwrite
        :param sorted_by: (list of string) list of sort keys
        :param columns: list of string (column) or string pairs (column range)
        :param lower_key: (string or string tuple) lower key bound of rows
        :param upper_key: (string or string tuple) upper bound of rows
        :param start_index: (int) lower bound of rows
        :param end_index: (int) upper bound of rows

        .. note:: 'upper_key' and 'lower_key' are special YT terms. \
        `See usage example. <https://wiki.yandex-team.ru/yt/Design/YPath#modifikatorydiapazonovtablicy>`_
        .. note:: don't specify lower_key (upper_key) and start_index (end_index) simultaneously
        .. note:: param `simplify` will be removed
        """
        self._append = append
        if simplify:
            self.name = parse_ypath(name, client=client)
            for key, value in self.name.attributes.items():
                if "-" in key:
                    self.name.attributes[key.replace("-", "_")] = value
                    del self.name.attributes[key]
        else:
            self.name = YsonString(name)

        if str(self.name) != "/" and not self.name.startswith("//") and not self.name.startswith("#"):
            prefix = get_config(client)["prefix"]
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
        if "channel" in attributes:
            attributes["columns"] = attributes["channel"]
            del attributes["channel"]
        if append is not None:
            self.append = append
        else:
            self.append = attributes.get("append", False)
        if sorted_by is not None:
            attributes["sorted_by"] = sorted_by
        if columns is not None:
            attributes["columns"] = columns

        if start_index is not None and lower_key is not None:
            raise YtError("You could not specify lower key bound and start index simultaneously")
        if end_index is not None and upper_key is not None:
            raise YtError("You could not specify upper key bound and end index simultaneously")

        # NB(ignat): for bakcward compatibility with 0.16
        if "columns" in attributes:
            attributes["channel"] = attributes["columns"]
            del attributes["columns"]

        if lower_key is not None:
            attributes["lower_limit"] = {"key": flatten(lower_key)}
        if upper_key is not None:
            if get_config(client)["yamr_mode"]["use_non_strict_upper_key"]:
                upper_key = upper_key + "\0"
            attributes["upper_limit"] = {"key": flatten(upper_key)}
        if start_index is not None:
            attributes["lower_limit"] = {"row_index": start_index}
        if end_index is not None:
            attributes["upper_limit"] = {"row_index": end_index}

    @property
    def attributes(self):
        return self.name.attributes

    @property
    def append(self):
        return parse_bool(self._append)

    @append.setter
    def append(self, value):
        self._append = value
        self.name.attributes["append"] = bool_to_string(self._append)

    def has_delimiters(self):
        """Check attributes for delimiters (channel, lower or upper limits)"""
        return any(key in self.name.attributes for key in ["columns", "lower_limit", "upper_limit", "ranges"])

    def to_yson_type(self):
        """Return YSON representation of path"""
        return self.name

    def __eq__(self, other):
        return str(self.name) == str(other.name)

    def __hash__(self):
        return hash(self.name)

    def __str__(self):
        return str(self.name)

    def __repr__(self):
        return str(self)

def to_table(object, client=None):
    """Return `TablePath` object"""
    if isinstance(object, TablePath):
        return object
    else:
        return TablePath(object, client=client)

def to_name(object, client=None):
    """Return `YsonString` name of path"""
    return to_table(object, client=client).name

def prepare_path(object, client=None):
    return to_table(object, client=client).to_yson_type()
