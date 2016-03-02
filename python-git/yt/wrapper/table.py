from common import flatten, require, bool_to_string, parse_bool, update
from errors import YtError
from etc_commands import parse_ypath
from config import get_config

from yt.yson import YsonString
import yt.yson as yson

from contextlib import contextmanager

def check_prefix(prefix):
    require(prefix.startswith("//"),
            YtError("PREFIX should start with //"))
    require(prefix.endswith("/"),
            YtError("PREFIX should end with /"))

class TablePath(object):
    """
    Table address in Cypress tree with some modifiers.

    Attributes:

    * name -- path to the table. It can contain YPath-style attributes.

    * append -- append to table or overwrite

    * columns -- list of string (column) or string pairs (column range).

    * exact_key, lower_key, upper_key -- tuple of strings to identify range of rows

    * exact_index, start_index, end_index -- tuple of indexes to identify range of rows

    * simplify -- request proxy to parse YPATH

    .. seealso:: `YPath on wiki <https://wiki.yandex-team.ru/yt/Design/YPath>`_
    """
    def __init__(self,
                 name,
                 append=None,
                 sorted_by=None,
                 columns=None,
                 exact_key=None,
                 lower_key=None,
                 upper_key=None,
                 exact_index=None,
                 start_index=None,
                 end_index=None,
                 ranges=None,
                 simplify=True,
                 attributes=None,
                 client=None):
        """
        :param name: (Yson string) path with attribute
        :param append: (bool) append to table or overwrite
        :param sorted_by: (list of string) list of sort keys
        :param columns: list of string (column) or string pairs (column range)
        :param exact_key: (string or string tuple) exact key of row
        :param lower_key: (string or string tuple) lower key bound of rows
        :param upper_key: (string or string tuple) upper bound of rows
        :param exact_index: (int) exact index of row
        :param start_index: (int) lower bound of rows
        :param end_index: (int) upper bound of rows
        :param ranges: (list) list of ranges of rows. It overwrites all other row limits.
        :param attributes: (dict) attributes, it updates attributes specified in name.


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

        if attributes is not None:
            self.name.attributes = update(self.name.attributes, attributes)

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

        if exact_key is not None:
            attributes["exact"] = {"key": flatten(exact_key)}
        if lower_key is not None:
            attributes["lower_limit"] = {"key": flatten(lower_key)}
        if upper_key is not None:
            if get_config(client)["yamr_mode"]["use_non_strict_upper_key"]:
                upper_key = upper_key + "\0"
            attributes["upper_limit"] = {"key": flatten(upper_key)}
        if exact_index is not None:
            attributes["exact"] = {"row_index": flatten(exact_index)}
        if start_index is not None:
            attributes["lower_limit"] = {"row_index": start_index}
        if end_index is not None:
            attributes["upper_limit"] = {"row_index": end_index}

        if ranges is not None:
            attributes["ranges"] = ranges
            if "exact" in attributes:
                del attributes["exact"]
            if "lower_limit" in attributes:
                del attributes["lower_limit"]
            if "upper_limit" in attributes:
                del attributes["upper_limit"]

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

    def to_yson_string(self):
        """Return yson path with attributes as string"""
        # NB: in text format \n can appear only as separator.
        return "<{0}>{1}".format(yson.dumps(self.name.attributes, yson_type="map_fragment", yson_format="text").replace("\n", ""), str(self.name))

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

@contextmanager
def TempTable(path=None, prefix=None, client=None):
    """Create temporary table in given path with given prefix on scope enter and remove it on scope exit.
       .. seealso:: :py:func:`yt.wrapper.table_commands.create_temp_table`
    """
    from cypress_commands import remove
    from table_commands import create_temp_table

    table = create_temp_table(path, prefix, client=client)
    try:
        yield table
    finally:
        remove(table, force=True, client=client)
