"""
YSON library.

Package supports `YT YSON format <https://wiki.yandex-team.ru/yt/userdoc/yson>`_.

Package provides special classes for all yson types, see `yt.yson.types`.
Also it provides methods for serialization and deserialization yson data:
`yt.yson.parser.load`, `yt.yson.parser.loads`, `yt.yson.writer.dump`, `yt.yson.writer.dumps`.
And finally it provides method `yt.yson.convert.to_yson_type` for convertion python objects to special yson types.

In special variable `TYPE` you can find implementation type of the library.
In equals "BINARY" if c++ bindings found and "PYTHON" otherwise.


Examples:

>>> import yt.yson as yson
>>> yson.loads("{a=10}")
{'a': 10}

>>> yson.dumps(True)
'"true"'

>>> number = yson.YsonInteger(10)
>>> number.attributes["my_attr"] = "hello"
>>> yson.dumps(number)
'<"attr"="hello">10'

>>> boolean = to_yson_type(False, attributes={"my_attr": "my_value"})

"""

import writer
import parser
import yson_types

try:
    from yt_yson_bindings import load, loads, dump, dumps
    TYPE = "BINARY"
except ImportError as error:
    from parser import load, loads
    from writer import dump, dumps
    TYPE = "PYTHON"

from yson_types import YsonString, YsonInt64, YsonUint64, YsonDouble, YsonBoolean, YsonList, YsonMap, YsonEntity
from convert import to_yson_type, yson_to_json, json_to_yson
from common import YsonError
