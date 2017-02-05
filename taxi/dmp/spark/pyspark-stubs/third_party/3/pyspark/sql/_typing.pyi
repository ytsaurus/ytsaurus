from typing import Any, List, Optional, TypeVar, Union
from pyspark.sql.types import *
from pyspark.sql.column import Column

ColumnOrName = Union[Column, str]
Literal = Union[bool, int, float, str]
LiteralType = TypeVar("LiteralType", bool, int, float, str)



