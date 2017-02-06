from typing import Any, List, Optional, TypeVar, Union
import pyspark.sql.column

ColumnOrName = Union[pyspark.sql.column.Column, str]
Literal = Union[bool, int, float, str]
LiteralType = TypeVar("LiteralType", bool, int, float, str)



