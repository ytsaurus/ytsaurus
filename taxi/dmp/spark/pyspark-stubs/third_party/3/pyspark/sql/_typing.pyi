from typing import Any, Callable, Iterable, List, NewType, Optional, Tuple, TypeVar, Union
from typing_extensions import Protocol, Literal
from types import FunctionType

import datetime
import decimal

import pyspark.sql.column
import pyspark.sql.types
from pyspark.sql.column import Column

from pyspark.sql.pandas._typing import DataFrameLike, SeriesLike
import pandas.core.frame      # type: ignore
import pandas.core.series     # type: ignore

ColumnOrName = Union[pyspark.sql.column.Column, str]
DecimalLiteral = decimal.Decimal
DateTimeLiteral = Union[datetime.datetime, datetime.date]
LiteralType = Union[bool, int, float, str]
AtomicDataTypeOrString = Union[pyspark.sql.types.AtomicType, str]
DataTypeOrString = Union[pyspark.sql.types.DataType, str]

RowLike = TypeVar("RowLike", List[Any], Tuple[Any, ...], pyspark.sql.types.Row)

class SupportsOpen(Protocol):
    def open(self, partition_id: int, epoch_id: int) -> bool:
        ...

class SupportsProcess(Protocol):
    def process(self, row: pyspark.sql.types.Row) -> None:
        ...

class SupportsClose(Protocol):
    def close(self, error: Exception) -> None:
        ...

class UserDefinedFunctionLike(Protocol):
    def __call__(self, *_: ColumnOrName) -> Column:
        ...
