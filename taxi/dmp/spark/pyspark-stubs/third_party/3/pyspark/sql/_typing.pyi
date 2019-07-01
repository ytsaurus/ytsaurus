from typing import Any, List, Optional, TypeVar, Union
from typing_extensions import Protocol, Literal
import datetime
import decimal

import pyspark.sql.column
import pyspark.sql.types
from pyspark.sql.column import Column

import pandas.core.frame      # type: ignore
import pandas.core.series     # type: ignore

ColumnOrName = Union[pyspark.sql.column.Column, str]
DecimalLiteral = decimal.Decimal
DateTimeLiteral = Union[datetime.datetime, datetime.date]
LiteralType = Union[bool, int, float, str]
AtomicDataTypeOrString = Union[pyspark.sql.types.AtomicType, str]
DataTypeOrString = Union[pyspark.sql.types.DataType, str]

class SupportsOpen(Protocol):
    def open(self, partition_id: int, epoch_id: int) -> bool:
        ...

class SupportsProcess(Protocol):
    def process(self, row: pyspark.sql.types.Row) -> None:
        ...

class SupportsClose(Protocol):
    def close(self, error: Exception) -> None:
        ...

PandasScalarUDFType = Literal[200]
PandasScalarIterUDFType = Literal[204]
PandasGroupedMapUDFType = Literal[201]
PandasGroupedAggUDFType = Literal[202]
PandasMapIterUDFType = Literal[205]

class PandasScalarFunction(Protocol):
    def __call__(self, *_: pandas.core.series.Series) -> pandas.core.series.Series:
        ...

class PandasGroupedMapFunction(Protocol):
    def __call__(self, _: pandas.core.frame.DataFrame) -> pandas.core.frame.DataFrame:
        ...

class PandasGroupedAggFunction(Protocol):
    def __call__(self, *_: pandas.core.series.Series) -> LiteralType:
        ...

class UserDefinedFunctionLike(Protocol):
    def __call__(self, *_: ColumnOrName) -> Column:
        ...
