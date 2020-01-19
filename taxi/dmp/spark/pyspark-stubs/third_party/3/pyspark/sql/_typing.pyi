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

PandasScalarUDFType = Literal[200]
PandasScalarIterUDFType = Literal[204]
PandasGroupedMapUDFType = Literal[201]
PandasCogroupedMapUDFType = Literal[206]
PandasGroupedAggUDFType = Literal[202]
PandasMapIterUDFType = Literal[205]

class PandasVariadicScalarToScalarFunction(Protocol):
    def __call__(self, *_: SeriesLike) -> SeriesLike:
        ...

PandasScalarToScalarFunction = Union[PandasVariadicScalarToScalarFunction, Callable[[SeriesLike], SeriesLike], Callable[[SeriesLike, SeriesLike], SeriesLike], Callable[[SeriesLike, SeriesLike, SeriesLike], SeriesLike], Callable[[SeriesLike, SeriesLike, SeriesLike, SeriesLike], SeriesLike], Callable[[SeriesLike, SeriesLike, SeriesLike, SeriesLike, SeriesLike], SeriesLike], Callable[[SeriesLike, SeriesLike, SeriesLike, SeriesLike, SeriesLike, SeriesLike], SeriesLike], Callable[[SeriesLike, SeriesLike, SeriesLike, SeriesLike, SeriesLike, SeriesLike, SeriesLike], SeriesLike], Callable[[SeriesLike, SeriesLike, SeriesLike, SeriesLike, SeriesLike, SeriesLike, SeriesLike, SeriesLike], SeriesLike], Callable[[SeriesLike, SeriesLike, SeriesLike, SeriesLike, SeriesLike, SeriesLike, SeriesLike, SeriesLike, SeriesLike], SeriesLike], Callable[[SeriesLike, SeriesLike, SeriesLike, SeriesLike, SeriesLike, SeriesLike, SeriesLike, SeriesLike, SeriesLike, SeriesLike], SeriesLike]]

class PandasVariadicScalarToStructFunction(Protocol):
    def __call__(self, *_: SeriesLike) -> DataFrameLike:
        ...

PandasScalarToStructFunction = Union[PandasVariadicScalarToStructFunction, Callable[[SeriesLike], DataFrameLike], Callable[[SeriesLike, SeriesLike], DataFrameLike], Callable[[SeriesLike, SeriesLike, SeriesLike], DataFrameLike], Callable[[SeriesLike, SeriesLike, SeriesLike, SeriesLike], DataFrameLike], Callable[[SeriesLike, SeriesLike, SeriesLike, SeriesLike, SeriesLike], DataFrameLike], Callable[[SeriesLike, SeriesLike, SeriesLike, SeriesLike, SeriesLike, SeriesLike], DataFrameLike], Callable[[SeriesLike, SeriesLike, SeriesLike, SeriesLike, SeriesLike, SeriesLike, SeriesLike], DataFrameLike], Callable[[SeriesLike, SeriesLike, SeriesLike, SeriesLike, SeriesLike, SeriesLike, SeriesLike, SeriesLike], DataFrameLike], Callable[[SeriesLike, SeriesLike, SeriesLike, SeriesLike, SeriesLike, SeriesLike, SeriesLike, SeriesLike, SeriesLike], DataFrameLike], Callable[[SeriesLike, SeriesLike, SeriesLike, SeriesLike, SeriesLike, SeriesLike, SeriesLike, SeriesLike, SeriesLike, SeriesLike], DataFrameLike]]

PandasScalarIterFunction = Callable[[Iterable[Union[SeriesLike, Tuple[SeriesLike, ...], DataFrameLike]]], Iterable[SeriesLike]]

PandasGroupedMapFunction = Union[Callable[[DataFrameLike], DataFrameLike], Callable[[Any, DataFrameLike], DataFrameLike]]

class PandasVariadicGroupedAggFunction(Protocol):
    def __call__(self, *_: SeriesLike) -> LiteralType:
        ...

PandasGroupedAggFunction = Union[Callable[[SeriesLike], LiteralType], Callable[[SeriesLike, SeriesLike], LiteralType], Callable[[SeriesLike, SeriesLike, SeriesLike], LiteralType], Callable[[SeriesLike, SeriesLike, SeriesLike, SeriesLike], LiteralType], Callable[[SeriesLike, SeriesLike, SeriesLike, SeriesLike, SeriesLike], LiteralType], Callable[[SeriesLike, SeriesLike, SeriesLike, SeriesLike, SeriesLike, SeriesLike], LiteralType], Callable[[SeriesLike, SeriesLike, SeriesLike, SeriesLike, SeriesLike, SeriesLike, SeriesLike], LiteralType], Callable[[SeriesLike, SeriesLike, SeriesLike, SeriesLike, SeriesLike, SeriesLike, SeriesLike, SeriesLike], LiteralType], Callable[[SeriesLike, SeriesLike, SeriesLike, SeriesLike, SeriesLike, SeriesLike, SeriesLike, SeriesLike, SeriesLike], LiteralType], Callable[[SeriesLike, SeriesLike, SeriesLike, SeriesLike, SeriesLike, SeriesLike, SeriesLike, SeriesLike, SeriesLike, SeriesLike], LiteralType], PandasVariadicGroupedAggFunction]

PandasMapIterFunction = Callable[[Iterable[DataFrameLike]], Iterable[DataFrameLike]]

PandasCogroupedMapFunction = Callable[[DataFrameLike, DataFrameLike], DataFrameLike]

class UserDefinedFunctionLike(Protocol):
    def __call__(self, *_: ColumnOrName) -> Column:
        ...

MapIterPandasUserDefinedFunction = NewType("MapIterPandasUserDefinedFunction", FunctionType)
GroupedMapPandasUserDefinedFunction = NewType("GroupedMapPandasUserDefinedFunction", FunctionType)
CogroupedMapPandasUserDefinedFunction = NewType("CogroupedMapPandasUserDefinedFunction", FunctionType)
