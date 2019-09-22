from typing import Any, Callable, Iterable, List, NewType, Optional, Tuple, TypeVar, Union
from typing_extensions import Protocol, Literal
from types import FunctionType

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
PandasCogroupedMapUDFType = Literal[206]
PandasGroupedAggUDFType = Literal[202]
PandasMapIterUDFType = Literal[205]

class PandasVariadicScalarToScalarFunction(Protocol):
    def __call__(self, *_: pandas.core.series.Series) -> pandas.core.series.Series:
        ...

PandasScalarToScalarFunction = Union[PandasVariadicScalarToScalarFunction, Callable[[pandas.core.series.Series], pandas.core.series.Series], Callable[[pandas.core.series.Series, pandas.core.series.Series], pandas.core.series.Series], Callable[[pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series], pandas.core.series.Series], Callable[[pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series], pandas.core.series.Series], Callable[[pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series], pandas.core.series.Series], Callable[[pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series], pandas.core.series.Series], Callable[[pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series], pandas.core.series.Series], Callable[[pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series], pandas.core.series.Series], Callable[[pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series], pandas.core.series.Series], Callable[[pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series], pandas.core.series.Series]]

class PandasVariadicScalarToStructFunction(Protocol):
    def __call__(self, *_: pandas.core.series.Series) -> pandas.core.frame.DataFrame:
        ...

PandasScalarToStructFunction = Union[PandasVariadicScalarToStructFunction, Callable[[pandas.core.series.Series], pandas.core.frame.DataFrame], Callable[[pandas.core.series.Series, pandas.core.series.Series], pandas.core.frame.DataFrame], Callable[[pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series], pandas.core.frame.DataFrame], Callable[[pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series], pandas.core.frame.DataFrame], Callable[[pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series], pandas.core.frame.DataFrame], Callable[[pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series], pandas.core.frame.DataFrame], Callable[[pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series], pandas.core.frame.DataFrame], Callable[[pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series], pandas.core.frame.DataFrame], Callable[[pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series], pandas.core.frame.DataFrame], Callable[[pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series], pandas.core.frame.DataFrame]]

PandasScalarIterFunction = Callable[[Iterable[Union[pandas.core.series.Series, Tuple[pandas.core.series.Series, ...], pandas.core.frame.DataFrame]]], Iterable[pandas.core.series.Series]]

PandasGroupedMapFunction = Union[Callable[[pandas.core.frame.DataFrame], pandas.core.frame.DataFrame], Callable[[Any, pandas.core.frame.DataFrame], pandas.core.frame.DataFrame]]

class PandasVariadicGroupedAggFunction(Protocol):
    def __call__(self, *_: pandas.core.series.Series) -> LiteralType:
        ...

PandasGroupedAggFunction = Union[Callable[[pandas.core.series.Series], LiteralType], Callable[[pandas.core.series.Series, pandas.core.series.Series], LiteralType], Callable[[pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series], LiteralType], Callable[[pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series], LiteralType], Callable[[pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series], LiteralType], Callable[[pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series], LiteralType], Callable[[pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series], LiteralType], Callable[[pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series], LiteralType], Callable[[pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series], LiteralType], Callable[[pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series, pandas.core.series.Series], LiteralType], PandasVariadicGroupedAggFunction]

PandasMapIterFunction = Callable[[Iterable[pandas.core.frame.DataFrame]], Iterable[pandas.core.frame.DataFrame]]

PandasCogroupedMapFunction = Callable[[pandas.core.frame.DataFrame, pandas.core.frame.DataFrame], pandas.core.frame.DataFrame]

class UserDefinedFunctionLike(Protocol):
    def __call__(self, *_: ColumnOrName) -> Column:
        ...

MapIterPandasUserDefinedFunction = NewType("MapIterPandasUserDefinedFunction", FunctionType)
GroupedMapPandasUserDefinedFunction = NewType("GroupedMapPandasUserDefinedFunction", FunctionType)
CogroupedMapPandasUserDefinedFunction = NewType("CogroupedMapPandasUserDefinedFunction", FunctionType)
