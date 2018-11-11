from typing import Any, List, Optional, TypeVar, Union
from typing_extensions import Protocol
import datetime
import decimal

import pyspark.sql.column
import pyspark.sql.types

ColumnOrName = Union[pyspark.sql.column.Column, str]
DecimalLiteral = decimal.Decimal
DateTimeLiteral = Union[datetime.datetime, datetime.date]
LiteralType = Union[bool, int, float, str]

class SupportsOpen(Protocol):
    def open(self, partition_id: int, epoch_id: int) -> bool:
        ...

class SupportsProcess(Protocol):
    def process(self, row: pyspark.sql.types.Row) -> None:
        ...

class SupportsClose(Protocol):
    def close(self, error: Exception) -> None:
        ...
