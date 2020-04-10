# Stubs for pyspark.sql.avro.functions (Python 3)

from typing import Any, Dict

from pyspark.sql._typing import ColumnOrName
from pyspark.sql.column import Column

def from_avro(
    data: ColumnOrName, jsonFormatSchema: str, options: Dict[str, str] = ...
) -> Column: ...
def to_avro(data: ColumnOrName, jsonFormatSchema: str = ...) -> Column: ...
