from typing import Union

from pyspark.sql.pandas._typing import (
    GroupedMapPandasUserDefinedFunction,
    CogroupedMapPandasUserDefinedFunction,
    PandasGroupedMapFunction,
    PandasCogroupedMapFunction,
)

from pyspark import since as since
from pyspark.rdd import PythonEvalType as PythonEvalType
from pyspark.sql.column import Column as Column
from pyspark.sql.context import SQLContext
import pyspark.sql.group
from pyspark.sql.dataframe import DataFrame as DataFrame
from pyspark.sql.types import StructType

class PandasGroupedOpsMixin:
    def cogroup(self, other: pyspark.sql.group.GroupedData) -> PandasCogroupedOps: ...
    def apply(self, udf: GroupedMapPandasUserDefinedFunction) -> DataFrame: ...
    def applyInPandas(
        self, func: PandasGroupedMapFunction, schema: Union[StructType, str]
    ) -> DataFrame: ...

class PandasCogroupedOps:
    sql_ctx: SQLContext
    def __init__(
        self, gd1: pyspark.sql.group.GroupedData, gd2: pyspark.sql.group.GroupedData
    ) -> None: ...
    def applyInPandas(
        self, func: PandasCogroupedMapFunction, schema: Union[StructType, str]
    ) -> DataFrame: ...
