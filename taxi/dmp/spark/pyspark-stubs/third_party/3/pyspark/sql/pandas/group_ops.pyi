from typing import Any

from pyspark.sql._typing import GroupedMapPandasUserDefinedFunction, CogroupedMapPandasUserDefinedFunction

from pyspark import since as since
from pyspark.rdd import PythonEvalType as PythonEvalType
from pyspark.sql.column import Column as Column
from pyspark.sql.context import SQLContext
import pyspark.sql.cogroup
import pyspark.sql.group
from pyspark.sql.dataframe import DataFrame as DataFrame

class PandasGroupedOpsMixin:
    def cogroup(self, other: pyspark.sql.group.GroupedData) -> pyspark.sql.cogroup.CoGroupedData: ...
    def apply(self, udf: GroupedMapPandasUserDefinedFunction) -> DataFrame: ...

class PandasCogroupedOps:
    sql_ctx: SQLContext
    def __init__(self, gd1: pyspark.sql.group.GroupedData, gd2:pyspark.sql.group.GroupedData) -> None: ...
    def apply(self, udf: CogroupedMapPandasUserDefinedFunction) -> DataFrame: ...
