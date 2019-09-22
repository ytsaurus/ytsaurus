# Stubs for pyspark.sql.cogroup (Python 3)
#

from pyspark.sql._typing import CogroupedMapPandasUserDefinedFunction
from pyspark.sql.context import SQLContext
from pyspark.sql.dataframe import DataFrame
import pyspark.sql.group

class CoGroupedData:
    sql_ctx: SQLContext = ...
    def __init__(self, gd1: pyspark.sql.group.GroupedData, gd2: pyspark.sql.group.GroupedData) -> None: ...
    def apply(self, udf: CogroupedMapPandasUserDefinedFunction) -> DataFrame: ...
