from typing import Any

from pyspark.sql._typing import ColumnOrName, LiteralType, MapIterPandasUserDefinedFunction
from pyspark import since as since
from pyspark.rdd import PythonEvalType as PythonEvalType

import pyspark.sql.dataframe

class PandasMapOpsMixin:
    def mapInPandas(self, udf: MapIterPandasUserDefinedFunction) -> pyspark.sql.dataframe.DataFrame: ...
