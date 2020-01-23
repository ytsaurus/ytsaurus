from typing import Any, Union

from pyspark.sql._typing import ColumnOrName, LiteralType, MapIterPandasUserDefinedFunction, PandasMapIterFunction
from pyspark import since as since
from pyspark.rdd import PythonEvalType as PythonEvalType
from pyspark.sql.types import StructType
import pyspark.sql.dataframe

class PandasMapOpsMixin:
    def mapInPandas(self, udf: PandasMapIterFunction, schema: Union[StructType, str]) -> pyspark.sql.dataframe.DataFrame: ...
