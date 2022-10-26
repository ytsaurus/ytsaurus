from typing import overload
from typing import Any, Iterable, List, Optional, Tuple, Union

from pyspark.sql.pandas._typing import DataFrameLike
from pyspark import since as since
from pyspark.rdd import RDD
import pyspark.sql.dataframe
from pyspark.sql.pandas.serializers import (
    ArrowCollectSerializer as ArrowCollectSerializer,
)
from pyspark.sql.types import *
from pyspark.traceback_utils import SCCallSiteSync as SCCallSiteSync

basestring = str
unicode = str
xrange = range

class PandasConversionMixin:
    def toPandas(self) -> DataFrameLike: ...

class SparkConversionMixin:
    @overload
    def createDataFrame(
        self, data: DataFrameLike, samplingRatio: Optional[float] = ...
    ) -> pyspark.sql.dataframe.DataFrame: ...
    @overload
    def createDataFrame(
        self,
        data: DataFrameLike,
        schema: Union[StructType, str],
        verifySchema: bool = ...,
    ) -> pyspark.sql.dataframe.DataFrame: ...
