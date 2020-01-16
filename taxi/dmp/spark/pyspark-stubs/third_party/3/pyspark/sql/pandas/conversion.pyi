from typing import overload
from typing import Any, Iterable, List, Optional, Tuple, Union

from pyspark import since as since
from pyspark.rdd import RDD
import pyspark.sql.dataframe
from pyspark.sql.pandas.serializers import ArrowCollectSerializer as ArrowCollectSerializer
from pyspark.sql.types import *
from pyspark.traceback_utils import SCCallSiteSync as SCCallSiteSync

import pandas.core.frame # type: ignore

basestring = str
unicode = str
xrange = range

class PandasConversionMixin:
    def toPandas(self) -> pandas.core.frame.DataFrame: ...

class SparkConversionMixin:
    @overload
    def createDataFrame(self, data: Union[RDD[Union[Tuple, List]], Iterable[Union[Tuple, List]], pandas.core.frame.DataFrame], samplingRatio: Optional[float] = ...) ->  pyspark.sql.dataframe.DataFrame: ...
    @overload
    def createDataFrame(self, data: Union[RDD[Union[Tuple, List]], Iterable[Union[Tuple, List]], pandas.core.frame.DataFrame], schema: Optional[Union[List[str], Tuple[str, ...]]] = ..., samplingRatio: Optional[float] = ...) ->  pyspark.sql.dataframe.DataFrame: ...
