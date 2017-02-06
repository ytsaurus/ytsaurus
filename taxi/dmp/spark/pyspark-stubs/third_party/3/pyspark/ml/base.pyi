# Stubs for pyspark.ml.base (Python 3.5)
#

from typing import overload
from typing import Any, Dict, List, Optional
from pyspark.ml.param import Params, Param
from pyspark.sql.dataframe import DataFrame

ParamMap = Dict[Param, Any]

class Estimator(Params):
    __metaclass__ = ...  # type: Any
    @overload
    def fit(self, dataset: DataFrame, params: Optional[ParamMap] = ...) -> Model: ...
    @overload
    def fit(self, dataset: DataFrame, params: List[ParamMap]) -> List[Model]: ...

class Transformer(Params):
    __metaclass__ = ...  # type: Any
    def transform(self, dataset: DataFrame, params: Optional[ParamMap] = ...) -> DataFrame: ...

class Model(Transformer):
    __metaclass__ = ...  # type: Any
