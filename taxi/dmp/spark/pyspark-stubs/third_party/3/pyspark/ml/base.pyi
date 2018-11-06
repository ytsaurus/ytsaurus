# Stubs for pyspark.ml.base (Python 3.5)
#

import abc
from typing import overload
from typing import Any, Callable, Dict, Generic, Iterable, List, Optional, Tuple, Type, TypeVar

import _thread

from pyspark.ml.param import Params, Param
from pyspark.ml.param.shared import HasInputCol, HasOutputCol
from pyspark.sql.column import Column
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import DataType, StructType

M = TypeVar("M", bound=Transformer)
ParamMap = Dict[Param, Any]

class _FitMultipleIterator:
    fitSingleModel = ...  # type: Callable[[int], Transformer]
    numModel = ...  # type: int
    counter: int = ...
    lock = ...  # type: _thread.LockType
    def __init__(self, fitSingleModel: Callable[[int], Transformer], numModels: int) -> None: ...
    def __iter__(self) -> _FitMultipleIterator: ...
    def __next__(self) -> Tuple[int, Transformer]: ...
    def next(self) -> Tuple[int, Transformer]: ...

class Estimator(Params, Generic[M]):
    __metaclass__ = ...  # type: Type[abc.ABCMeta]
    @overload
    def fit(self, dataset: DataFrame, params: Optional[ParamMap] = ...) -> M: ...
    @overload
    def fit(self, dataset: DataFrame, params: List[ParamMap]) -> List[M]: ...
    def fitMultiple(self, dataset: DataFrame, params: List[ParamMap]) -> Iterable[Tuple[int, M]]: ...

class Transformer(Params):
    __metaclass__ = ...  # type: Type[abc.ABCMeta]
    def transform(self, dataset: DataFrame, params: Optional[ParamMap] = ...) -> DataFrame: ...

class Model(Transformer):
    __metaclass__ = ...  # type: Type[abc.ABCMeta]

class UnaryTransformer(HasInputCol, HasOutputCol, Transformer):
    def createTransformFunc(self) -> Callable: ...
    def outputDataType(self) -> DataType: ...
    def validateInputType(self, inputType: DataType) -> None: ...
    def transformSchema(self, schema: StructType) -> StructType: ...
