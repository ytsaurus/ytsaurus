# Stubs for pyspark.ml.wrapper (Python 3.7)
#

import abc
from typing import Any, Optional, Type, TypeVar

from pyspark.ml import Estimator, Model, Transformer
from pyspark.ml.param import Params

JM = TypeVar("JM", bound=JavaTransformer)

xrange = range

class JavaWrapper:
    def __init__(self, java_obj: Optional[Any] = ...) -> None: ...
    def __del__(self) -> None: ...

class JavaParams(JavaWrapper, Params):
    __metaclass__: Type[abc.ABCMeta]
    def copy(self, extra: Optional[Any] = ...): ...

class JavaEstimator(JavaParams, Estimator[JM]):
    __metaclass__: Type[abc.ABCMeta]

class JavaTransformer(JavaParams, Transformer):
    __metaclass__: Type[abc.ABCMeta]

class JavaModel(JavaTransformer, Model):
    __metaclass__: Type[abc.ABCMeta] = ...
    def __init__(self, java_model: Optional[Any] = ...) -> None: ...
