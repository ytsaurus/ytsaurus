# Stubs for pyspark.mllib.fpm (Python 3.5)
#

from typing import Any, Optional, Generic, List, TypeVar
from pyspark.context import SparkContext
from pyspark.rdd import RDD
from pyspark.mllib.common import JavaModelWrapper
from pyspark.mllib.util import JavaSaveable, JavaLoader

T = TypeVar('T')

class FPGrowthModel(JavaModelWrapper, JavaSaveable, JavaLoader, Generic[T]):
    def freqItemsets(self) -> RDD[FPGrowth.FreqItemset[T]]: ...
    @classmethod
    def load(cls, sc: SparkContext, path: str) -> 'FPGrowthModel': ...

class FPGrowth:
    @classmethod
    def train(cls, data: RDD[List[T]], minSupport: float = ..., numPartitions: int = ...) -> FPGrowthModel[T]: ...
    class FreqItemset(Generic[T]):
        items = ...  # List[T]
        freq = ...  # int

class PrefixSpanModel(JavaModelWrapper, Generic[T]):
    def freqSequences(self) -> RDD[PrefixSpan.FreqSequence[T]]: ...

class PrefixSpan:
    @classmethod
    def train(cls, data: RDD[List[List[T]]], minSupport: float = ..., maxPatternLength: int = ..., maxLocalProjDBSize: int = ...) -> PrefixSpanModel[T]: ...
    class FreqSequence(tuple, Generic[T]):
        sequence = ...  # type: List[T]
        freq = ...  # type: int
