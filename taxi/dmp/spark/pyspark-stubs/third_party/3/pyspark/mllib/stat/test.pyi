# Stubs for pyspark.mllib.stat.test (Python 3.5)
#

from typing import Generic, Tuple, TypeVar

from pyspark.mllib.common import JavaModelWrapper

DF = TypeVar("DF", int, float, Tuple[int, ...], Tuple[float, ...])

class TestResult(JavaModelWrapper, Generic[DF]):
    @property
    def pValue(self) -> float: ...
    @property
    def degreesOfFreedom(self) -> DF: ...
    @property
    def statistic(self) -> float: ...
    @property
    def nullHypothesis(self) -> str: ...

class ChiSqTestResult(TestResult[int]):
    @property
    def method(self) -> str: ...

class KolmogorovSmirnovTestResult(TestResult[int]): ...
