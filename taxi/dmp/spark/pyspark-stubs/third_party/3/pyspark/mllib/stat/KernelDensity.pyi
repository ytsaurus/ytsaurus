# Stubs for pyspark.mllib.stat.KernelDensity (Python 3.5)
#

from typing import Any, Iterable
from pyspark.rdd import RDD
from numpy import ndarray  # type: ignore

xrange = ...  # type: range

class KernelDensity:
    def __init__(self) -> None: ...
    def setBandwidth(self, bandwidth: float) -> None: ...
    def setSample(self, sample: RDD[float]) -> None: ...
    def estimate(self, points: Iterable[float]) -> ndarray: ...
