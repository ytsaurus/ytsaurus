# Stubs for pyspark.mllib.util (Python 3)
#

from typing import Generic, List, Optional, Type, TypeVar

from pyspark.mllib._typing import VectorLike
from pyspark.context import SparkContext
from pyspark.mllib.linalg import Vector
from pyspark.mllib.regression import LabeledPoint
from pyspark.rdd import RDD
from pyspark.sql.dataframe import DataFrame

T = TypeVar("T")

xrange: Type[range]
basestring: Type[str]

class MLUtils:
    @staticmethod
    def loadLibSVMFile(
        sc: SparkContext,
        path: str,
        numFeatures: int = ...,
        minPartitions: Optional[int] = ...,
    ) -> RDD[LabeledPoint]: ...
    @staticmethod
    def saveAsLibSVMFile(data: RDD[LabeledPoint], dir: str) -> None: ...
    @staticmethod
    def loadLabeledPoints(
        sc: SparkContext, path: str, minPartitions: Optional[int] = ...
    ) -> RDD[LabeledPoint]: ...
    @staticmethod
    def appendBias(data: Vector) -> Vector: ...
    @staticmethod
    def loadVectors(sc: SparkContext, path: str) -> RDD[Vector]: ...
    @staticmethod
    def convertVectorColumnsToML(dataset: DataFrame, *cols: str) -> DataFrame: ...
    @staticmethod
    def convertVectorColumnsFromML(dataset: DataFrame, *cols: str) -> DataFrame: ...
    @staticmethod
    def convertMatrixColumnsToML(dataset: DataFrame, *cols: str) -> DataFrame: ...
    @staticmethod
    def convertMatrixColumnsFromML(dataset: DataFrame, *cols: str) -> DataFrame: ...

class Saveable:
    def save(self, sc: SparkContext, path: str) -> None: ...

class JavaSaveable(Saveable):
    def save(self, sc: SparkContext, path: str) -> None: ...

class Loader(Generic[T]):
    @classmethod
    def load(cls, sc: SparkContext, path: str) -> T: ...

class JavaLoader(Loader[T]):
    @classmethod
    def load(cls, sc: SparkContext, path: str) -> T: ...

class LinearDataGenerator:
    @staticmethod
    def generateLinearInput(
        intercept: float,
        weights: VectorLike,
        xMean: VectorLike,
        xVariance: VectorLike,
        nPoints: int,
        seed: int,
        eps: float,
    ) -> List[LabeledPoint]: ...
    @staticmethod
    def generateLinearRDD(
        sc: SparkContext,
        nexamples: int,
        nfeatures: int,
        eps: float,
        nParts: int = ...,
        intercept: float = ...,
    ) -> RDD[LabeledPoint]: ...
