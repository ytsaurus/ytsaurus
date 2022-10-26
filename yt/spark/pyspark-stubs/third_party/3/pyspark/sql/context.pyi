# Stubs for pyspark.sql.context (Python 3.5)
#

from typing import overload
from typing import Any, Callable, Iterable, List, Optional, Tuple, TypeVar, Union

from py4j.java_gateway import JavaObject  # type: ignore

from pyspark.sql._typing import (
    DateTimeLiteral,
    LiteralType,
    DecimalLiteral,
    DataTypeOrString,
    RowLike,
)
from pyspark.sql.pandas._typing import DataFrameLike
from pyspark.context import SparkContext
from pyspark.rdd import RDD
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession
from pyspark.sql.types import AtomicType, DataType, StructType
from pyspark.sql.udf import UDFRegistration as UDFRegistration
from pyspark.sql.readwriter import DataFrameReader
from pyspark.sql.streaming import DataStreamReader, StreamingQueryManager

T = TypeVar("T")

class SQLContext:
    sparkSession: SparkSession
    def __init__(
        self,
        sparkContext,
        sparkSession: Optional[SparkSession] = ...,
        jsqlContext: Optional[JavaObject] = ...,
    ) -> None: ...
    @classmethod
    def getOrCreate(cls: type, sc: SparkContext) -> SQLContext: ...
    def newSession(self) -> SQLContext: ...
    def setConf(self, key: str, value) -> None: ...
    def getConf(self, key: str, defaultValue: Optional[str] = ...) -> str: ...
    @property
    def udf(self) -> UDFRegistration: ...
    def range(
        self,
        start: int,
        end: Optional[int] = ...,
        step: int = ...,
        numPartitions: Optional[int] = ...,
    ) -> DataFrame: ...
    def registerFunction(
        self, name: str, f: Callable[..., Any], returnType: DataType = ...
    ) -> None: ...
    def registerJavaFunction(
        self, name: str, javaClassName: str, returnType: Optional[DataType] = ...
    ) -> None: ...
    @overload
    def createDataFrame(
        self,
        data: Union[RDD[RowLike], Iterable[RowLike]],
        samplingRatio: Optional[float] = ...,
    ) -> DataFrame: ...
    @overload
    def createDataFrame(
        self,
        data: Union[RDD[RowLike], Iterable[RowLike]],
        schema: Union[List[str], Tuple[str, ...]] = ...,
        verifySchema: bool = ...,
    ) -> DataFrame: ...
    @overload
    def createDataFrame(
        self,
        data: Union[
            RDD[Union[DateTimeLiteral, LiteralType, DecimalLiteral]],
            Iterable[Union[DateTimeLiteral, LiteralType, DecimalLiteral]],
        ],
        schema: Union[AtomicType, str],
        verifySchema: bool = ...,
    ) -> DataFrame: ...
    @overload
    def createDataFrame(
        self,
        data: Union[RDD[RowLike], Iterable[RowLike]],
        schema: Union[StructType, str],
        verifySchema: bool = ...,
    ) -> DataFrame: ...
    @overload
    def createDataFrame(
        self, data: DataFrameLike, samplingRatio: Optional[float] = ...
    ) -> DataFrame: ...
    @overload
    def createDataFrame(
        self,
        data: DataFrameLike,
        schema: Union[StructType, str],
        verifySchema: bool = ...,
    ) -> DataFrame: ...
    def registerDataFrameAsTable(self, df: DataFrame, tableName: str) -> None: ...
    def dropTempTable(self, tableName: str) -> None: ...
    def sql(self, sqlQuery: str) -> DataFrame: ...
    def table(self, tableName: str) -> DataFrame: ...
    def tables(self, dbName: Optional[str] = ...) -> DataFrame: ...
    def tableNames(self, dbName: Optional[str] = ...) -> List[str]: ...
    def cacheTable(self, tableName: str) -> None: ...
    def uncacheTable(self, tableName: str) -> None: ...
    def clearCache(self) -> None: ...
    @property
    def read(self) -> DataFrameReader: ...
    @property
    def readStream(self) -> DataStreamReader: ...
    @property
    def streams(self) -> StreamingQueryManager: ...
