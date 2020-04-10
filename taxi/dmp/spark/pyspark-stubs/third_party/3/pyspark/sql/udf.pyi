# Stubs for pyspark.sql.udf (Python 3)
#

from typing import Any, Callable, Optional

from pyspark.sql._typing import ColumnOrName, DataTypeOrString
from pyspark.sql.column import Column
import pyspark.sql.session

class UserDefinedFunction:
    func: Callable[..., Any]
    evalType: int
    deterministic: bool
    def __init__(
        self,
        func: Callable[..., Any],
        returnType: DataTypeOrString = ...,
        name: Optional[str] = ...,
        evalType: int = ...,
        deterministic: bool = ...,
    ) -> None: ...
    @property
    def returnType(self): ...
    def __call__(self, *cols: ColumnOrName) -> Column: ...
    def asNondeterministic(self) -> UserDefinedFunction: ...

class UDFRegistration:
    sparkSession: pyspark.sql.session.SparkSession
    def __init__(self, sparkSession: pyspark.sql.session.SparkSession) -> None: ...
    def register(
        self,
        name: str,
        f: Callable[..., Any],
        returnType: Optional[DataTypeOrString] = ...,
    ): ...
    def registerJavaFunction(
        self,
        name: str,
        javaClassName: str,
        returnType: Optional[DataTypeOrString] = ...,
    ) -> None: ...
    def registerJavaUDAF(self, name: str, javaClassName: str) -> None: ...
