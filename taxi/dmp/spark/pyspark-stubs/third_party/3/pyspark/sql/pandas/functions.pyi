from typing import overload
from typing import Any, Optional, Union, Callable

from pyspark.sql._typing import (
    AtomicDataTypeOrString,
    ColumnOrName,
    DataTypeOrString,
    UserDefinedFunctionLike,
)
from pyspark.sql.pandas._typing import (
    GroupedMapPandasUserDefinedFunction,
    MapIterPandasUserDefinedFunction,
    CogroupedMapPandasUserDefinedFunction,
    PandasCogroupedMapFunction,
    PandasCogroupedMapUDFType,
    PandasGroupedAggFunction,
    PandasGroupedAggUDFType,
    PandasGroupedMapFunction,
    PandasGroupedMapUDFType,
    PandasMapIterFunction,
    PandasMapIterUDFType,
    PandasScalarIterFunction,
    PandasScalarIterUDFType,
    PandasScalarToScalarFunction,
    PandasScalarToStructFunction,
    PandasScalarUDFType,
)

from pyspark import since as since
from pyspark.rdd import PythonEvalType as PythonEvalType
from pyspark.sql.column import Column
from pyspark.sql.types import ArrayType, DataType, StructType

class PandasUDFType:
    SCALAR: PandasScalarUDFType
    SCALAR_ITER: PandasScalarIterUDFType
    GROUPED_MAP: PandasGroupedMapUDFType
    GROUPED_AGG: PandasGroupedAggUDFType

@overload
def pandas_udf(
    f: PandasScalarToScalarFunction,
    returnType: Union[AtomicDataTypeOrString, ArrayType],
    functionType: PandasScalarUDFType,
) -> UserDefinedFunctionLike: ...
@overload
def pandas_udf(f: Union[AtomicDataTypeOrString, ArrayType], returnType: PandasScalarUDFType) -> Callable[[PandasScalarToScalarFunction], UserDefinedFunctionLike]: ...  # type: ignore[misc]
@overload
def pandas_udf(f: Union[AtomicDataTypeOrString, ArrayType], *, functionType: PandasScalarUDFType) -> Callable[[PandasScalarToScalarFunction], UserDefinedFunctionLike]: ...  # type: ignore[misc]
@overload
def pandas_udf(*, returnType: Union[AtomicDataTypeOrString, ArrayType], functionType: PandasScalarUDFType) -> Callable[[PandasScalarToScalarFunction], UserDefinedFunctionLike]: ...  # type: ignore[misc]
@overload
def pandas_udf(
    f: PandasScalarToStructFunction,
    returnType: Union[StructType, str],
    functionType: PandasScalarUDFType,
) -> UserDefinedFunctionLike: ...
@overload
def pandas_udf(f: Union[StructType, str], returnType: PandasScalarUDFType) -> Callable[[PandasScalarToStructFunction], UserDefinedFunctionLike]: ...  # type: ignore[misc]
@overload
def pandas_udf(f: Union[StructType, str], *, functionType: PandasScalarUDFType) -> Callable[[PandasScalarToStructFunction], UserDefinedFunctionLike]: ...  # type: ignore[misc]
@overload
def pandas_udf(*, returnType: Union[StructType, str], functionType: PandasScalarUDFType) -> Callable[[PandasScalarToStructFunction], UserDefinedFunctionLike]: ...  # type: ignore[misc]
@overload
def pandas_udf(
    f: PandasScalarIterFunction,
    returnType: Union[AtomicDataTypeOrString, ArrayType],
    functionType: PandasScalarIterUDFType,
) -> UserDefinedFunctionLike: ...
@overload
def pandas_udf(
    f: Union[AtomicDataTypeOrString, ArrayType], returnType: PandasScalarIterUDFType
) -> Callable[[PandasScalarIterFunction], UserDefinedFunctionLike]: ...
@overload
def pandas_udf(
    *,
    returnType: Union[AtomicDataTypeOrString, ArrayType],
    functionType: PandasScalarIterUDFType
) -> Callable[[PandasScalarIterFunction], UserDefinedFunctionLike]: ...
@overload
def pandas_udf(
    f: Union[AtomicDataTypeOrString, ArrayType],
    *,
    functionType: PandasScalarIterUDFType
) -> Callable[[PandasScalarIterFunction], UserDefinedFunctionLike]: ...
@overload
def pandas_udf(
    f: PandasGroupedMapFunction,
    returnType: Union[StructType, str],
    functionType: PandasGroupedMapUDFType,
) -> GroupedMapPandasUserDefinedFunction: ...
@overload
def pandas_udf(
    f: Union[StructType, str], returnType: PandasGroupedMapUDFType
) -> Callable[[PandasGroupedMapFunction], GroupedMapPandasUserDefinedFunction]: ...
@overload
def pandas_udf(
    *, returnType: Union[StructType, str], functionType: PandasGroupedMapUDFType
) -> Callable[[PandasGroupedMapFunction], GroupedMapPandasUserDefinedFunction]: ...
@overload
def pandas_udf(
    f: Union[StructType, str], *, functionType: PandasGroupedMapUDFType
) -> Callable[[PandasGroupedMapFunction], GroupedMapPandasUserDefinedFunction]: ...
@overload
def pandas_udf(
    f: PandasGroupedAggFunction,
    returnType: Union[AtomicDataTypeOrString, ArrayType],
    functionType: PandasGroupedAggUDFType,
) -> UserDefinedFunctionLike: ...
@overload
def pandas_udf(
    f: Union[AtomicDataTypeOrString, ArrayType], returnType: PandasGroupedAggUDFType
) -> Callable[[PandasGroupedAggFunction], UserDefinedFunctionLike]: ...
@overload
def pandas_udf(
    *,
    returnType: Union[AtomicDataTypeOrString, ArrayType],
    functionType: PandasGroupedAggUDFType
) -> Callable[[PandasGroupedAggFunction], UserDefinedFunctionLike]: ...
@overload
def pandas_udf(
    f: Union[AtomicDataTypeOrString, ArrayType],
    *,
    functionType: PandasGroupedAggUDFType
) -> Callable[[PandasGroupedAggFunction], UserDefinedFunctionLike]: ...
@overload
def pandas_udf(
    f: PandasMapIterFunction,
    returnType: Union[StructType, str],
    functionType: PandasMapIterUDFType,
) -> MapIterPandasUserDefinedFunction: ...
@overload
def pandas_udf(
    f: Union[StructType, str], returnType: PandasMapIterUDFType
) -> Callable[[PandasMapIterFunction], MapIterPandasUserDefinedFunction]: ...
@overload
def pandas_udf(
    *, returnType: Union[StructType, str], functionType: PandasMapIterUDFType
) -> Callable[[PandasMapIterFunction], MapIterPandasUserDefinedFunction]: ...
@overload
def pandas_udf(
    f: Union[StructType, str], *, functionType: PandasMapIterUDFType
) -> Callable[[PandasMapIterFunction], MapIterPandasUserDefinedFunction]: ...
@overload
def pandas_udf(
    f: PandasCogroupedMapFunction,
    returnType: Union[StructType, str],
    functionType: PandasCogroupedMapUDFType,
) -> CogroupedMapPandasUserDefinedFunction: ...
@overload
def pandas_udf(
    f: Union[StructType, str], returnType: PandasCogroupedMapUDFType
) -> Callable[[PandasCogroupedMapFunction], CogroupedMapPandasUserDefinedFunction]: ...
@overload
def pandas_udf(
    *, returnType: Union[StructType, str], functionType: PandasCogroupedMapUDFType
) -> Callable[[PandasCogroupedMapFunction], CogroupedMapPandasUserDefinedFunction]: ...
@overload
def pandas_udf(
    f: Union[StructType, str], *, functionType: PandasCogroupedMapUDFType
) -> Callable[[PandasCogroupedMapFunction], CogroupedMapPandasUserDefinedFunction]: ...
