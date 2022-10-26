from typing import Any, Iterable, List, Optional, Tuple, TypeVar, Union
from pyspark.mllib.linalg import Vector
from numpy import ndarray  # type: ignore

VectorLike = Union[Vector, List[float], Tuple[float, ...]]
