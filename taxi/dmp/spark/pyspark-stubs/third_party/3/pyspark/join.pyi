# Stubs for pyspark.join (Python 3)
#

from typing import Hashable, Iterable, List, Optional, Tuple, TypeVar

from pyspark.resultiterable import ResultIterable
import pyspark.rdd

K = TypeVar("K", bound=Hashable)
V = TypeVar("V")
U = TypeVar("U")

def python_join(
    rdd: pyspark.rdd.RDD[Tuple[K, V]],
    other: pyspark.rdd.RDD[Tuple[K, U]],
    numPartitions: int,
) -> pyspark.rdd.RDD[Tuple[K, Tuple[V, U]]]: ...
def python_right_outer_join(
    rdd: pyspark.rdd.RDD[Tuple[K, V]],
    other: pyspark.rdd.RDD[Tuple[K, U]],
    numPartitions: int,
) -> pyspark.rdd.RDD[Tuple[K, Tuple[V, Optional[U]]]]: ...
def python_left_outer_join(
    rdd: pyspark.rdd.RDD[Tuple[K, V]],
    other: pyspark.rdd.RDD[Tuple[K, U]],
    numPartitions: int,
) -> pyspark.rdd.RDD[Tuple[K, Tuple[Optional[V], U]]]: ...
def python_full_outer_join(
    rdd: pyspark.rdd.RDD[Tuple[K, V]],
    other: pyspark.rdd.RDD[Tuple[K, U]],
    numPartitions: int,
) -> pyspark.rdd.RDD[Tuple[K, Tuple[Optional[V], Optional[U]]]]: ...
def python_cogroup(
    rdds: Iterable[pyspark.rdd.RDD[Tuple[K, V]]], numPartitions: int
) -> pyspark.rdd.RDD[Tuple[K, Tuple[ResultIterable[V], ...]]]: ...
