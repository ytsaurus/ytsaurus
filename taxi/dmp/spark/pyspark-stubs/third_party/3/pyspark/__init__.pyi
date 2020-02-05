# Stubs for pyspark (Python 3)
#

from typing import Callable, TypeVar

from pyspark.status import *
from pyspark.accumulators import (
    Accumulator as Accumulator,
    AccumulatorParam as AccumulatorParam,
)
from pyspark.broadcast import Broadcast as Broadcast
from pyspark.conf import SparkConf as SparkConf
from pyspark.context import SparkContext as SparkContext
from pyspark.files import SparkFiles as SparkFiles
from pyspark.profiler import BasicProfiler as BasicProfiler, Profiler as Profiler
from pyspark.rdd import RDD as RDD, RDDBarrier as RDDBarrier
from pyspark.resourceinformation import ResourceInformation as ResourceInformation
from pyspark.serializers import (
    MarshalSerializer as MarshalSerializer,
    PickleSerializer as PickleSerializer,
)
from pyspark.storagelevel import StorageLevel as StorageLevel
from pyspark.taskcontext import (
    BarrierTaskContext as BarrierTaskContext,
    BarrierTaskInfo as BarrierTaskInfo,
    TaskContext as TaskContext,
)

# Compatiblity imports
from pyspark.sql import SQLContext, Row

T = TypeVar("T")

def since(version: str) -> Callable[[T], T]: ...

# Names in __all__ with no definition:
#   SparkJobInfo
#   SparkStageInfo
#   StatusTracker
