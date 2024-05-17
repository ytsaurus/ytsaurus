"""SPYT extensions for pyspark module.

Usage notes: spyt module must be imported before any of pyspark.* modules in order
for extensions to take an effect.
"""

from .client import connect, spark_session, connect_direct, direct_spark_session, \
    info, stop, jvm_process_pid, yt_client, is_stopped
from .extensions import read_yt, schema_hint, write_yt, sorted_by, optimize_for, withYsonColumn, transform, \
    _extract_code_globals, _code_reduce
from spyt.types import UInt64Type
import pyspark.context
import pyspark.sql.types
import pyspark.sql.readwriter
import pyspark.cloudpickle.cloudpickle
import pyspark.cloudpickle.cloudpickle_fast
from types import CodeType

__all__ = [
    'connect',
    'connect_direct',
    'spark_session',
    'direct_spark_session',
    'info',
    'stop',
    'jvm_process_pid',
    'yt_client',
    'is_stopped'
]


def initialize():
    pyspark.sql.types._atomic_types.append(UInt64Type)
    # exact copy of the corresponding line in pyspark/sql/types.py
    pyspark.sql.types._all_atomic_types = dict((t.typeName(), t) for t in pyspark.sql.types._atomic_types)
    pyspark.sql.types._acceptable_types.update({UInt64Type: (int,)})

    pyspark.sql.readwriter.DataFrameReader.yt = read_yt
    pyspark.sql.readwriter.DataFrameReader.schema_hint = schema_hint

    pyspark.sql.readwriter.DataFrameWriter.yt = write_yt
    pyspark.sql.readwriter.DataFrameWriter.sorted_by = sorted_by
    pyspark.sql.readwriter.DataFrameWriter.optimize_for = optimize_for

    pyspark.sql.dataframe.DataFrame.withYsonColumn = withYsonColumn
    pyspark.sql.dataframe.DataFrame.transform = transform

    pyspark.context.SparkContext.PACKAGE_EXTENSIONS += ('.whl',)

    pyspark.cloudpickle.cloudpickle._extract_code_globals = _extract_code_globals
    pyspark.cloudpickle.cloudpickle_fast._extract_code_globals = _extract_code_globals
    pyspark.cloudpickle.cloudpickle_fast._code_reduce = _code_reduce
    pyspark.cloudpickle.cloudpickle_fast.CloudPickler.dispatch_table[CodeType] = _code_reduce


initialize()
