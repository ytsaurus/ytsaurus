"""SPYT extensions for pyspark module.

Usage notes: spyt module must be imported before any of pyspark.* modules in order
for extensions to take an effect.
"""

from .client import connect, spark_session, info, stop, jvm_process_pid, yt_client, is_stopped
from spyt.types import UInt64Type
import pyspark.sql.types

__all__ = [
    'connect',
    'spark_session',
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


initialize()
