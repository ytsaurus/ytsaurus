# Stubs for pyspark.storagelevel (Python 3.5)
#

from typing import Any

class StorageLevel:
    useDisk = ...  # type: bool
    useMemory = ...  # type: bool
    useOffHeap = ...  # type: bool
    deserialized = ...  # type: bool
    replication = ...  # type: int
    def __init__(self, useDisk: bool, useMemory: bool, useOffHeap: bool, deserialized: bool, replication: int = ...) -> None: ...
