# Stubs for pyspark.storagelevel (Python 3.5)
#

from typing import Any

class StorageLevel:
    useDisk: bool
    useMemory: bool
    useOffHeap: bool
    deserialized: bool
    replication: int
    def __init__(
        self,
        useDisk: bool,
        useMemory: bool,
        useOffHeap: bool,
        deserialized: bool,
        replication: int = ...,
    ) -> None: ...
