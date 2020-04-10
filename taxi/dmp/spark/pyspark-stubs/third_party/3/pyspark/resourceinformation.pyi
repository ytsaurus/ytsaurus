# Stubs for pyspark.resourceinformation (Python 3)
#

from typing import List

class ResourceInformation:
    def __init__(self, name: str, addresses: List[str]) -> None: ...
    @property
    def name(self) -> str: ...
    @property
    def addresses(self) -> List[str]: ...
