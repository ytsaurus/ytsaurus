from typing import Any
from typing import TypeAlias


class Types:
    Schema: TypeAlias = list[dict[str, str | bool | int]]
    Attributes: TypeAlias = dict[str, Any]
    ReplicaKey: TypeAlias = tuple[str, str]
