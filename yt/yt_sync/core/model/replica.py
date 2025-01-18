from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from enum import StrEnum
from typing import Any
from typing import Generator

from .helpers import get_rtt_enabled_from_attrs
from .helpers import iter_attributes_recursively
from .types import Types


@dataclass
class YtReplica:
    class Mode(StrEnum):
        ASYNC = "async"
        SYNC = "sync"

        @classmethod
        def all(cls) -> list[str]:
            return [v.value for v in cls]

    class ContentType(StrEnum):
        DATA = "data"
        QUEUE = "queue"

        @classmethod
        def all(cls) -> list[str]:
            return [v.value for v in cls]

    class ReplicaType(StrEnum):
        TABLE_REPLICA = "table_replica"
        CHAOS_TABLE_REPLICA = "chaos_table_replica"

        @classmethod
        def all(cls) -> list[str]:
            return [v.value for v in cls]

    class State(StrEnum):
        ENABLED = "enabled"
        DISABLED = "disabled"

        @classmethod
        def all(cls) -> list[str]:
            return [v.value for v in cls]

    replica_id: str | None
    cluster_name: str
    replica_path: str
    enabled: bool
    mode: Mode
    enable_replicated_table_tracker: bool = True
    content_type: str | None = None
    attributes: Types.Attributes = dataclass_field(default_factory=dict)
    exists: bool = True
    is_temporary: bool = False

    def __eq__(self, other: YtReplica) -> bool:
        return self.compare(other, with_mode=True)

    def compare(self, other: YtReplica, with_mode: bool) -> bool:
        if not isinstance(other, YtReplica):
            raise TypeError()
        mode_equals = self.mode == other.mode
        if self.ContentType.QUEUE == self.content_type and self.enable_replicated_table_tracker:
            # RTT forces chaos queue to be sync
            mode_equals = True
        if not with_mode:
            mode_equals = True
        return (
            self.cluster_name == other.cluster_name
            and self.replica_path == other.replica_path
            and self.enabled == other.enabled
            and mode_equals
            and self.enable_replicated_table_tracker == other.enable_replicated_table_tracker
            and self.content_type == other.content_type
        )

    @property
    def key(self) -> Types.ReplicaKey:
        return self.make_key(self.cluster_name, self.replica_path)

    @staticmethod
    def make_key(cluster_name: str, table_path: str) -> Types.ReplicaKey:
        return (cluster_name, table_path)

    @property
    def rich_path(self) -> str:
        return f"{self.cluster_name}:{self.replica_path}"

    @property
    def yt_attributes(self) -> Types.Attributes:
        result = deepcopy(self.attributes)
        result["cluster_name"] = self.cluster_name
        result["replica_path"] = self.replica_path
        result["mode"] = str(self.mode)
        result["enable_replicated_table_tracker"] = self.enable_replicated_table_tracker
        if self.enabled:
            result["enabled"] = True
        if self.content_type:
            result["content_type"] = self.content_type
        return result

    def changed_attributes(self, other: YtReplica) -> Generator[tuple[str, Any | None, Any | None], None, None]:
        for path, desired, actual in iter_attributes_recursively(self.yt_attributes, other.yt_attributes):
            if desired != actual:
                yield (path, desired, actual)

    @classmethod
    def make(cls, replica_id: str, attributes: Types.Attributes) -> YtReplica:
        attributes_copy = deepcopy(attributes)
        assert "cluster_name" in attributes_copy, "Missing mandatory attribute 'cluster_name'"
        assert "replica_path" in attributes_copy, "Missing mandatory attribute 'replica_path'"
        assert "state" in attributes_copy, "Missing mandatory attribute 'state'"
        assert "mode" in attributes_copy, "Missing mandatory attribute 'mode'"
        replica = cls(
            replica_id=replica_id,
            cluster_name=attributes_copy.pop("cluster_name"),
            replica_path=attributes_copy.pop("replica_path"),
            enabled=True,
            mode=cls.Mode.ASYNC,
        )
        replica.update_from_attributes(attributes_copy)
        return replica

    def update_from_attributes(self, attributes: Types.Attributes):
        state = attributes.pop("state", None)
        enabled = attributes.pop("enabled", None)
        assert not (
            state is not None and enabled is not None
        ), f"State should be set by 'state' or 'enabled' attribute, not both for replica {self.rich_path}"
        if state is not None:
            assert state in self.State.all(), f"Unknown state='{state}' for replica {self.rich_path}"

            self.enabled = self.State.ENABLED == state
        elif enabled is not None:
            self.enabled = enabled

        mode = attributes.pop("mode", None)
        if mode:
            assert mode in self.Mode.all(), f"Unknown mode='{mode}' for replica {self.rich_path}"
            self.mode = self.Mode(mode)
        rtt_enabled = get_rtt_enabled_from_attrs(attributes)
        if rtt_enabled is not None:
            self.enable_replicated_table_tracker = rtt_enabled
        content_type = attributes.pop("content_type", None)
        if content_type:
            assert (
                content_type in self.ContentType.all()
            ), f"Unknown content_type '{content_type}' for replica {self.rich_path}"
            self.content_type = content_type
        for key, value in attributes.items():
            if value is None:
                self.attributes.pop(key, None)
            else:
                self.attributes[key] = value
