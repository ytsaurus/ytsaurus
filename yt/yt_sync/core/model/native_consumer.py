from dataclasses import dataclass
from dataclasses import field as dataclass_field

from yt.yson import yson_types

from .table import YtTable
from .types import Types


def _normalize_partitions(partitions: list[int] | yson_types.YsonEntity | None) -> list[int] | None:
    if partitions is None or isinstance(partitions, yson_types.YsonEntity):
        return None
    return list(sorted(set(partitions)))


@dataclass
class YtNativeConsumerRegistration:
    cluster_name: str
    path: str
    vital: bool
    partitions: list[int] | None

    @property
    def rich_path(self) -> str:
        return f"{self.cluster_name}:{self.path}"


YtNativeConsumerRegistrationMap = dict[Types.ReplicaKey, YtNativeConsumerRegistration]


@dataclass
class YtNativeConsumer:
    table: YtTable
    registrations: YtNativeConsumerRegistrationMap = dataclass_field(default_factory=dict)

    def add_registration(
        self, cluster_name: str, path: str, vital: bool, partitions: list[int] | yson_types.YsonEntity | None
    ):
        self.registrations[(cluster_name, path)] = YtNativeConsumerRegistration(
            cluster_name, path, vital, _normalize_partitions(partitions)
        )

    @classmethod
    def format_consumer_path(cls, cluster_name: str, path: str) -> str:
        return f'<cluster="{cluster_name}">{path}'

    @property
    def full_path(self):
        return self.format_consumer_path(self.table.cluster_name, self.table.path)
