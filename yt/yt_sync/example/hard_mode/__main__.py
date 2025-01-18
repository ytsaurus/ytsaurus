from yt.yt_sync.core.constants import CONSUMER_ATTRS
from yt.yt_sync.core.constants import CONSUMER_SCHEMA
from yt.yt_sync.core.spec import ClusterTable
from yt.yt_sync.core.spec import Column
from yt.yt_sync.core.spec import Consumer
from yt.yt_sync.core.spec import QueueRegistration
from yt.yt_sync.core.spec import SchemaSpec
from yt.yt_sync.core.spec import Table
from yt.yt_sync.runner.core import Description
from yt.yt_sync.runner.core import run_yt_sync
from yt.yt_sync.runner.core import StageDescription


def _get_sorted_schema() -> list[Column]:
    return SchemaSpec.parse(
        [{"name": "key", "type": "int64", "sort_order": "ascending"}, {"name": "value", "type": "string"}]
    )


def _get_ordered_schema() -> list[Column]:
    return SchemaSpec.parse([{"name": "row_data", "type": "string"}])


def _get_stage_description(stage: str) -> StageDescription:
    return StageDescription(
        tables={
            "Sorted": Table(
                schema=_get_sorted_schema(),
                clusters={"primary": ClusterTable(path=f"//tmp/{stage}/sorted", attributes={"tablet_count": 10})},
            ),
            "Ordered": Table(
                schema=_get_ordered_schema(),
                clusters={"primary": ClusterTable(path=f"//tmp/{stage}/ordered", attributes={"tablet_count": 5})},
            ),
        },
        consumers={
            "Consumer": Consumer(
                table=Table(
                    schema=SchemaSpec.parse(CONSUMER_SCHEMA),
                    clusters={"primary": ClusterTable(path=f"//tmp/{stage}/consumer", attributes=CONSUMER_ATTRS)},
                ),
                queues=[QueueRegistration(cluster="primary", path=f"//tmp/{stage}/ordered")],
            )
        },
    )


def _get_description() -> Description:
    return Description(
        stages={
            "testing": _get_stage_description("testing"),
            "production": _get_stage_description("production"),
        }
    )


def main():
    run_yt_sync("example", _get_description())


if __name__ == "__main__":
    main()
