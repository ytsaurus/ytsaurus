from yt.yt_sync.runner import StagesSpec, run_yt_sync_easy_mode

from .queues import QUEUES, CONSUMERS
from .stages import STAGES
from .tables import TABLES


def main():
    run_yt_sync_easy_mode(
        "example",
        StagesSpec(
            stages=STAGES,
            tables={**TABLES, **QUEUES},
            consumers=CONSUMERS,
            # These attributes are not required, but are kept as a reminder of the possibilities.
            producers={},
            nodes={},
            pipelines={},
        ),
    )


if __name__ == "__main__":
    main()
