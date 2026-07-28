from yt.yt_sync.runner import StagesSpec, run_yt_sync_easy_mode

from .pipelines import PIPELINES
from .stages import STAGES
from .tables import TABLES
from .queues import CONSUMERS, PRODUCERS, QUEUES


def main():
    run_yt_sync_easy_mode(
        "wait_click_join",
        StagesSpec(
            stages=STAGES,
            tables={**TABLES, **QUEUES},
            consumers=CONSUMERS,
            producers=PRODUCERS,
            pipelines=PIPELINES,
        ),
    )


if __name__ == "__main__":
    main()
