# Prepares YT cluster resources required to run YTFlow.

import sys

from yt.yt_sync.runner import StagesSpec, run_yt_sync_easy_mode

from .pipelines import PIPELINES
from .stages import STAGES


def main():
    run_yt_sync_easy_mode(
        # "noop" is a human-readable label used only for CLI help text.
        # This is NOT the pipeline entity name.
        # Actual pipeline names are defined as keys in PIPELINES dict.
        "noop",
        StagesSpec(
            stages=STAGES,
            pipelines=PIPELINES,
        ),
        args=sys.argv[1:],
    )


if __name__ == "__main__":
    main()
