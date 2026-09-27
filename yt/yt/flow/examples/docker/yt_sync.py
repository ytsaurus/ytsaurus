# Creates the Cypress objects for the docker example: the pipeline node and its
# system tables. The noop pipeline reads a random in-memory source, so it needs
# no queues or consumers.
#
# Runs in the yt-sync service of docker-compose.yml, or by hand:
#   YT_PROXY=<your-http-proxy> python3 yt_sync.py
# Requires the pip-installed ytsaurus-flow-yt-sync-mini package. The token comes
# from YT_TOKEN. The ensure flow is idempotent.
#
# FOLDER must match the parent of `path` in the YSON configs.

import os

from yt.yt.flow.library.python.yt_sync_mini import StagesSpec, run_yt_sync_easy_mode

CLUSTER = os.environ["YT_PROXY"]
FOLDER = os.environ.get("YT_FLOW_FOLDER", "//tmp/flow/noop")


def main():
    stages = {
        "default": {},
        "example": {
            "folder": FOLDER,
            "presets": {
                "builtin:storage_preset": {"clusters": {CLUSTER: {"attributes": {"primary_medium": "default"}}}},
                "builtin:table_preset": {"clusters": {CLUSTER: {"attributes": {"tablet_cell_bundle": "default"}}}},
            },
        },
    }

    pipelines = {
        "pipeline": {
            "default": {
                "$merge_presets": ["builtin:pipeline_preset"],
                "monitoring_project": "",
                "monitoring_cluster": "",
            },
        },
    }

    run_yt_sync_easy_mode(
        "docker_example",
        StagesSpec(stages=stages, pipelines=pipelines),
        args=["--stage", "example", "--scenario", "ensure", "--parallel-factor", "0", "--commit"],
    )


if __name__ == "__main__":
    main()
