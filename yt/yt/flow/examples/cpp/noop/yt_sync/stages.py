import enum
import os


class Stage(str, enum.Enum):
    TEST = "test"


TEST_CLUSTER = os.environ.get("TEST_CLUSTER")
TEST_FOLDER = os.environ.get("TEST_YT_PATH")


# Stages describes deployment environments the pipeline can run in.
#
# "default" is the base stage inherited by all others.
# "test" targets YT cluster used for test purposes:
#   "folder" is the root Cypress directory where yt_sync places all pipeline objects.
#   "presets" are parameter sets injected into configs of created objects;
#   here we override the cluster address to point at the given YT instance.
STAGES = {
    "default": {},
    Stage.TEST: {
        "folder": TEST_FOLDER,
        "presets": {
            "builtin:storage_preset": {"clusters": {TEST_CLUSTER: {"attributes": {"primary_medium": "default"}}}},
            "builtin:table_preset": {"clusters": {TEST_CLUSTER: {"attributes": {"tablet_cell_bundle": "default"}}}},
        },
    },
}
