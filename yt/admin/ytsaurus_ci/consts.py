import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
COMPONENTS_PATH = "configs/components.yaml"
SNAPSHOTS_PATH = "snapshots"
CLOUD_FUNCTION_TOKEN_PATHS = [
    os.path.expanduser("~/.yt/ytsaurus_ci_token"),
    os.path.expanduser("~/.yc/cf_token"),
]
