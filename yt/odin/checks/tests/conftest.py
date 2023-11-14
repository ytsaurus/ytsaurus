import logging
import sys

from yt_odin.test_helpers import yt_env, yt_env_two_clusters  # noqa

pytest_plugins = [
    "yt_odin.test_helpers",
]

handler = logging.StreamHandler(sys.stderr)
handler.setFormatter(logging.Formatter("%(asctime)-15s\t%(levelname)s\t%(message)s"))
logging.getLogger("Odin").handlers.append(handler)
