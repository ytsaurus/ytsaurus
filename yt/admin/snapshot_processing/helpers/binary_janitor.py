import os
from datetime import datetime, timedelta

from .cypress_helpers import join_path
import yt.logger as logger

DATETIME_PATTERN = "%Y-%m-%dT%H:%M:%S.%fZ"
BINARY_LIFETIME_HOURS = 8
LOCAL_MASTER_TRUNK_BINARY_DIR_PREFIX = "/tmp/ytserver-master_custom_trunk"


# Unique for every process.
def get_local_master_trunk_binary_dir():
    return LOCAL_MASTER_TRUNK_BINARY_DIR_PREFIX + "_" + str(os.getpid())


# Lexicographical order of binary names corresponds to their version.
def get_trunk_binary_cypress_path(yt_client, master_binary_cypress_dir):
    fresh_binary_name = yt_client.list(master_binary_cypress_dir)[-1]
    if fresh_binary_name.find("trunk") == -1:
        return None, False

    fresh_binary_full_path = join_path(master_binary_cypress_dir, fresh_binary_name)

    return fresh_binary_full_path, True


def check_trunk_binary_is_needed(yt_client, master_binary_cypress_dir):
    fresh_binary_full_path, is_trunk_binary_present = get_trunk_binary_cypress_path(yt_client, master_binary_cypress_dir)
    if is_trunk_binary_present is False:
        logger.info("Trunk binary is missing in cypress and needs to be uploaded")
        return True

    creation_time_str = yt_client.get("{}/@creation_time".format(fresh_binary_full_path))
    creation_time = datetime.strptime(creation_time_str, DATETIME_PATTERN)

    current_time = datetime.now()
    if (creation_time + timedelta(hours=BINARY_LIFETIME_HOURS) < current_time):
        logger.info("Trunk binary is older than expected, new binary needs to be uploaded")
        return True

    return False
