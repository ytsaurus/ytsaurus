from yt.wrapper.common import YtError

import os


def get_alias_from_env_or_raise():
    alias = os.getenv("CHYT_ALIAS")
    if alias is None:
        raise YtError("Clique alias should be specified (via argument or via CHYT_PROXY env variable)")
    return alias
