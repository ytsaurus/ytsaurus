from .base import YtCommands
from .chaos import YtCommandsChaos
from .replicated import YtCommandsReplicated


def make_yt_commands(is_chaos: bool) -> YtCommands:
    if is_chaos:
        return YtCommandsChaos()
    else:
        return YtCommandsReplicated()
