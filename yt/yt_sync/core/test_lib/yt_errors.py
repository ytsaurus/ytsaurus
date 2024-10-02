import yt.wrapper as yt


def yt_generic_error() -> yt.YtError:
    return yt.YtError("generic error")


def yt_faulty_error() -> yt.YtError:
    return yt.YtError("faulty error")


def yt_resolve_error() -> yt.YtError:
    return yt.YtError("resolve error", code=500)


def yt_access_denied() -> yt.YtError:
    return yt.YtError("access denied", code=901)


def yt_retryable_error() -> yt.YtError:
    return yt.YtError("table mount info is not ready", code=1707)
