import yt.wrapper as yt


def basename(path):
    return yt.ypath_split(path)[-1]


def join_path(*args):
    return yt.ypath_join(*args)


def dirname(path):
    return yt.ypath_dirname(path)


def get_master_binary_path(yt_client, master_binary_path, snapshot_path):
    # COMPAT(akozhikhov)
    try:
        master_version = yt_client.get("{}/@master_version".format(snapshot_path))
        master_binary_path = "{}_{}".format(master_binary_path, master_version)
    except yt.YtResponseError as err:
        if not err.is_resolve_error():
            raise

    if not yt_client.exists(master_binary_path):
        raise RuntimeError("Master binary not found at {}".format(master_binary_path))
    return master_binary_path
