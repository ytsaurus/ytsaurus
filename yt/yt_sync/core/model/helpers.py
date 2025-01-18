import posixpath
from typing import Any
from typing import Generator

from yt.wrapper import ypath_split

from .types import Types


def make_log_name(table: str) -> str:
    return f"{table}_log"


def make_tmp_name(table: str) -> str:
    return f"{table}.tmp"


def iter_attributes_recursively(
    desired: Any, actual: Any, path: str | None = None
) -> Generator[tuple[str, Any | None, Any | None], None, None]:
    """Returns a triple of values (path, desired, origin) for each key in desired."""
    if actual and isinstance(desired, dict) and isinstance(actual, dict):
        for name in desired.keys():
            new_path = name if not path else f"{path}/{name}"
            yield from iter_attributes_recursively(desired[name], actual.get(name), new_path)
    else:
        assert path
        yield (path, desired, actual)


def get_folder(path: str) -> str | None:
    if path == "/":
        return None
    return ypath_split(path)[0]


def get_node_name(path: str) -> str:
    return ypath_split(path)[1]


def is_in_subtree(path: str, prefix: str) -> bool:
    # Path is in prefix subtree when it is equal to prefix or starts with prefix followed by '/'
    return path == prefix or path.startswith(prefix + "/")


def get_list_lca(paths: list[str]) -> str:
    assert paths, "Cannot find lca path for empty list of paths"
    common_path = posixpath.commonpath(paths)
    # YtPath starts from '/'.
    return "/" + common_path if common_path != "/" else common_path


def get_rtt_enabled_from_attrs(attrs: Types.Attributes) -> bool | None:
    contains = "replicated_table_tracker_enabled" in attrs or "enable_replicated_table_tracker" in attrs
    if not contains:
        return None
    rtt_flag1 = attrs.pop("replicated_table_tracker_enabled", False)
    rtt_flag2 = attrs.pop("enable_replicated_table_tracker", False)
    return bool(rtt_flag1) or bool(rtt_flag2)
