import pytest

from yt.yt_sync.core.model.helpers import get_folder
from yt.yt_sync.core.model.helpers import get_list_lca
from yt.yt_sync.core.model.helpers import get_node_name
from yt.yt_sync.core.model.helpers import get_rtt_enabled_from_attrs
from yt.yt_sync.core.model.helpers import is_in_subtree


@pytest.mark.parametrize("key", ["replicated_table_tracker_enabled", "enable_replicated_table_tracker", None])
@pytest.mark.parametrize("value", [True, False, None])
def test_get_rtt_enabled_from_attrs(key: str | None, value: bool | None):
    attrs = {}
    if key is not None:
        attrs[key] = value

    rtt_enabled = get_rtt_enabled_from_attrs(attrs)
    if key is not None:
        assert rtt_enabled is not None
        assert rtt_enabled == bool(value)
    else:
        assert rtt_enabled is None
    assert {} == attrs


def test_path_decomposition():
    assert get_folder("//a/b/c") == "//a/b"
    assert get_node_name("//a/b/c") == "c"

    assert get_folder("//a") == "/"
    assert get_node_name("//a") == "a"

    assert get_folder("/") is None
    assert get_node_name("/") == ""


def test_subtree():
    assert is_in_subtree("//a/b/c", "//a")
    assert is_in_subtree("//a/b/c", "//a/b")
    assert is_in_subtree("//a/b/c", "//a/b/c")

    assert not is_in_subtree("//a/b/c", "//b")
    assert not is_in_subtree("//a/b", "//a/b/c")
    assert not is_in_subtree("//a/folder_other", "//a/folder")


def test_list_lca():
    assert get_list_lca(["//a/b", "//a/c"]) == "//a"
    assert get_list_lca(["//a/b", "//a/c", "//b/c"]) == "/"
    assert get_list_lca(["//a/b", "//a/c", "//a"]) == "//a"
    assert get_list_lca(["//a/b"]) == "//a/b"

    with pytest.raises(AssertionError):
        get_list_lca([])
