from yt.testlib import authors, set_config_option

from yt.wrapper import YtError, YPath
from yt.yson import YsonString, YsonUnicode

import pytest


def _to_yson_string(string):
    return YsonString(string.encode())


@authors("babenko")
@pytest.mark.parametrize("str_type", [str, YsonUnicode, _to_yson_string])
def test_ampersand_object_root(str_type):
    with set_config_option("prefix", None):
        path = YPath(str_type("&#1-2-3-4"))
        assert str(path) == "&#1-2-3-4"
        assert "cluster" not in path.attributes

        path = YPath(str_type("&#1-2-3-4/@attr"))
        assert str(path) == "&#1-2-3-4/@attr"

        path = YPath(str_type("&#1-2-3-4[#1:#2]"))
        assert str(path) == "&#1-2-3-4"
        assert path.attributes["ranges"][0]["lower_limit"]["row_index"] == 1
        assert path.attributes["ranges"][0]["upper_limit"]["row_index"] == 2

        path = YPath(str_type("<attr=10> &#1-2-3-4"))
        assert str(path) == "&#1-2-3-4"
        assert path.attributes["attr"] == 10

        path = YPath(str_type("mycluster:&#1-2-3-4"))
        assert str(path) == "&#1-2-3-4"
        assert path.attributes["cluster"] == "mycluster"

        with pytest.raises(YtError, match=r"should be absolute or you should specify a prefix"):
            YPath(str_type("&//home"))


@authors("babenko")
def test_ampersand_object_root_ignores_prefix():
    with set_config_option("prefix", "//my/path/"):
        assert str(YPath("&#1-2-3-4")) == "&#1-2-3-4"
        assert str(YPath("mycluster:&#1-2-3-4")) == "&#1-2-3-4"
        assert str(YPath("table")) == "//my/path/table"
