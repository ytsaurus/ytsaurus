import yt.wrapper as yt
from yt.transfer_manager import pattern_matching
import pytest

class TestPatternMatching(object):
    def test_tokenize(self):
        tokenize = pattern_matching._tokenize
        assert tokenize("//") == ("//", )
        assert tokenize("//home/{a}/{b}") == ("//home/", "{a}", "/", "{b}")
        assert tokenize("//home/{a}{b}") == ("//home/", "{a}", "{b}")
        assert tokenize("//{a}") == ("//", "{a}")
        assert tokenize("//home/{}/{0a_}") == ("//home/", "{}", "/", "{0a_}")
        assert tokenize("//home/{*}") == ("//home/", "{*}")
        assert tokenize("//home/{ab/c") == ("//home/{ab/c",)
        # Placeholder does not match \w pattern
        assert tokenize("//home/{a/b/c}") == ("//home/{a/b/c}",)
        assert tokenize("") == tuple()

    def test_match_copy_pattern(self):
        match = pattern_matching.match_copy_pattern

        class YtClientMock(object):
            TEST_TABLE_NAMES = ["//a/table", "//a/b/table2", "//a/b/c/table3", "//d/table",
                                "//d/d1/a/table", "//d/d2/a/table", "//d/d3/a/b/table"]
            def __init__(self):
                self._type = "yt"

            def search(self, *args, **kwargs):
                """ Emulates YT client search() method """
                return (name for name in self.TEST_TABLE_NAMES)

            def get_type(self, path, *args, **kwargs):
                """ Emulates YT client get_type() method """
                map_nodes = ["//a", "//a/b", "//a/b/c", "//d", "//d/d1", "//d/d1/a", "//d/d2", "//d/d2/a",
                             "//d/d3", "//d/d3/a", "//d/d3/a/b"]
                return "map_node" if path in map_nodes else "table"

            def exists(self, path, *args, **kwargs):
                return True

        client = YtClientMock()

        with pytest.raises(yt.YtError):
            match(client, "//a/{p}", "//a")
        with pytest.raises(yt.YtError):
            match(client, "//a/{b}/{c}", "//a/{c}/{b}")
        with pytest.raises(yt.YtError):
            match(client, "//a/{a}{b}", "tmp/{a}/{b}")
        with pytest.raises(yt.YtError):
            match(client, "//{*}/a", "tmp/{*}")
        with pytest.raises(yt.YtError):
            match(client, "//{*}/{*}", "tmp/{*}/{*}")

        assert match(client, "//a/{p}", "tmp/{p}") == [("//a/table", "tmp/table")]
        assert match(client, "//{p1}/{p2}/table2", "tmp/{p1}/{p2}/table2") == [("//a/b/table2",
                                                                                "tmp/a/b/table2")]
        assert match(client, "//a/table", "tmp/table") == [("//a/table", "tmp/table")]
        assert match(client, "//d/{p}/a", "tmp/{p}") == []
        assert match(client, "//d/{p}/a/b/table", "tmp/{p}/a/b/other") == [("//d/d3/a/b/table", "tmp/d3/a/b/other")]
        assert match(client, "//d/{p}/a/{p}", "tmp/{p}/a/{p}") == [("//d/d1/a/table", "tmp/d1/a/table"),
                                                                   ("//d/d2/a/table", "tmp/d2/a/table")]

        assert match(client, "//a/{*}", "tmp/{*}") == [("//a/table", "tmp/table"),
                                                       ("//a/b/table2", "tmp/b/table2"),
                                                       ("//a/b/c/table3", "tmp/b/c/table3")]
        # If source path is a directory add "{*}" placeholder automatically
        assert match(client, "//a", "tmp/") == [("//a/table", "tmp/table"),
                                                ("//a/b/table2", "tmp/b/table2"),
                                                ("//a/b/c/table3", "tmp/b/c/table3")]

        assert match(client, "//d/{p}/a/{*}", "tmp/{p}/{*}") == [("//d/d1/a/table", "tmp/d1/table"),
                                                                 ("//d/d2/a/table", "tmp/d2/table"),
                                                                 ("//d/d3/a/b/table", "tmp/d3/b/table")]
        assert match(client, "//d/{p}/{*}", "tmp/{p}/{*}") == [("//d/d1/a/table", "tmp/d1/a/table"),
                                                               ("//d/d2/a/table", "tmp/d2/a/table"),
                                                               ("//d/d3/a/b/table", "tmp/d3/a/b/table")]
