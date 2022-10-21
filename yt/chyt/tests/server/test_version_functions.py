from yt_commands import authors

from base import ClickHouseTestBase, Clique


class TestVersionFunctions(ClickHouseTestBase):
    @authors("gudqeit")
    def test_version_functions(self):
        with Clique(1) as clique:
            assert len(clique.make_query("select ytVersion()")) != 0
            assert len(clique.make_query("select chytVersion()")) != 0
