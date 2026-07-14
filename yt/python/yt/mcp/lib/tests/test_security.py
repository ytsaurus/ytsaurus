import os
from unittest.mock import patch, MagicMock

import pytest

from yt.mcp.lib.tool_runner_mcp import YTToolRunnerMCP
from yt.mcp.lib.tools.query_tracker import (
    _USE_CLUSTER_RE,
    _QUALIFIED_TABLE_RE,
    _check_query_clusters_yql,
    _check_query_clusters_substring,
)
from yt.mcp.lib.tools.security import (
    WRITE_KEYWORDS,
    check_system_paths,
    check_truncate,
    check_write_keywords,
)


class TestCheckSystemPaths:
    def test_sys_path_in_query_blocked(self):
        with pytest.raises(ValueError, match="//sys"):
            check_system_paths("SELECT * FROM `//sys/users`")

    def test_sys_path_exact_blocked(self):
        with pytest.raises(ValueError):
            check_system_paths("//sys")

    def test_sys_subdir_blocked(self):
        with pytest.raises(ValueError):
            check_system_paths("SELECT * FROM `//sys/tokens/abc`")

    def test_sys_case_insensitive(self):
        with pytest.raises(ValueError):
            check_system_paths("SELECT * FROM `//SYS/users`")

    def test_normal_path_allowed(self):
        check_system_paths("SELECT * FROM `//home/user/table`")

    def test_tmp_path_allowed(self):
        check_system_paths("SELECT * FROM `//tmp/my_table`")

    def test_syscall_not_blocked(self):
        check_system_paths("SELECT * FROM `//syscall_logs`")

    def test_empty_query_allowed(self):
        check_system_paths("")


class TestCheckTruncate:
    def test_truncate_in_with_clause_blocked(self):
        with pytest.raises(ValueError, match="truncate"):
            check_truncate("INSERT INTO t WITH TRUNCATE SELECT * FROM src")

    def test_truncate_in_parens_blocked(self):
        with pytest.raises(ValueError):
            check_truncate("INSERT INTO t WITH (TRUNCATE, EXPIRATION='15m') SELECT * FROM src")

    def test_truncate_case_insensitive(self):
        with pytest.raises(ValueError):
            check_truncate("INSERT INTO t WITH Truncate SELECT * FROM src")

    def test_insert_without_truncate_allowed(self):
        check_truncate("INSERT INTO t SELECT * FROM src")

    def test_select_allowed(self):
        check_truncate("SELECT * FROM t")

    def test_empty_query_allowed(self):
        check_truncate("")


class TestCheckWriteKeywords:
    @pytest.mark.parametrize("keyword", sorted(WRITE_KEYWORDS))
    def test_write_keyword_blocked_without_rw_mode(self, keyword):
        query = "{} INTO t SELECT * FROM src".format(keyword.upper())
        with pytest.raises(ValueError, match="--rw-mode"):
            check_write_keywords(query, rw_mode=False)

    @pytest.mark.parametrize("keyword", sorted(WRITE_KEYWORDS))
    def test_write_keyword_allowed_with_rw_mode(self, keyword):
        query = "{} INTO t SELECT * FROM src".format(keyword.upper())
        check_write_keywords(query, rw_mode=True)

    def test_select_allowed_without_rw_mode(self):
        check_write_keywords("SELECT * FROM t", rw_mode=False)

    def test_select_allowed_with_rw_mode(self):
        check_write_keywords("SELECT * FROM t", rw_mode=True)

    def test_error_message_mentions_rw_mode(self):
        with pytest.raises(ValueError, match="--rw-mode"):
            check_write_keywords("INSERT INTO t SELECT 1", rw_mode=False)

    def test_error_message_mentions_keyword(self):
        with pytest.raises(ValueError, match="insert"):
            check_write_keywords("INSERT INTO t SELECT 1", rw_mode=False)

    def test_case_insensitive_upper(self):
        with pytest.raises(ValueError):
            check_write_keywords("INSERT INTO t SELECT 1", rw_mode=False)

    def test_case_insensitive_mixed(self):
        with pytest.raises(ValueError):
            check_write_keywords("Insert Into t SELECT 1", rw_mode=False)

    def test_keyword_in_path_not_blocked(self):
        check_write_keywords("SELECT * FROM `//home/user/drop_shadow_data`", rw_mode=False)

    def test_keyword_in_column_not_blocked(self):
        check_write_keywords("SELECT last_update_time FROM t", rw_mode=False)

    def test_keyword_prefix_not_blocked(self):
        check_write_keywords("SELECT recreate FROM t", rw_mode=False)

    def test_empty_query_allowed(self):
        check_write_keywords("", rw_mode=False)


class TestDisallowedClusters:
    def _make_runner(self, disallowed_csv=""):
        runner = YTToolRunnerMCP()
        with patch.dict(os.environ, {"YT_MCP_DISALLOWED_CLUSTERS": disallowed_csv}):
            runner.configure_disallowed_clusters_from_env()
        return runner

    def test_disallowed_cluster_blocked(self):
        runner = self._make_runner("restricted")
        with pytest.raises(ValueError, match="forbidden"):
            runner.helper_check_cluster_allowed("restricted")

    def test_allowed_cluster_passes(self):
        runner = self._make_runner("restricted")
        runner.helper_check_cluster_allowed("production")

    def test_multiple_disallowed_clusters(self):
        runner = self._make_runner("restricted,internal")
        with pytest.raises(ValueError):
            runner.helper_check_cluster_allowed("restricted")
        with pytest.raises(ValueError):
            runner.helper_check_cluster_allowed("internal")
        runner.helper_check_cluster_allowed("production")

    def test_case_insensitive(self):
        runner = self._make_runner("restricted")
        with pytest.raises(ValueError):
            runner.helper_check_cluster_allowed("Restricted")
        with pytest.raises(ValueError):
            runner.helper_check_cluster_allowed("RESTRICTED")

    def test_empty_env_allows_all(self):
        runner = self._make_runner("")
        runner.helper_check_cluster_allowed("restricted")
        runner.helper_check_cluster_allowed("production")

    def test_whitespace_in_csv_handled(self):
        runner = self._make_runner(" restricted , internal ")
        with pytest.raises(ValueError):
            runner.helper_check_cluster_allowed("restricted")
        with pytest.raises(ValueError):
            runner.helper_check_cluster_allowed("internal")

    def test_none_cluster_passes(self):
        runner = self._make_runner("restricted")
        runner.helper_check_cluster_allowed(None)
        runner.helper_check_cluster_allowed("")

    def test_helper_get_disallowed_clusters(self):
        runner = self._make_runner("restricted,internal")
        disallowed = runner.helper_get_disallowed_clusters()
        assert "restricted" in disallowed
        assert "internal" in disallowed
        assert len(disallowed) == 2

    def test_explicit_disabled_clusters_parameter(self):
        runner = YTToolRunnerMCP()
        runner.configure_disallowed_clusters_from_env(disabled_clusters=["restricted", "internal"])
        with pytest.raises(ValueError):
            runner.helper_check_cluster_allowed("restricted")
        runner.helper_check_cluster_allowed("production")

    def test_explicit_parameter_takes_priority_over_env(self):
        runner = YTToolRunnerMCP()
        with patch.dict(os.environ, {"YT_MCP_DISALLOWED_CLUSTERS": "from_env"}):
            runner.configure_disallowed_clusters_from_env(disabled_clusters=["from_param"])
        with pytest.raises(ValueError):
            runner.helper_check_cluster_allowed("from_param")
        runner.helper_check_cluster_allowed("from_env")


class TestDisallowedClustersIntegration:
    def test_helper_get_yt_client_blocks_disallowed_cluster(self):
        runner = YTToolRunnerMCP()
        runner.configure_disallowed_clusters_from_env(disabled_clusters=["restricted"])
        runner._yt_token = "fake-token"
        context = MagicMock()

        with pytest.raises(ValueError, match="forbidden"):
            runner.helper_get_yt_client("restricted", context)

    def test_helper_get_yt_client_allows_permitted_cluster(self):
        runner = YTToolRunnerMCP()
        runner.configure_disallowed_clusters_from_env(disabled_clusters=["restricted"])
        runner._yt_token = "fake-token"
        context = MagicMock()

        client = runner.helper_get_yt_client("production", context)
        assert client is not None


class TestUseClusterRegex:
    def test_use_simple(self):
        match = _USE_CLUSTER_RE.search("USE target_cluster; SELECT 1")
        assert match and match.group(1) == "target_cluster"

    def test_use_backtick(self):
        match = _USE_CLUSTER_RE.search("USE `target_cluster`; SELECT 1")
        assert match and match.group(1) == "target_cluster"

    def test_use_case_insensitive(self):
        match = _USE_CLUSTER_RE.search("use Target_Cluster; SELECT 1")
        assert match and match.group(1) == "Target_Cluster"

    def test_no_use(self):
        match = _USE_CLUSTER_RE.search("SELECT * FROM `//home/table`")
        assert match is None

    def test_use_with_spaces(self):
        match = _USE_CLUSTER_RE.search("USE   my_cluster ; SELECT 1")
        assert match and match.group(1) == "my_cluster"


class TestQualifiedTableRegex:
    def test_backtick_qualified(self):
        match = _QUALIFIED_TABLE_RE.search("SELECT * FROM `target_cluster`.`//home/t`")
        assert match and match.group(1) == "target_cluster"

    def test_quote_qualified(self):
        match = _QUALIFIED_TABLE_RE.search("SELECT * FROM 'target_cluster'.'//home/t'")
        assert match and match.group(1) == "target_cluster"

    def test_no_qualified(self):
        match = _QUALIFIED_TABLE_RE.search("SELECT * FROM `//home/t`")
        assert match is None

    def test_space_around_dot(self):
        match = _QUALIFIED_TABLE_RE.search("SELECT * FROM `my_cluster` . `//tmp/x`")
        assert match and match.group(1) == "my_cluster"

    def test_bareword_cluster(self):
        match = _QUALIFIED_TABLE_RE.search("SELECT * FROM target_cluster.`//home/t`")
        assert match and match.group(1) == "target_cluster"

    def test_two_path_join_not_matched(self):
        match = _QUALIFIED_TABLE_RE.search("SELECT * FROM `//a`.`//b`")
        assert match is None


class TestQueryClusterCheckYql:
    def test_blocks_use_statement(self):
        disallowed = {"restricted"}
        with pytest.raises(ValueError, match="forbidden"):
            _check_query_clusters_yql(
                "USE restricted; SELECT 1",
                disallowed,
                YTToolRunnerMCP.helper_check_cluster_allowed.__get__(
                    self._make_runner(disallowed), YTToolRunnerMCP
                ),
            )

    def test_blocks_qualified_table(self):
        disallowed = {"restricted"}
        with pytest.raises(ValueError, match="forbidden"):
            _check_query_clusters_yql(
                "SELECT * FROM `restricted`.`//home/t`",
                disallowed,
                YTToolRunnerMCP.helper_check_cluster_allowed.__get__(
                    self._make_runner(disallowed), YTToolRunnerMCP
                ),
            )

    def test_allows_unrestricted_cluster(self):
        disallowed = {"restricted"}
        _check_query_clusters_yql(
            "USE production; SELECT * FROM `//home/t`",
            disallowed,
            YTToolRunnerMCP.helper_check_cluster_allowed.__get__(
                self._make_runner(disallowed), YTToolRunnerMCP
            ),
        )

    def _make_runner(self, disallowed):
        runner = YTToolRunnerMCP()
        runner._DISALLOWED_CLUSTERS = disallowed
        return runner


class TestQueryClusterCheckSubstring:
    def test_blocks_cluster_name_in_query(self):
        disallowed = {"restricted"}
        runner = self._make_runner(disallowed)
        with pytest.raises(ValueError, match="forbidden"):
            _check_query_clusters_substring(
                "use chyt.restricted",
                disallowed,
                runner.helper_check_cluster_allowed,
            )

    def test_allows_query_without_disallowed_cluster(self):
        disallowed = {"restricted"}
        runner = self._make_runner(disallowed)
        _check_query_clusters_substring(
            "use chyt.production",
            disallowed,
            runner.helper_check_cluster_allowed,
        )

    def test_case_insensitive_substring(self):
        disallowed = {"restricted"}
        runner = self._make_runner(disallowed)
        with pytest.raises(ValueError, match="forbidden"):
            _check_query_clusters_substring(
                "USE chyt.RESTRICTED",
                disallowed,
                runner.helper_check_cluster_allowed,
            )

    def _make_runner(self, disallowed):
        runner = YTToolRunnerMCP()
        runner._DISALLOWED_CLUSTERS = disallowed
        return runner
