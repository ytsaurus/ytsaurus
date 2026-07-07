import pytest

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
