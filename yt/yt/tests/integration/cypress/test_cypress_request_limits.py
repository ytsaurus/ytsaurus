from yt_env_setup import YTEnvSetup

from yt_commands import (
    authors, get, set, raises_yt_error, ls, create, create_user, remove, exists,
    print_debug)

import pytest

################################################################################


def set_or_remove(path, value):
    if value is None:
        if exists(path):
            remove(path)
    else:
        set(path, value)


class TestCypressRequestLimits(YTEnvSetup):
    NUM_CELLS = 1
    NUM_MASTERS = 1
    NUM_NODES = 0

    DELTA_DYNAMIC_MASTER_CONFIG = {
        "object_service": {
            "enable_read_request_complexity_limits": True
        }
    }

    @authors("kvk1920")
    def test_get_node_count_limit(self):
        create_user("u")
        tmp = {f"d{i}": {} for i in range(50)}
        set("//tmp", tmp, force=True)
        assert get("//tmp", authenticated_user="u") == tmp
        # explicit limit, user's default limit, global default limit, ok
        cases = [
            (None, 30, 60, False),
            (None, 60, 30, True),
            (None, None, 30, False),
            (None, None, 60, True),
            (30, 60, 60, False),
            (30, None, 60, False),
            (60, 30, 30, True),
            (60, None, 30, True),
        ]
        for explicit_limit, per_user_limit, global_limit, ok in cases:
            set_or_remove("//sys/users/u/@request_limits/default_read_request_complexity/node_count", per_user_limit)
            set("//sys/@config/object_service/default_read_request_complexity_limits/node_count", global_limit)
            if ok:
                assert tmp == get("//tmp", authenticated_user="u", node_count_limit=explicit_limit)
            else:
                with raises_yt_error("Read request complexity limits exceeded"):
                    get("//tmp", authenticated_user="u", node_count_limit=explicit_limit)

    @authors("kvk1920")
    def test_list_node_count_limit(self):
        create_user("u")
        tmp = {f"d{i}": {} for i in range(50)}
        set("//tmp", tmp, force=True)
        tmp = sorted(tmp)
        assert sorted(ls("//tmp", authenticated_user="u")) == tmp
        # explicit limit, user's default limit, global default limit, ok
        cases = [
            (None, 30, 60, False),
            (None, 60, 30, True),
            (None, None, 30, False),
            (None, None, 60, True),
            (30, 60, 60, False),
            (30, None, 60, False),
            (60, 30, 30, True),
            (60, None, 30, True),
        ]
        for explicit_limit, per_user_limit, global_limit, ok in cases:
            set_or_remove("//sys/users/u/@request_limits/default_read_request_complexity/node_count", per_user_limit)
            set("//sys/@config/object_service/default_read_request_complexity_limits/node_count", global_limit)
            if ok:
                assert tmp == sorted(ls("//tmp", authenticated_user="u", node_count_limit=explicit_limit))
            else:
                with raises_yt_error("Read request complexity limits exceeded"):
                    ls("//tmp", authenticated_user="u", node_count_limit=explicit_limit)

    @authors("kvk1920")
    def test_get_result_size_limit(self):
        create_user("u")
        tmp = {f"d{i}": {} for i in range(50)}
        set("//tmp", tmp, force=True)
        assert get("//tmp", authenticated_user="u") == tmp
        size = len(get("//tmp", is_raw=True))
        # explicit limit, user's default limit, global default limit, ok
        cases = [
            (None, size - 10, size + 10, False),
            (None, size + 10, size - 10, True),
            (None, None, size - 10, False),
            (None, None, size + 10, True),
            (size - 10, size + 10, size + 10, False),
            (size - 10, None, size + 10, False),
            (size + 10, size - 10, size - 10, True),
            (size + 10, None, size - 10, True),
        ]
        for explicit_limit, per_user_limit, global_limit, ok in cases:
            set_or_remove("//sys/users/u/@request_limits/default_read_request_complexity/result_size", per_user_limit)
            set("//sys/@config/object_service/default_read_request_complexity_limits/result_size", global_limit)
            if ok:
                assert tmp == get("//tmp", authenticated_user="u", result_size_limit=explicit_limit)
            else:
                with raises_yt_error("Read request complexity limits exceeded"):
                    get("//tmp", authenticated_user="u", result_size_limit=explicit_limit)

    @authors("kvk1920")
    @pytest.mark.parametrize("size", [10, 20, 50, 100])
    def test_list_result_size_limit(self, size):
        create_user("u")
        tmp = {f"d{i}": {} for i in range(size)}
        set("//tmp", tmp, force=True)
        tmp = sorted(tmp)
        assert sorted(ls("//tmp", authenticated_user="u")) == tmp
        size = len(str(ls("//tmp")))
        print_debug(f"raw_size = {size}")
        # explicit limit, user's default limit, global default limit, ok
        cases = [
            (None, size // 2, size * 2, False),
            (None, size * 2, size // 2, True),
            (None, None, size // 2, False),
            (None, None, size * 2, True),
            (size // 2, size * 2, size * 2, False),
            (size // 2, None, size * 2, False),
            (size * 2, size // 2, size // 2, True),
            (size * 2, None, size // 2, True),
        ]
        for explicit_limit, per_user_limit, global_limit, ok in cases:
            set_or_remove("//sys/users/u/@request_limits/default_read_request_complexity/result_size", per_user_limit)
            set("//sys/@config/object_service/default_read_request_complexity_limits/result_size", global_limit)
            if ok:
                assert tmp == sorted(ls("//tmp", authenticated_user="u", result_size_limit=explicit_limit))
            else:
                with raises_yt_error("Read request complexity limits exceeded"):
                    ls("//tmp", authenticated_user="u", result_size_limit=explicit_limit)

    @authors("kvk1920")
    def test_get_attribute_result_size_limit(self):
        create_user("u")
        attr = {"a": 1, "b": 2, "c": {"a": 4, "d": 7}}
        create("table", "//tmp/t")
        set("//tmp/t/@my_attr", attr)
        raw_attr = get("//tmp/t/@my_attr", is_raw=True)
        raw_size = len(raw_attr)
        cases = [
            (None, 2**30, True),
            (raw_size - 10, 2**30, False),
            (raw_size + 10, 2**30, True),
            (None, raw_size + 10, True),
            (raw_size - 10, raw_size + 10, False),
            (raw_size + 10, raw_size + 10, True),
            (None, raw_size - 10, False),
            (raw_size - 10, raw_size - 10, False),
            (raw_size + 10, raw_size - 10, True),
        ]
        for per_user_limit, global_limit, ok in cases:
            set_or_remove("//sys/users/u/@request_limits/default_read_request_complexity/result_size", per_user_limit)
            set("//sys/@config/object_service/default_read_request_complexity_limits/result_size", global_limit)
            if ok:
                assert get("//tmp/t/@my_attr", authenticated_user="u") == attr
            else:
                with raises_yt_error("Read request complexity limits exceeded"):
                    get("//tmp/t/@my_attr", authenticated_user="u")

    @authors("kvk1920")
    def test_overflow_during_attribute_read(self):
        create_user("u")
        create("document", "//tmp/d")
        set("//tmp/d/@my_attr", list(range(1000)))

        set("//sys/@config/object_service/default_read_request_complexity_limits", {"node_count": 100, "result_size": 100})
        with raises_yt_error("Read request complexity limits exceeded"):
            ls("//tmp", attributes=["my_attr"], authenticated_user="u")

    @authors("kvk1920")
    def test_disabling(self):
        create_user("u")
        for i in range(50):
            create("map_node", f"//tmp/{i}")
        with raises_yt_error("Read request complexity limits exceeded"):
            ls("//tmp", authenticated_user="u", node_count_limit=10)
        set("//sys/users/u/@request_limits/default_read_request_complexity/node_count", 10)
        with raises_yt_error("Read request complexity limits exceeded"):
            ls("//tmp", authenticated_user="u")
        set("//sys/@config/object_service/enable_read_request_complexity_limits", False)
        assert ls("//tmp") == ls("//tmp", authenticated_user="u", node_count_limit=10)

    @authors("kvk1920")
    def test_max_complexity_limit_per_user(self):
        create_user("u")
        set("//sys/@config/object_service/max_read_request_complexity_limits/node_count", 100)
        with raises_yt_error("Read request complexity limits too large"):
            ls("//tmp", authenticated_user="u", node_count_limit=111)
        set_or_remove("//sys/users/u/@request_limits/max_read_request_complexity/node_count", 150)
        assert ls("//tmp", authenticated_user="u", node_count_limit=111) == []


################################################################################


class TestCypressRequestLimitsRpcProxy(TestCypressRequestLimits):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
    ENABLE_HTTP_PROXY = True
