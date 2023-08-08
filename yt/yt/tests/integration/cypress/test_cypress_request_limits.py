from yt_env_setup import YTEnvSetup

from yt_commands import authors, get, set, raises_yt_error, ls, create, create_user, remove

from contextlib import contextmanager

################################################################################


def set_or_remove(path, value):
    if value is None:
        remove(path)
    else:
        set(path, value)


@contextmanager
def set_config(path, value):
    old_value = get(path)
    set_or_remove(path, value)
    try:
        yield
    finally:
        set_or_remove(path, old_value)


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
        cases = [
            (30, 60, False),
            (60, 30, False),
            (60, 60, True),
            (60, None, True),
            (None, 30, False),
            (None, None, True),
        ]
        for per_user_limit, global_limit, ok in cases:
            with set_config("//sys/users/u/@request_limits/read_request_complexity/node_count", per_user_limit):
                with set_config("//sys/@config/object_service/max_read_request_complexity_limits/node_count", global_limit):
                    if ok:
                        assert tmp == get("//tmp", authenticated_user="u")
                    else:
                        with raises_yt_error("Read complexity limit exceeded"):
                            get("//tmp", authenticated_user="u")

    @authors("kvk1920")
    def test_list_node_count_limit(self):
        create_user("u")
        tmp = {f"d{i}": {} for i in range(50)}
        set("//tmp", tmp, force=True)
        assert sorted(ls("//tmp", authenticated_user="u")) == sorted(tmp.keys())
        cases = [
            (30, 60, False),
            (60, 30, False),
            (60, 60, True),
            (60, None, True),
            (None, 30, False),
            (None, None, True),
        ]
        for per_user_limit, global_limit, ok in cases:
            with set_config("//sys/users/u/@request_limits/read_request_complexity/node_count", per_user_limit):
                with set_config("//sys/@config/object_service/max_read_request_complexity_limits/node_count", global_limit):
                    if ok:
                        assert sorted(tmp.keys()) == sorted(ls("//tmp", authenticated_user="u"))
                    else:
                        with raises_yt_error("Read complexity limit exceeded"):
                            ls("//tmp", authenticated_user="u")

    @authors("kvk1920")
    def test_get_attribute_result_size_limit(self):
        create_user("u")
        attr = {"a": 1, "b": 2, "c": {"a": 4, "d": 7}}
        create("table", "//tmp/t")
        set("//tmp/t/@my_attr", attr)
        raw_attr = get("//tmp/t/@my_attr", is_raw=True)
        raw_size = len(raw_attr)
        cases = [
            (None, None, True),
            (raw_size - 5, None, False),
            (raw_size + 5, None, True),
            (None, raw_size + 5, True),
            (raw_size - 5, raw_size + 5, False),
            (raw_size + 5, raw_size + 5, True),
            (None, raw_size - 5, False),
            (raw_size - 5, raw_size - 5, False),
            (raw_size + 5, raw_size - 5, None),
        ]
        for per_user_limit, global_limit, ok in cases:
            with set_config("//sys/users/u/@request_limits/read_request_complexity/result_size", per_user_limit):
                with set_config("//sys/@config/object_service/max_read_request_complexity_limits/result_size", global_limit):
                    if ok:
                        assert get("//tmp/t/@my_attr", authenticated_user="u") == attr
                    else:
                        with raises_yt_error("Read complexity limit exceeded"):
                            get("//tmp/t/@my_attr", authenticated_user="u")

    @authors("kvk1920")
    def test_overflow_during_attribute_write(self):
        create_user("u")
        create("document", "//tmp/d")
        set("//tmp/d/@my_attr", list(range(1000)))

        with set_config("//sys/@config/object_service/max_read_request_complexity_limits", {"node_count": 100, "result_size": 100}):
            with raises_yt_error("Read complexity limit exceeded"):
                ls("//tmp", attributes=["my_attr"], authenticated_user="u")

    @authors("kvk1920")
    def test_disabling(self):
        create_user("u")
        for i in range(50):
            create("map_node", f"//tmp/{i}")
        set("//sys/users/u/@request_limits/read_request_complexity/node_count", 10)
        with raises_yt_error("Read complexity limit exceeded"):
            ls("//tmp", authenticated_user="u")
        set("//sys/@config/object_service/enable_read_request_complexity_limits", False)
        assert ls("//tmp") == ls("//tmp", authenticated_user="u")
