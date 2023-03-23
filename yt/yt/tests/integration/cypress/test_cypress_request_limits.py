from yt_env_setup import YTEnvSetup

from yt_commands import authors, get, set, raises_yt_error, ls, create

from contextlib import contextmanager

################################################################################


@contextmanager
def set_sys_config_object_service(path, value):
    path = f"//sys/@config/object_service{path}"
    old_value = get(path)
    set(path, value)
    try:
        yield
    finally:
        set(path, old_value)


class CypressRequestLimitsBase(YTEnvSetup):
    NUM_CELLS = 1
    NUM_MASTERS = 1
    NUM_NODES = 0

    @authors("kvk1920")
    def test_get_node_count_limit(self):
        tmp = {f"d{i}": {} for i in range(50)}
        set("//tmp", tmp, force=True)
        assert get("//tmp") == tmp
        with set_sys_config_object_service("/default_read_request_complexity_limits/node_count", 32):
            with raises_yt_error("Read complexity limit exceeded"):
                get("//tmp")
            with raises_yt_error("Read complexity limit exceeded"):
                get("//tmp", node_count_limit=45)
            assert get("//tmp", node_count_limit=60) == tmp
        assert get("//tmp") == tmp

    @authors("kvk1920")
    def test_list_node_count_limit(self):
        tmp = {f"d{i}": {} for i in range(50)}
        set("//tmp", tmp, force=True)
        tmp = sorted(tmp)
        assert sorted(ls("//tmp")) == tmp
        with set_sys_config_object_service("/default_read_request_complexity_limits/node_count", 32):
            with raises_yt_error("Read complexity limit exceeded"):
                ls("//tmp")
            with raises_yt_error("Read complexity limit exceeded"):
                ls("//tmp", node_count_limit=45)
            assert sorted(ls("//tmp", node_count_limit=60)) == tmp
        assert sorted(ls("//tmp")) == tmp

    @authors("kvk1920")
    def test_get_attribute_result_size_limit(self):
        attr = {"a": 1, "b": 2, "c": {"a": 4, "d": 7}}
        create("table", "//tmp/t")
        set("//tmp/t/@my_attr", attr)
        raw_attr = get("//tmp/t/@my_attr", is_raw=True)
        raw_size = len(raw_attr)
        for limit in (raw_size - 10, raw_size + 10):
            if limit >= raw_size:
                assert get("//tmp/t/@my_attr", result_size_limit=limit) == attr
            else:
                with raises_yt_error("Read complexity limit exceeded"):
                    get("//tmp/t/@my_attr", result_size_limit=limit)

    @authors("kvk1920")
    def test_list_attribute_result_count_limit(self):
        attr = {"a": 1, "b": 2, "c": {"a": 4, "d": 7}, "d": 3, "f": 5}
        create("table", "//tmp/t")
        set("//tmp/t/@my_attr", attr)
        node_count = len(ls("//tmp/t/@my_attr/c"))
        assert sorted(ls("//tmp/t/@my_attr/c", node_count_limit=node_count)) == sorted(attr["c"])

        # NB: Node count limit doesn't work for attributes.
        sorted(ls("//tmp/t/@my_attr", node_count_limit=node_count)) == sorted(attr)

    @authors("kvk1920")
    def test_get_result_size_limit(self):
        tmp = {f"d{i}": {} for i in range(50)}
        set("//tmp", tmp, force=True)
        assert get("//tmp") == tmp
        raw_size = len(get("//tmp", is_raw=True))
        for limit in (raw_size - 10, raw_size + 10):
            if limit >= raw_size:
                assert get("//tmp", result_size_limit=limit) == tmp
            else:
                with raises_yt_error("Read complexity limit exceeded"):
                    get("//tmp", result_size_limit=limit)

    @authors("kvk1920")
    def test_get_hard_limit(self):
        tmp = {f"d{i}": {} for i in range(50)}
        set("//tmp", tmp, force=True)
        assert get("//tmp") == tmp
        with set_sys_config_object_service("/max_read_request_complexity_limits/node_count", 32):
            with raises_yt_error("Read complexity limit exceeded"):
                get("//tmp")
            with raises_yt_error("Read complexity limit exceeded"):
                get("//tmp", node_count_limit=45)
            with raises_yt_error("Read complexity limit exceeded"):
                get("//tmp", node_count_limit=55)


################################################################################


class TestCypressRequestLimitsHttpProxies(CypressRequestLimitsBase):
    ENABLE_HTTP_PROXY = True
    NUM_HTTP_PROXIES = 1


################################################################################


class TestCypressRequestLimitsRpcProxies(CypressRequestLimitsBase):
    ENABLE_RPC_PROXY = True
    NUM_RPC_PROXIES = 1
