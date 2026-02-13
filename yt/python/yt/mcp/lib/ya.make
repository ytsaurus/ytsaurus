PY3_LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

SUBSCRIBER(g:yt-python)

PY_SRCS(
    NAMESPACE yt.mcp.lib

    __init__.py
    tool_runner_mcp.py
    tools/helpers.py
    # tools
    tools/__init__.py
    tools/list_dir.py
    tools/get_attributes.py
    tools/check_is_paths_exists.py
    tools/admin.py
    tools/account.py
    tools/common_client.py
)

PEERDIR(
    yt/python/client
    contrib/python/mcp
    contrib/python/pydantic/pydantic-2
)

END()

RECURSE_FOR_TESTS(
    tests
)
