PY3_LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yql/cfg/proto
    yql/essentials/core/file_storage/proto
    yql/essentials/providers/common/proto
    yt/python/yt/environment
    yt/python/yt/wrapper
)

PY_SRCS(
    NAMESPACE yt.environment.components.yql_agent

    __init__.py
    qtworker_configs.py
)

END()
