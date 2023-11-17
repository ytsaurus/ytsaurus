LIBRARY()

SRCS(
    config.cpp
    control_plane_proxy.cpp
    probes.cpp
)

PEERDIR(
    library/cpp/actors/core
    contrib/ydb/core/base
    contrib/ydb/core/fq/libs/actors
    contrib/ydb/core/fq/libs/actors/logging
    contrib/ydb/core/fq/libs/compute/ydb
    contrib/ydb/core/fq/libs/compute/ydb/control_plane
    contrib/ydb/core/fq/libs/control_plane_config
    contrib/ydb/core/fq/libs/control_plane_proxy/actors
    contrib/ydb/core/fq/libs/control_plane_proxy/events
    contrib/ydb/core/fq/libs/control_plane_storage
    contrib/ydb/core/fq/libs/rate_limiter/events
    contrib/ydb/core/fq/libs/result_formatter
    contrib/ydb/core/mon
    contrib/ydb/library/folder_service
    contrib/ydb/library/security
    contrib/ydb/library/ycloud/api
    contrib/ydb/library/ycloud/impl
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    actors
    events
)

RECURSE_FOR_TESTS(
    ut
)
