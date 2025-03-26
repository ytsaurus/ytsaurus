LIBRARY()

SRCS(
    events.cpp
    lock_handle.cpp
    types.cpp
)

PEERDIR(
    library/cpp/cgiparam
    library/cpp/lwtrace
    library/cpp/uri
    contrib/ydb/core/base
    contrib/ydb/core/protos
    contrib/ydb/core/util
    yql/essentials/public/issue
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
