LIBRARY()

SRCS(
    common.cpp
)

PEERDIR(
    library/cpp/monlib/dynamic_counters

    contrib/ydb/library/conclusion
    contrib/ydb/library/yql/dq/actors/protos

    yql/essentials/public/issue
)

YQL_LAST_ABI_VERSION()

END()
