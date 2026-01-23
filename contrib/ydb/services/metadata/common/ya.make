LIBRARY()

SRCS(
    timeout.cpp
    ss_dialog.cpp
)

PEERDIR(
    contrib/ydb/services/metadata/initializer
    contrib/ydb/services/metadata/abstract
    contrib/ydb/services/bg_tasks/abstract
    contrib/ydb/core/tx/scheme_cache
)

YQL_LAST_ABI_VERSION()

END()
