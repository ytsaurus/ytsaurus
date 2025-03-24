LIBRARY()

SRCS(
    config_base.h
    config_base.cpp
)

PEERDIR(
    library/cpp/deprecated/enum_codegen
    contrib/ydb/core/util
    contrib/ydb/public/lib/deprecated/client
    yql/essentials/minikql
)

YQL_LAST_ABI_VERSION()

END()
