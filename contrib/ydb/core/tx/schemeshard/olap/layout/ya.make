LIBRARY()

SRCS(
    layout.cpp
)

PEERDIR(
    contrib/ydb/core/scheme
    contrib/ydb/core/tablet_flat
)

YQL_LAST_ABI_VERSION()

END()
