SRCS(
    yt/yql/tools/ytfilerun/ytfilerun.cpp
)

PEERDIR(
    contrib/ydb/library/yql/dq/opt
)

INCLUDE(${ARCADIA_ROOT}/yql/tools/yqlrun/ya.make.inc)

YQL_CURRENT_ABI_VERSION()
