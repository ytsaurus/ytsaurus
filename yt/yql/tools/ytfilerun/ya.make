PROGRAM(ytfilerun)

SRCS(
    ytfilerun.cpp
)

PEERDIR(
    contrib/ydb/library/yql/dq/opt
)

INCLUDE(${ARCADIA_ROOT}/yql/tools/yqlrun/ya.make.inc)

YQL_LAST_ABI_VERSION()

END()
