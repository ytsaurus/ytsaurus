LIBRARY()

SRCS(
    fmrrun_lib.cpp
)

PEERDIR(
    contrib/ydb/library/yql/dq/opt
    yt/yql/providers/yt/gateway/file
    yt/yql/providers/yt/gateway/fmr
    yt/yql/providers/yt/fmr/fmr_tool_lib
    yql/tools/yqlrun/lib
)

YQL_LAST_ABI_VERSION()

END()
