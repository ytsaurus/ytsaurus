YQL_LIBRARY()

SRCS(
    fmrrun_lib.cpp
)

PEERDIR(
    contrib/ydb/library/yql/dq/opt
    yt/yql/providers/yt/gateway/file
    yt/yql/providers/yt/gateway/fmr
    yt/yql/providers/yt/fmr/coordinator/yt_coordinator_service/file
    yt/yql/providers/yt/fmr/fmr_tool_lib
    yt/yql/providers/yt/fmr/job_preparer/impl
    yql/tools/yqlrun/lib
)

END()
