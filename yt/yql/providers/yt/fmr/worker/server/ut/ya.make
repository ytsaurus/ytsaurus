UNITTEST()

SRCS(
    yql_yt_fmr_worker_server_ut.cpp
)

PEERDIR(
    yt/yql/providers/yt/fmr/worker/server
    library/cpp/http/simple
    yql/essentials/utils
    yt/yql/providers/yt/fmr/job/impl
    yt/yql/providers/yt/fmr/job_factory/impl
    yt/yql/providers/yt/fmr/job_preparer/impl
    yt/yql/providers/yt/fmr/test_tools/table_data_service
    yt/yql/providers/yt/fmr/coordinator/yt_coordinator_service/file
    yt/yql/providers/yt/fmr/coordinator/impl
)

YQL_LAST_ABI_VERSION()

END()
