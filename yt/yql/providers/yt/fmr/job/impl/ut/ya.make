UNITTEST()

SRCS(
    yql_yt_job_ut.cpp
)

PEERDIR(
    library/cpp/yt/assert
    yt/cpp/mapreduce/interface
    yt/yql/providers/yt/fmr/job/impl
    yt/yql/providers/yt/fmr/yt_service/mock
    yt/yql/providers/yt/fmr/table_data_service/local
    yql/essentials/utils/log
)

YQL_LAST_ABI_VERSION()

END()
