UNITTEST()

SRCS(
    yql_yt_table_data_service_server_ut.cpp
)

PEERDIR(
    yt/yql/providers/yt/fmr/request_options
    yt/yql/providers/yt/fmr/test_tools/table_data_service
    yql/essentials/utils
)

YQL_LAST_ABI_VERSION()

END()
