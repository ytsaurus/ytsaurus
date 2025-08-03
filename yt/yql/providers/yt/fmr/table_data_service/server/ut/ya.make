UNITTEST()

SRCS(
    yql_yt_table_data_service_server_ut.cpp
)

PEERDIR(
    yt/yql/providers/yt/fmr/table_data_service/client
    yt/yql/providers/yt/fmr/table_data_service/server
    yt/yql/providers/yt/fmr/table_data_service/discovery/file
    yql/essentials/utils
)

YQL_LAST_ABI_VERSION()

END()
