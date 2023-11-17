LIBRARY()

PROVIDES(YqlServicePolicy)

SRCS(
    udf_service.cpp
)

PEERDIR(
    contrib/ydb/library/yql/public/udf
)

YQL_LAST_ABI_VERSION()

END()
