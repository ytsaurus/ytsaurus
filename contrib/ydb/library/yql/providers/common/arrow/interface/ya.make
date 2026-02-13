LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    library/cpp/threading/future
    contrib/ydb/library/yql/providers/common/http_gateway
)

SRCS(
    arrow_reader.cpp
)

END()