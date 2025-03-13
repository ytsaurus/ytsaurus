LIBRARY()

SRCS(
    csv_arrow.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    contrib/ydb/public/api/protos
)

END()
