LIBRARY()

SRCS(
    csv_arrow.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    contrib/ydb/public/api/protos
    contrib/ydb/public/lib/scheme_types
    yql/essentials/types/uuid
)

END()
