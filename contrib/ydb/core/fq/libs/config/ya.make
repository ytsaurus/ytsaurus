LIBRARY()

SRCS(
    yq_issue.cpp
)

PEERDIR(
    contrib/ydb/core/fq/libs/config/protos
    contrib/ydb/library/yql/public/issue/protos
)

END()

RECURSE(
    protos
)
