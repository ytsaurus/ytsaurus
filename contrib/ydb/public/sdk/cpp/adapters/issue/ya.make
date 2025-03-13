LIBRARY()

PEERDIR(
    yql/essentials/public/issue
    contrib/ydb/library/yql/public/ydb_issue
    contrib/ydb/public/sdk/cpp/src/library/issue
)

SRCS(
    issue.cpp
)

END()
