LIBRARY()

SRCS(
    issue_helpers.h
    issue_helpers.cpp
)

PEERDIR(
    contrib/ydb/library/ydb_issue/proto
    contrib/ydb/library/yql/public/ydb_issue
)

RESOURCE(
    contrib/ydb/library/ydb_issue/ydb_issue.txt ydb_issue.txt
)

END()
