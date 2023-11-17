LIBRARY()

SRCS(
    http_download.cpp
)

PEERDIR(
    contrib/ydb/library/yql/core/file_storage/defs
    contrib/ydb/library/yql/core/file_storage/download
    contrib/ydb/library/yql/core/file_storage/proto
    contrib/ydb/library/yql/core/file_storage/http_download/proto
    contrib/ydb/library/yql/utils/fetch
    contrib/ydb/library/yql/utils/log
    contrib/ydb/library/yql/utils
    library/cpp/digest/md5
    library/cpp/http/misc
)

END()
