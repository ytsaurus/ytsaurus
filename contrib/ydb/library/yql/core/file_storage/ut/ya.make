UNITTEST_FOR(contrib/ydb/library/yql/core/file_storage)

SRCS(
    file_storage_ut.cpp
    sized_cache_ut.cpp
    storage_ut.cpp
)

PEERDIR(
    library/cpp/http/server
    library/cpp/threading/future
    library/cpp/deprecated/atomic
    contrib/ydb/library/yql/utils/test_http_server
)

END()
