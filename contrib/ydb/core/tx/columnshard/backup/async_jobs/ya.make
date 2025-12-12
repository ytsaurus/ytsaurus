LIBRARY()

SRCS(
    import_downloader.cpp
)

PEERDIR(
    contrib/ydb/core/formats/arrow
    contrib/ydb/library/actors/core
    contrib/ydb/core/tx/datashard
)

YQL_LAST_ABI_VERSION()


END()

RECURSE_FOR_TESTS(
    ut
)