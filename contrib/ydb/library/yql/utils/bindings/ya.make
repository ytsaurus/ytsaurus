LIBRARY()

PEERDIR(
    library/cpp/json
    library/cpp/json/yson
    library/cpp/yson
    contrib/ydb/library/yql/sql/settings
    contrib/ydb/library/yql/utils    
)

SRCS(utils.cpp)

END()

RECURSE_FOR_TESTS(
    ut
)
