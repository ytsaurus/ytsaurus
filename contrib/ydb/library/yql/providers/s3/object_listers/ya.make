LIBRARY()

SRCS(
    yql_s3_list.cpp
    yql_s3_path.cpp
)

GENERATE_ENUM_SERIALIZATION(yql_s3_list.h)

PEERDIR(
    contrib/libs/re2
    library/cpp/string_utils/quote
    library/cpp/xml/document
    contrib/ydb/library/yql/providers/common/http_gateway
    contrib/ydb/library/yql/utils
    contrib/ydb/library/yql/utils/threading
)

END()

RECURSE_FOR_TESTS(
    ut
)
