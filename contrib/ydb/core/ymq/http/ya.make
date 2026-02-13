LIBRARY()

SRCS(
    parser.rl6
    http.cpp
    types.cpp
    xml.cpp
    xml_builder.cpp
)

PEERDIR(
    contrib/libs/libxml
    contrib/ydb/library/actors/core
    library/cpp/cgiparam
    library/cpp/http/misc
    library/cpp/http/server
    library/cpp/protobuf/json
    library/cpp/string_utils/base64
    library/cpp/string_utils/quote
    library/cpp/string_utils/url
    contrib/ydb/core/protos
    contrib/ydb/core/ymq/actor
    contrib/ydb/core/ymq/base
    contrib/ydb/library/http_proxy/authorization
    contrib/ydb/library/http_proxy/error
)

END()

RECURSE_FOR_TESTS(
    ut
)
