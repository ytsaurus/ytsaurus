RECURSE_FOR_TESTS(
    ut
)

LIBRARY()

SRCS(
    auditable_actions.cpp
    audit.cpp
    url_matcher.cpp
)

PEERDIR(
    library/cpp/cgiparam
    library/cpp/json
    library/cpp/protobuf/json
    contrib/ydb/library/actors/http
    contrib/ydb/core/audit
)

END()
