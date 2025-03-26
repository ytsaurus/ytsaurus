LIBRARY()

SRCS(
    login_page.cpp
    login_page.h
    login_shared_func.cpp
    secure_request.h
    ticket_parser_impl.h
    ticket_parser.cpp
    ticket_parser.h
)

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/library/actors/http
    contrib/ydb/library/grpc/actor_client
    library/cpp/monlib/service/pages
    library/cpp/openssl/io
    contrib/ydb/core/audit
    contrib/ydb/core/base
    contrib/ydb/core/protos
    contrib/ydb/library/aclib
    contrib/ydb/library/aclib/protos
    contrib/ydb/library/login
    contrib/ydb/library/ncloud/impl
    contrib/ydb/library/security
    contrib/ydb/library/ycloud/api
    contrib/ydb/library/ycloud/impl
)

END()

RECURSE_FOR_TESTS(
    ut
)

RECURSE(
    certificate_check
    ldap_auth_provider
)
