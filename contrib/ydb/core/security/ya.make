LIBRARY()

SRCS(
    login_page.cpp
    login_page.h
    login_shared_func.cpp
    secure_request.h
    ticket_parser_impl.h
    ticket_parser.cpp
    ticket_parser.h
    ldap_auth_provider.cpp
    ldap_utils.cpp
)

IF(OS_LINUX OR OS_DARWIN)
    PEERDIR(
        contrib/libs/openldap
    )

    SRCS(
        ldap_auth_provider_linux.cpp
    )
ELSEIF(OS_WINDOWS)
    EXTRALIBS_STATIC(wldap32.lib)

    SRCS(
        ldap_auth_provider_win.cpp
    )
ENDIF()

PEERDIR(
    library/cpp/actors/core
    library/cpp/actors/http
    library/cpp/monlib/service/pages
    library/cpp/openssl/io
    contrib/ydb/core/base
    contrib/ydb/core/protos
    contrib/ydb/library/ycloud/api
    contrib/ydb/library/ycloud/impl
    contrib/ydb/library/aclib
    contrib/ydb/library/aclib/protos
    contrib/ydb/library/login
    contrib/ydb/library/security
)

END()

RECURSE_FOR_TESTS(
    ut
)
