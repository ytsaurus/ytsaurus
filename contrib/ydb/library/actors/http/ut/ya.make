UNITTEST_FOR(contrib/ydb/library/actors/http)

SIZE(SMALL)

PEERDIR(
    contrib/libs/poco/NetSSL_OpenSSL
    contrib/libs/poco/Crypto
    contrib/libs/poco/Foundation
    contrib/libs/poco/Net
    contrib/ydb/core/security/certificate_check/test_utils
    contrib/ydb/library/actors/testlib
)


IF (NOT OS_WINDOWS)
SRCS(
    http_cache_ut.cpp
    http_ut.cpp
    http2_ut.cpp
    tls_client_connection.cpp
)
ELSE()
ENDIF()

END()
