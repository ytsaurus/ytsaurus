LIBRARY()

PEERDIR(
    contrib/libs/openssl
    library/cpp/digest/argonish
    library/cpp/json
    library/cpp/string_utils/base64
    contrib/ydb/core/protos
    contrib/ydb/core/tx/scheme_cache
    contrib/ydb/library/actors/core
    contrib/ydb/library/login/hashes_checker
    contrib/ydb/library/login/password_checker
    contrib/ydb/library/login/protos
    contrib/ydb/library/login/sasl
    contrib/ydb/library/ydb_issue/proto
    yql/essentials/public/issue
)

SRCS(
    base_auth_actors.cpp
    hasher.cpp
    plain_auth_actor.cpp
    plain_ldap_auth_proxy_actor.cpp
    scram_auth_actor.cpp
    static_credentials_provider.cpp
)

END()
