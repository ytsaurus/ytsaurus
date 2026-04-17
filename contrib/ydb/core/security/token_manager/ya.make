LIBRARY()

SRCS(
    token_manager.cpp
    vm_metadata_token_provider_handler.cpp
    token_provider.cpp
)


PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/library/actors/core
    contrib/ydb/library/actors/http
    contrib/ydb/core/protos
    contrib/ydb/core/util
    library/cpp/json
)

END()

RECURSE_FOR_TESTS(
    ut
)
