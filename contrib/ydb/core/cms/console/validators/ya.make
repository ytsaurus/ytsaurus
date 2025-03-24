LIBRARY()

SRCS(
    core_validators.cpp
    core_validators.h
    registry.cpp
    registry.h
    validator.cpp
    validator.h
    validator_bootstrap.cpp
    validator_bootstrap.h
    validator_nameservice.cpp
    validator_nameservice.h
)

PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/core/protos
    contrib/ydb/core/tablet
    contrib/ydb/public/api/protos
    library/cpp/deprecated/atomic
)

END()

RECURSE_FOR_TESTS(
    ut
)
